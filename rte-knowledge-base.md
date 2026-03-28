# RTE (Reliable Transaction Engine) AI Agent 知识库

## 项目概述

RTE 是一个用 Go 语言实现的可靠分布式事务引擎，采用编排式 Saga 模式管理分布式事务的编排、执行、补偿和恢复。

## 核心架构

### 模块结构

```
rte/
├── engine.go           # 引擎入口，提供事务创建和执行API
├── coordinator.go      # 协调器，核心事务编排逻辑
├── transaction.go      # 事务定义和Builder模式
├── step.go             # 步骤接口和基础实现
├── state.go            # 状态机定义
├── context.go          # 事务上下文
├── options.go          # 配置选项
├── errors.go           # 错误定义
├── store_interface.go  # 存储接口
├── admin/              # 管理接口(Web控制台+REST API)
├── circuit/            # 熔断器
├── event/              # 事件总线
├── idempotency/        # 幂等性检查
├── lock/               # 分布式锁
├── metrics/            # Prometheus指标
├── recovery/           # 恢复Worker
├── store/              # 存储实现(MySQL)
├── tracing/            # OpenTelemetry追踪
└── testinfra/          # 测试基础设施
```

### 事务状态机

```
CREATED → LOCKED → EXECUTING → CONFIRMING → COMPLETED
                      ↓
                   FAILED → COMPENSATING → COMPENSATED
                      ↓            ↓
                 EXECUTING   COMPENSATION_FAILED
                 （重试）
                      ↓
                  CANCELLED
                  （强制取消）
```

步骤级状态机补充：
- `StepStatusFailed → StepStatusExecuting`（步骤重试）

## 关键接口

### Step 接口
```go
type Step interface {
    Name() string
    Execute(ctx context.Context, txCtx *TxContext) error
    Compensate(ctx context.Context, txCtx *TxContext) error
    SupportsCompensation() bool
    IdempotencyKey(txCtx *TxContext) string
    SupportsIdempotency() bool
    Config() *StepConfig
}
```

### TxStore 接口
```go
type TxStore interface {
    // 事务操作
    CreateTransaction(ctx context.Context, tx *StoreTx) error
    UpdateTransaction(ctx context.Context, tx *StoreTx) error
    GetTransaction(ctx context.Context, txID string) (*StoreTx, error)

    // 步骤操作
    CreateStep(ctx context.Context, step *StoreStepRecord) error
    UpdateStep(ctx context.Context, step *StoreStepRecord) error
    GetStep(ctx context.Context, txID string, stepIndex int) (*StoreStepRecord, error)
    GetSteps(ctx context.Context, txID string) ([]*StoreStepRecord, error)

    // 恢复查询
    GetPendingTransactions(ctx context.Context, olderThan time.Duration) ([]*StoreTx, error)
    GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*StoreTx, error)
    GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*StoreTx, error)

    // 管理查询
    ListTransactions(ctx context.Context, filter *StoreTxFilter) ([]*StoreTx, int64, error)
    CountTransactionsByStatus(ctx context.Context) (map[TxStatus]int64, error)

    // 幂等性操作
    CheckIdempotency(ctx context.Context, key string) (exists bool, result []byte, err error)
    MarkIdempotency(ctx context.Context, key string, result []byte, ttl time.Duration) error
    DeleteExpiredIdempotency(ctx context.Context) (int64, error)
}
```

TxStore 直接由 MySQL 实现（`store/mysql`），不存在额外的 store.Store 包装接口。

### Breaker 接口（熔断器管理器）

```go
// circuit/breaker.go

type Breaker interface {
    // Get 返回指定服务的熔断器（使用默认配置）
    Get(service string) CircuitBreaker
    // GetWithConfig 返回指定服务的熔断器（使用自定义配置）
    GetWithConfig(service string, config BreakerConfig) CircuitBreaker
    // List 返回所有已注册的熔断器，按服务名排序
    List() []ServiceBreaker
}

// ServiceBreaker 将服务名与其 CircuitBreaker 实例配对
type ServiceBreaker struct {
    Service string
    Breaker CircuitBreaker
}
```

### Recovery Worker Coordinator 接口

`recovery` 包内部定义的 `Coordinator` 接口（由 `*rte.Coordinator` 实现）：

```go
// recovery/worker.go

type Coordinator interface {
    Resume(ctx context.Context, tx *rte.StoreTx) (*rte.TxResult, error)
}
```

注意：不存在 `recovery.StoreTx` 或 `recovery.TxStore` 类型，recovery 包直接使用 `rte.StoreTx` 和 `rte.TxStore`。

## 开发指南

### 创建新Step

1. 嵌入 `*rte.BaseStep`
2. 实现 `Execute` 方法
3. 如需补偿，实现 `Compensate` 并返回 `SupportsCompensation() = true`
4. 如需幂等性，实现 `IdempotencyKey` 并返回 `SupportsIdempotency() = true`

### Engine 初始化

```go
engine := rte.NewEngine(store,
    rte.WithEngineLocker(locker),
    rte.WithEngineBreaker(breaker),
    rte.WithEngineEventBus(eventBus),
    rte.WithEngineChecker(checker),
    rte.WithEngineConfig(cfg),
)
```

`NewEngine(store TxStore, opts ...EngineOption)` — store 为必传参数，其余通过 `EngineOption` 注入，不存在 `WithEngineStore` 选项。

### 事务构建

```go
tx, err := engine.NewTransaction("tx_type").
    WithLockKeys("key1", "key2").
    WithInput(map[string]any{"param": value}).
    AddStep("step1").
    AddStep("step2").
    Build()
```

### 配置参数

关键配置在 `options.go` 中的 `Config` 结构体：
- `LockTTL`: 锁超时时间 (默认30s)
- `StepTimeout`: 单步骤超时 (默认10s)
- `TxTimeout`: 事务总超时 (默认5min)
- `MaxRetries`: 最大重试次数 (默认3)
- `RetryInterval`: 重试间隔 (默认1s)

## 测试指南

### 运行测试

```bash
# 运行所有测试
go test ./... -v

# 运行短测试(跳过集成测试)
go test ./... -short

# 查看覆盖率
go test ./... -cover
```

### 属性测试

项目使用 `pgregory.net/rapid` 进行属性测试，关键属性包括：
- `TestProperty_TransactionStateConsistency`: 状态转换一致性
- `TestProperty_CompensationCompleteness`: 补偿完整性
- `TestProperty_BalanceConservation`: 余额守恒
- `TestProperty_IdempotentExecution`: 幂等执行

### 集成测试

集成测试位于 `testinfra/` 目录，需要 MySQL 和 Redis 环境。

## Admin 管理接口

### Web 控制台路由

| 路径 | 功能 |
|------|------|
| `/` | 仪表盘 |
| `/transactions` | 事务列表 |
| `/transactions/{txID}` | 事务详情 |
| `/recovery` | 恢复监控 |
| `/circuit-breakers` | 熔断器面板 |
| `/events` | 事件日志 |

### REST API

| 方法 | 路径 | 功能 |
|------|------|------|
| GET | `/api/transactions` | 事务列表 |
| GET | `/api/transactions/{txID}` | 事务详情 |
| POST | `/api/transactions/{txID}/force-complete` | 强制完成 |
| POST | `/api/transactions/{txID}/force-cancel` | 强制取消 |
| POST | `/api/transactions/{txID}/retry` | 重试事务 |
| GET | `/api/stats` | 统计信息 |

## 常见问题排查

### 事务卡住

1. 检查 Recovery Worker 是否运行
2. 查看 `rte_transactions` 表中的 `status` 和 `updated_at`
3. 检查分布式锁是否正常释放

### 补偿失败

1. 检查 `COMPENSATION_FAILED` 状态的事务
2. 查看 `error_msg` 字段
3. 手动处理或使用 Admin API 重试

### 性能优化

1. 调整 `LockTTL` 和 `LockExtendPeriod`
2. 配置合适的 `StepTimeout`
3. 使用熔断器保护下游服务

## 文件修改注意事项

### 修改 Step 实现时
- 确保 `Compensate` 方法是幂等的
- 测试失败场景下的补偿逻辑
- 更新相关的属性测试

### 修改状态机时
- 更新 `state.go` 中的 `validTxTransitions`
- 更新相关测试
- 检查 Recovery Worker 的处理逻辑

### 修改存储层时
- 确保乐观锁正确实现
- 更新 `schema.sql`
- 运行集成测试验证

## 依赖关系

```
engine.go
    └── coordinator.go
            ├── store (TxStore)
            ├── lock (Locker)
            ├── circuit (Breaker)
            ├── event (EventBus)
            └── idempotency (Checker)
```

## Prometheus 指标

指标前缀默认为 `rte_`，主要指标包括：`tx_started_total`、`tx_completed_total`、`tx_failed_total`、`step_failed_total`、`circuit_breaker_state` 等。

### reason 标签白名单机制

`tx_failed_total` 和 `step_failed_total` 等计数器带有 `reason` 标签。为防止用户传入任意字符串导致 Prometheus 基数爆炸（OOM），实现层维护了一个白名单（`knownReasons`）：

| 白名单值 | 含义 |
|---|---|
| `timeout` | 超时 |
| `cancelled` | 已取消 |
| `compensation_failed` | 补偿失败 |
| `step_error` | 步骤错误 |
| `lock_failed` | 锁获取失败 |
| `circuit_open` | 熔断器开路 |
| `idempotency_failed` | 幂等检查失败 |
| `version_conflict` | 乐观锁版本冲突 |
| `store_error` | 存储层错误 |
| `context_cancelled` | context 已取消 |
| `context_deadline` | context 超时 |

不在白名单内的 reason 值统一归并为 `other`（由 `normalizeReason` 函数处理，位于 `metrics/prometheus/prometheus.go`）。

## 版本信息

- Go 版本: 1.25.0
- 主要依赖:
  - `pgregory.net/rapid`: 属性测试
  - `github.com/prometheus/client_golang`: Prometheus指标
  - `go.opentelemetry.io/otel`: OpenTelemetry追踪
  - `github.com/redis/go-redis/v9`: Redis客户端
  - `github.com/go-sql-driver/mysql`: MySQL驱动
