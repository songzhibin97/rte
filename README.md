# RTE - Reliable Transaction Engine

RTE 是一个用 Go 语言实现的可靠分布式事务引擎，采用 Saga 模式管理分布式事务的编排、执行、补偿和恢复。

## 特性

- **Saga 编排模式** - 中央协调器管理多步骤事务编排与自动补偿
- **分布式锁** - Redis 实现，防止并发冲突
- **熔断器** - 保护下游服务，支持自动恢复
- **幂等性** - 步骤级别幂等性检查，防止重复执行
- **自动恢复** - 后台 Worker 自动恢复卡住/失败的事务
- **可观测性** - Prometheus 指标 + OpenTelemetry 追踪
- **管理接口** - Web 控制台 + REST API，事务查询、强制完成/取消、手动重试

## 安装

```bash
go get github.com/your-org/rte  # 替换为实际的模块路径
```

> **注意**: 请将 `github.com/your-org/rte` 替换为您实际的模块路径。

## 快速开始

### 1. 初始化数据库

```sql
-- 执行 store/mysql/schema.sql 创建表结构
source store/mysql/schema.sql;
```

### 2. 创建引擎

```go
package main

import (
    "database/sql"
    "time"
    
    "rte"
    "rte/circuit/memory"
    "rte/event"
    "rte/lock/redis"
    "rte/store/mysql"
    
    goredis "github.com/redis/go-redis/v9"
)

func main() {
    // 初始化 MySQL
    db, _ := sql.Open("mysql", "user:password@tcp(localhost:3306)/rte")
    store := mysql.New(db)
    
    // 初始化 Redis 锁
    redisClient := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
    locker := redis.NewRedisLocker(redisClient)
    
    // 初始化熔断器
    breaker := memory.NewMemoryBreaker()
    
    // 初始化事件总线
    eventBus := event.NewMemoryEventBus()
    
    // 创建引擎
    engine := rte.NewEngine(
        rte.WithEngineStore(store),
        rte.WithEngineLocker(locker),
        rte.WithEngineBreaker(breaker),
        rte.WithEngineEventBus(eventBus),
        rte.WithEngineConfig(rte.Config{
            LockTTL:          30 * time.Second,
            LockExtendPeriod: 10 * time.Second,
            StepTimeout:      10 * time.Second,
            TxTimeout:        5 * time.Minute,
            MaxRetries:       3,
            RetryInterval:    5 * time.Second,
            IdempotencyTTL:   24 * time.Hour,
        }),
    )
    
    // 注册步骤并执行事务...
}
```

### 3. 定义步骤 (Step)

```go
// 定义一个扣款步骤
type DebitStep struct {
    *rte.BaseStep
    accountService AccountService
}

func NewDebitStep(svc AccountService) *DebitStep {
    return &DebitStep{
        BaseStep:       rte.NewBaseStep("debit"),
        accountService: svc,
    }
}

// 执行扣款
func (s *DebitStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
    // 从输入获取参数
    accountID, _ := rte.GetInputAs[string](txCtx, "account_id")
    amount, _ := rte.GetInputAs[float64](txCtx, "amount")
    
    // 执行业务逻辑
    ref, err := s.accountService.Debit(ctx, accountID, amount)
    if err != nil {
        return err
    }
    
    // 设置输出供后续步骤使用
    txCtx.SetOutput("debit_ref", ref)
    return nil
}

// 补偿操作 - 退款
func (s *DebitStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
    ref, _ := rte.GetOutputAs[string](txCtx, "debit_ref")
    return s.accountService.Refund(ctx, ref)
}

// 声明支持补偿
func (s *DebitStep) SupportsCompensation() bool {
    return true
}

// 幂等性 Key (可选)
func (s *DebitStep) IdempotencyKey(txCtx *rte.TxContext) string {
    return fmt.Sprintf("debit:%s:%s", txCtx.TxID, txCtx.Input["account_id"])
}

func (s *DebitStep) SupportsIdempotency() bool {
    return true
}
```

### 4. 注册步骤并执行事务

```go
// 注册步骤
engine.RegisterStep(NewDebitStep(accountSvc))
engine.RegisterStep(NewTransferStep(transferSvc))
engine.RegisterStep(NewCreditStep(accountSvc))

// 创建事务
tx, err := engine.NewTransaction("fund_transfer").
    WithLockKeys("account:123", "account:456").  // 需要锁定的资源
    WithInput(map[string]any{
        "from_account": "123",
        "to_account":   "456",
        "amount":       100.00,
        "currency":     "USD",
    }).
    WithTimeout(2 * time.Minute).
    WithMaxRetries(3).
    AddStep("debit").
    AddStep("transfer").
    AddStep("credit").
    Build()

if err != nil {
    log.Fatal(err)
}

// 执行事务
result, err := engine.Execute(ctx, tx)
if err != nil {
    log.Printf("Transaction failed: %v", err)
}

log.Printf("Transaction %s completed with status: %s", result.TxID, result.Status)
log.Printf("Output: %v", result.Output)
```

## 事务状态

```
CREATED → LOCKED → EXECUTING → CONFIRMING → COMPLETED
                      ↓
                   FAILED → COMPENSATING → COMPENSATED
                                ↓
                        COMPENSATION_FAILED
```

| 状态 | 说明 |
|------|------|
| CREATED | 事务已创建 |
| LOCKED | 已获取分布式锁 |
| EXECUTING | 正在执行步骤 |
| CONFIRMING | 正在确认完成 |
| COMPLETED | 执行成功 |
| FAILED | 执行失败 |
| COMPENSATING | 正在补偿 |
| COMPENSATED | 补偿完成 |
| COMPENSATION_FAILED | 补偿失败 |
| TIMEOUT | 超时 |
| CANCELLED | 已取消 |

## 事件订阅

```go
// 订阅特定事件
engine.Subscribe(event.EventTxCompleted, func(ctx context.Context, e event.Event) error {
    log.Printf("Transaction %s completed", e.TxID)
    return nil
})

engine.Subscribe(event.EventTxFailed, func(ctx context.Context, e event.Event) error {
    log.Printf("Transaction %s failed: %v", e.TxID, e.Error)
    // 发送告警通知
    return nil
})

engine.Subscribe(event.EventAlertCritical, func(ctx context.Context, e event.Event) error {
    // 补偿失败等严重告警
    alertService.SendCritical(e.TxID, e.Data["message"])
    return nil
})

// 订阅所有事件
engine.SubscribeAll(func(ctx context.Context, e event.Event) error {
    log.Printf("[%s] %s - %s", e.Type, e.TxID, e.StepName)
    return nil
})
```

### 可用事件类型

| 事件 | 说明 |
|------|------|
| `tx.created` | 事务创建 |
| `tx.completed` | 事务完成 |
| `tx.failed` | 事务失败 |
| `tx.timeout` | 事务超时 |
| `tx.cancelled` | 事务取消 |
| `tx.compensation_failed` | 补偿失败 |
| `step.started` | 步骤开始 |
| `step.completed` | 步骤完成 |
| `step.failed` | 步骤失败 |
| `circuit.opened` | 熔断器打开 |
| `circuit.closed` | 熔断器关闭 |
| `recovery.start` | 恢复扫描开始 |
| `alert.warning` | 警告告警 |
| `alert.critical` | 严重告警 |

## 恢复 Worker

自动恢复卡住和失败的事务：

```go
import "rte/recovery"

worker := recovery.NewWorker(
    recovery.WithStore(store),
    recovery.WithLocker(locker),
    recovery.WithCoordinator(engine.Coordinator()),
    recovery.WithEventBus(eventBus),
    recovery.WithConfig(recovery.Config{
        RecoveryInterval: 30 * time.Second,  // 扫描间隔
        StuckThreshold:   5 * time.Minute,   // 卡住阈值
        MaxRetries:       3,                  // 最大重试次数
        LockTTL:          30 * time.Second,
    }),
)

// 启动 Worker
worker.Start(ctx)

// 停止 Worker
defer worker.Stop()

// 获取统计信息
stats := worker.Stats()
log.Printf("Scanned: %d, Processed: %d, Failed: %d", 
    stats.ScannedCount, stats.ProcessedCount, stats.FailedCount)
```

## 管理接口

RTE 提供两种管理方式：编程式 API 和 Web 管理控制台。

### Web 管理控制台

RTE 内置了一个功能完整的 Web 管理控制台，提供可视化的事务管理界面。

**启动控制台：**

```bash
go run cmd/admin/main.go
# 访问 http://localhost:8080
```

**功能页面：**

| 页面 | 路径 | 功能 |
|------|------|------|
| 仪表盘 | `/` | 事务统计概览、状态分布 |
| 事务列表 | `/transactions` | 事务查询、筛选、分页 |
| 事务详情 | `/transactions/{txID}` | 事务完整信息、步骤执行详情 |
| 恢复监控 | `/recovery` | Recovery Worker 状态和统计 |
| 熔断器 | `/circuit-breakers` | 熔断器状态、手动重置 |
| 事件日志 | `/events` | 实时事件流、事件筛选 |

**集成到现有服务：**

```go
import "rte/admin"

// 创建 Admin 实现
adminImpl := admin.NewAdmin(
    admin.WithAdminStore(store),
    admin.WithAdminCoordinator(engine.Coordinator()),
    admin.WithAdminEventBus(eventBus),
)

// 创建事件存储（用于事件日志页面）
eventStore := admin.NewEventStore(1000) // 保留最近 1000 条事件
eventBus.SubscribeAll(eventStore.EventHandler())

// 创建 Admin Server
server := admin.NewAdminServer(
    admin.WithAddr(":8080"),
    admin.WithAdminImpl(adminImpl),
    admin.WithServerStore(store),
    admin.WithServerBreaker(breaker),
    admin.WithServerEventBus(eventBus),
    admin.WithEventStore(eventStore),
    admin.WithServerRecovery(recoveryWorker), // 可选
)

// 启动服务器
go server.Start()
defer server.Stop(ctx)
```

### REST API

Web 控制台同时提供 REST API，可用于自动化集成：

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/transactions` | 事务列表（支持筛选和分页） |
| GET | `/api/transactions/{txID}` | 事务详情 |
| POST | `/api/transactions/{txID}/force-complete` | 强制完成事务 |
| POST | `/api/transactions/{txID}/force-cancel` | 强制取消事务 |
| POST | `/api/transactions/{txID}/retry` | 重试事务 |
| GET | `/api/stats` | 事务统计 |
| GET | `/api/recovery/stats` | 恢复统计 |
| GET | `/api/circuit-breakers` | 熔断器列表 |
| POST | `/api/circuit-breakers/{service}/reset` | 重置熔断器 |
| GET | `/api/events` | 事件列表 |

**API 示例：**

```bash
# 获取失败的事务
curl "http://localhost:8080/api/transactions?status=FAILED&page=1&page_size=20"

# 强制完成事务
curl -X POST "http://localhost:8080/api/transactions/tx-123/force-complete" \
  -H "Content-Type: application/json" \
  -d '{"reason": "manual intervention"}'

# 获取统计信息
curl "http://localhost:8080/api/stats"
```

### 编程式 API

```go
import "rte/admin"

adm := admin.NewAdmin(
    admin.WithAdminStore(store),
    admin.WithAdminCoordinator(engine.Coordinator()),
    admin.WithAdminEventBus(eventBus),
)

// 列出事务
result, _ := adm.ListTransactions(ctx, &rte.StoreTxFilter{
    Status: []rte.TxStatus{rte.TxStatusFailed, rte.TxStatusExecuting},
    TxType: "fund_transfer",
    Limit:  100,
    Offset: 0,
})

for _, tx := range result.Transactions {
    log.Printf("TX: %s, Status: %s, Step: %d/%d", 
        tx.TxID, tx.Status, tx.CurrentStep, tx.TotalSteps)
}

// 获取事务详情
detail, _ := adm.GetTransaction(ctx, "tx-123")
log.Printf("Transaction: %+v", detail.Transaction)
for _, step := range detail.Steps {
    log.Printf("  Step %d: %s - %s", step.StepIndex, step.StepName, step.Status)
}

// 强制完成卡住的事务
err := adm.ForceComplete(ctx, "tx-123", "manual intervention")

// 强制取消事务 (会触发补偿)
err := adm.ForceCancel(ctx, "tx-123", "business requirement")

// 手动重试失败的事务
err := adm.RetryTransaction(ctx, "tx-123")

// 获取统计信息
stats, _ := adm.GetStats(ctx)
log.Printf("Total: %d, Pending: %d, Failed: %d, Completed: %d",
    stats.TotalTransactions, stats.PendingTransactions,
    stats.FailedTransactions, stats.CompletedTransactions)
```

## Prometheus 指标

```go
import "rte/metrics/prometheus"

metrics := prometheus.NewPrometheusMetrics()

// 在引擎中使用 (需要自行集成到 Coordinator)
// 或者通过事件订阅来记录指标

engine.Subscribe(event.EventTxCompleted, func(ctx context.Context, e event.Event) error {
    metrics.TxCompleted(e.TxType, time.Since(e.Timestamp))
    return nil
})
```

### 可用指标

| 指标 | 类型 | 说明 |
|------|------|------|
| `rte_tx_started_total` | Counter | 事务启动总数 |
| `rte_tx_completed_total` | Counter | 事务完成总数 |
| `rte_tx_failed_total` | Counter | 失败事务数 |
| `rte_tx_compensated_total` | Counter | 补偿完成事务数 |
| `rte_tx_duration_seconds` | Histogram | 事务执行时间 |
| `rte_step_started_total` | Counter | 步骤启动总数 |
| `rte_step_completed_total` | Counter | 步骤完成总数 |
| `rte_step_failed_total` | Counter | 步骤失败总数 |
| `rte_step_duration_seconds` | Histogram | 步骤执行时间 |
| `rte_circuit_breaker_state` | Gauge | 熔断器状态 (0=关闭, 1=打开, 2=半开) |
| `rte_lock_acquired_total` | Counter | 锁获取总数 |
| `rte_lock_acquire_duration_seconds` | Histogram | 锁获取时间 |
| `rte_recovery_scanned_total` | Counter | 恢复扫描数 |
| `rte_recovery_processed_total` | Counter | 恢复处理数 |

## OpenTelemetry 追踪

```go
import "rte/tracing"

tracer := tracing.NewOTelTracer(tracing.Config{
    ServiceName: "my-service",
})

// 在事务执行时创建 Span
ctx, span := tracer.StartTransaction(ctx, tx.TxID(), tx.TxType())
defer span.End()

// 在步骤执行时创建子 Span
ctx, stepSpan := tracer.StartStep(ctx, tx.TxID(), "debit", 0)
defer stepSpan.End()

// 记录错误
if err != nil {
    stepSpan.SetError(err)
}
```

## 配置参数

```go
rte.Config{
    // 锁配置
    LockTTL:          30 * time.Second,  // 锁超时时间
    LockExtendPeriod: 10 * time.Second,  // 锁续期间隔
    
    // 重试配置
    MaxRetries:    3,                    // 最大重试次数
    RetryInterval: 5 * time.Second,      // 重试间隔
    
    // 熔断器配置
    CircuitThreshold:    5,              // 失败阈值
    CircuitTimeout:      30 * time.Second, // 恢复超时
    CircuitHalfOpenReqs: 3,              // 半开状态最大请求数
    
    // 恢复配置
    RecoveryInterval: 30 * time.Second,  // 恢复扫描间隔
    StuckThreshold:   5 * time.Minute,   // 卡住判定阈值
    
    // 超时配置
    StepTimeout: 10 * time.Second,       // 单步骤超时
    TxTimeout:   5 * time.Minute,        // 事务总超时
    
    // 幂等性配置
    IdempotencyTTL: 24 * time.Hour,      // 幂等记录保留时间
}
```

## 完整示例

参见 [examples/](./examples/) 目录。

## 数据库表结构

参见 [store/mysql/schema.sql](./store/mysql/schema.sql)。

## Saga 模式说明

### RTE 采用的模式：编排式 Saga (Orchestration)

RTE 实现的是 **编排式 Saga**，由中央协调器 (`Coordinator`) 控制整个事务流程：

```
┌─────────────────────────────────────────────────────────────┐
│                      Coordinator                             │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐  │
│  │ Step 1  │───▶│ Step 2  │───▶│ Step 3  │───▶│ Confirm │  │
│  │ (Debit) │    │(Transfer)│    │ (Credit)│    │         │  │
│  └────┬────┘    └────┬────┘    └────┬────┘    └─────────┘  │
│       │              │              │                       │
│       ▼              ▼              ▼                       │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐                 │
│  │Compensate│◀──│Compensate│◀──│Compensate│  (失败时反向)   │
│  └─────────┘    └─────────┘    └─────────┘                 │
└─────────────────────────────────────────────────────────────┘
```

**特点：**
- 协调器知道所有步骤，按顺序执行
- 失败时由协调器统一触发补偿
- 状态集中存储，易于追踪和恢复
- 同步调用为主，流程清晰

### 两种 Saga 模式对比

| 特性 | 编排式 (Orchestration) | 协调式 (Choreography) |
|------|------------------------|----------------------|
| **控制方式** | 中央协调器控制流程 | 各服务通过事件自主响应 |
| **通信方式** | 同步调用为主 | 异步消息/事件驱动 |
| **耦合度** | 协调器知道所有步骤 | 服务间松耦合 |
| **复杂度** | 流程清晰，易于理解和调试 | 流程分散，需要追踪事件链 |
| **单点风险** | 协调器是关键节点 | 无单点，但依赖消息队列可靠性 |
| **补偿触发** | 协调器统一触发 | 各服务监听失败事件自行补偿 |
| **适用规模** | 中小规模，步骤 3-10 个 | 大规模微服务，跨团队协作 |

### 协调式 Saga 示意图

```
Service A                Service B                Service C
    │                        │                        │
    │  ──── OrderCreated ───▶│                        │
    │                        │  ──── PaymentDone ────▶│
    │                        │                        │
    │                        │◀─── ShipmentFailed ────│
    │◀─── PaymentRefund ─────│                        │
    │                        │                        │
(各服务监听事件，自主决定下一步动作)
```

### 如何选择？

**选择编排式 (RTE) 当：**
- ✅ 事务步骤较少（3-10 步）
- ✅ 需要清晰的执行流程和状态追踪
- ✅ 服务由同一团队或少数团队维护
- ✅ 需要强一致性和可预测的执行顺序
- ✅ 调试和问题排查是优先考虑
- ✅ 单体应用或服务数量有限的微服务

**选择协调式当：**
- ✅ 微服务数量多（10+），由不同团队独立维护
- ✅ 需要高度解耦，服务独立演进
- ✅ 已有成熟的消息队列基础设施（Kafka、RabbitMQ、AWS SQS）
- ✅ 可以接受最终一致性
- ✅ 事务流程可能频繁变化
- ✅ 跨组织/跨公司的服务集成

### RTE 的设计权衡

RTE 选择编排式的原因：

1. **可观测性** - 所有状态集中在数据库，Admin 界面可查看完整事务链路
2. **恢复简单** - Recovery Worker 可以直接恢复卡住的事务
3. **调试友好** - 单点追踪，不需要跨多个服务查日志
4. **补偿可靠** - 协调器确保补偿按正确顺序执行

### 扩展为消息驱动（混合模式）

如果需要在微服务场景下使用 RTE，可以采用**混合模式**：

```go
// 方案 1: 步骤内部使用消息队列
type AsyncTransferStep struct {
    *rte.BaseStep
    producer MessageProducer
    consumer MessageConsumer
}

func (s *AsyncTransferStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
    // 发送消息到目标服务
    correlationID := txCtx.TxID + "-transfer"
    s.producer.Send(ctx, "transfer.request", TransferRequest{
        CorrelationID: correlationID,
        FromAccount:   txCtx.Input["from_account"].(string),
        ToAccount:     txCtx.Input["to_account"].(string),
        Amount:        txCtx.Input["amount"].(float64),
    })
    
    // 等待响应（带超时）
    response, err := s.consumer.WaitForResponse(ctx, correlationID, 30*time.Second)
    if err != nil {
        return err
    }
    
    txCtx.SetOutput("transfer_ref", response.Reference)
    return nil
}
```

```go
// 方案 2: 事件驱动的步骤触发
// 协调器仍然控制流程，但通过消息队列与远程服务通信

type RemoteServiceStep struct {
    *rte.BaseStep
    queue    MessageQueue
    timeout  time.Duration
}

func (s *RemoteServiceStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
    requestID := uuid.New().String()
    
    // 发送请求
    err := s.queue.Publish(ctx, "service.request", Message{
        RequestID: requestID,
        TxID:      txCtx.TxID,
        Payload:   txCtx.Input,
    })
    if err != nil {
        return err
    }
    
    // 订阅响应队列，等待结果
    responseCh := s.queue.Subscribe(ctx, "service.response."+requestID)
    
    select {
    case response := <-responseCh:
        if response.Error != "" {
            return errors.New(response.Error)
        }
        for k, v := range response.Output {
            txCtx.SetOutput(k, v)
        }
        return nil
    case <-time.After(s.timeout):
        return rte.ErrStepTimeout
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

### 完全协调式 vs RTE 混合模式

| 方面 | 完全协调式 | RTE + 消息队列 |
|------|-----------|---------------|
| 实现复杂度 | 高（需要设计事件流） | 中（复用 RTE 框架） |
| 状态追踪 | 需要额外实现 | RTE 内置 |
| 补偿逻辑 | 各服务自行实现 | RTE 统一管理 |
| 恢复机制 | 需要额外实现 | RTE Recovery Worker |
| 适用场景 | 大规模微服务 | 中等规模，需要异步通信 |

### 总结

- **RTE 是编排式 Saga**，适合需要清晰流程控制和状态追踪的场景
- **协调式 Saga** 更适合大规模、高度解耦的微服务架构
- 两种模式**不是非此即彼**，可以根据需求采用混合方案
- 如果已有 RTE，可以通过在 Step 内部集成消息队列来支持异步通信

## License

MIT License
