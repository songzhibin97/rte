# RTE Example Application

这是一个演示如何使用 RTE (Reliable Transaction Engine) 库的示例应用程序。

## 项目结构

```
rte-example-app/
├── cmd/
│   └── server/
│       └── main.go          # 主程序入口
├── internal/
│   ├── domain/
│   │   └── models.go        # 领域模型
│   ├── steps/
│   │   ├── debit.go         # 扣款步骤
│   │   ├── credit.go        # 入账步骤
│   │   ├── notify.go        # 通知步骤
│   │   └── finalize.go      # 完成步骤
│   ├── store/
│   │   └── memory_store.go  # 内存存储实现
│   └── service/
│       └── transfer.go      # 转账服务
├── go.mod
└── README.md
```

## 运行示例

```bash
cd rte-example-app
go run ./cmd/server
```

## 功能演示

1. **账户转账**: 演示从储蓄账户到外汇账户的转账流程
2. **补偿机制**: 演示失败时的自动补偿
3. **幂等性**: 演示重复请求的幂等处理
4. **事件监听**: 演示事务生命周期事件的监听

## 核心概念

### Step 实现

每个 Step 需要实现以下方法:
- `Name()`: 返回步骤名称
- `Execute()`: 执行步骤逻辑
- `Compensate()`: 补偿逻辑（可选）
- `SupportsCompensation()`: 是否支持补偿
- `IdempotencyKey()`: 幂等性键（可选）
- `SupportsIdempotency()`: 是否支持幂等性

### 事务构建

```go
tx, err := engine.NewTransaction("transfer").
    WithLockKeys("account:1", "account:2").
    WithInput(map[string]any{
        "from_account_id": int64(1),
        "to_account_id":   int64(2),
        "amount":          100.0,
    }).
    AddStep("debit").
    AddStep("credit").
    AddStep("notify").
    AddStep("finalize").
    Build()
```

### 事件监听

```go
engine.Subscribe(event.EventTxCompleted, func(ctx context.Context, e event.Event) error {
    log.Printf("Transaction %s completed", e.TxID)
    return nil
})
```
