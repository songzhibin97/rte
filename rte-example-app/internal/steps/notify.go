package steps

import (
	"context"
	"fmt"
	"log"

	"rte"
)

// NotifyStep 通知步骤
// 发送转账通知（模拟）
type NotifyStep struct {
	*rte.BaseStep
	// 模拟失败率，用于测试补偿机制
	failRate float64
}

// NewNotifyStep 创建通知步骤
func NewNotifyStep() *NotifyStep {
	return &NotifyStep{
		BaseStep: rte.NewBaseStep("notify"),
		failRate: 0, // 默认不失败
	}
}

// NewNotifyStepWithFailRate 创建带失败率的通知步骤
func NewNotifyStepWithFailRate(failRate float64) *NotifyStep {
	return &NotifyStep{
		BaseStep: rte.NewBaseStep("notify"),
		failRate: failRate,
	}
}

// Execute 执行通知
func (s *NotifyStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	// 获取转账信息
	fromAccountID, _ := rte.GetInputAs[int64](txCtx, "from_account_id")
	toAccountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")

	// 模拟发送通知
	log.Printf("[Notify] Transfer notification sent: ticket=%s, from=%d, to=%d, amount=%.2f",
		ticket, fromAccountID, toAccountID, amount)

	txCtx.SetOutput("notified", true)
	txCtx.SetOutput("notification_id", fmt.Sprintf("NOTIF-%s", ticket))
	return nil
}

// SupportsCompensation 不支持补偿（通知已发送无法撤回）
func (s *NotifyStep) SupportsCompensation() bool {
	return false
}
