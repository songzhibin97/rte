// Package steps 定义事务步骤
package steps

import (
	"context"
	"fmt"

	"rte"
	"rte-example-app/internal/domain"
)

// DebitStep 扣款步骤
// 从源账户扣除金额
type DebitStep struct {
	*rte.BaseStep
	repo *domain.MemoryAccountRepository
}

// NewDebitStep 创建扣款步骤
func NewDebitStep(repo *domain.MemoryAccountRepository) *DebitStep {
	return &DebitStep{
		BaseStep: rte.NewBaseStep("debit"),
		repo:     repo,
	}
}

// Execute 执行扣款
func (s *DebitStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	// 获取输入参数
	accountID, err := rte.GetInputAs[int64](txCtx, "from_account_id")
	if err != nil {
		return fmt.Errorf("missing from_account_id: %w", err)
	}
	amount, err := rte.GetInputAs[float64](txCtx, "amount")
	if err != nil {
		return fmt.Errorf("missing amount: %w", err)
	}
	ticket, err := rte.GetInputAs[string](txCtx, "ticket")
	if err != nil {
		return fmt.Errorf("missing ticket: %w", err)
	}

	// 验证金额
	if amount <= 0 {
		return domain.ErrInvalidAmount
	}

	// 创建待处理日志
	logID := s.repo.CreateLog(ticket, accountID, "OUT", -amount)
	txCtx.SetOutput("debit_log_id", logID)

	// 扣款
	if err := s.repo.UpdateBalance(accountID, -amount); err != nil {
		return fmt.Errorf("failed to debit account: %w", err)
	}

	txCtx.SetOutput("debited", true)
	txCtx.SetOutput("debit_amount", amount)
	return nil
}

// Compensate 补偿扣款（恢复余额）
func (s *DebitStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "from_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")
	logID, _ := rte.GetOutputAs[int64](txCtx, "debit_log_id")

	// 恢复余额
	if err := s.repo.UpdateBalance(accountID, amount); err != nil {
		return fmt.Errorf("failed to restore balance: %w", err)
	}

	// 更新日志状态
	if logID > 0 {
		s.repo.UpdateLogStatus(logID, "FAILED")
	}

	return nil
}

// SupportsCompensation 支持补偿
func (s *DebitStep) SupportsCompensation() bool {
	return true
}

// IdempotencyKey 幂等性键
func (s *DebitStep) IdempotencyKey(txCtx *rte.TxContext) string {
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")
	accountID, _ := rte.GetInputAs[int64](txCtx, "from_account_id")
	return fmt.Sprintf("debit:%s:%d", ticket, accountID)
}

// SupportsIdempotency 支持幂等性
func (s *DebitStep) SupportsIdempotency() bool {
	return true
}
