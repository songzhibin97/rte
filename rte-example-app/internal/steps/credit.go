package steps

import (
	"context"
	"fmt"

	"rte"
	"rte-example-app/internal/domain"
)

// CreditStep 入账步骤
// 向目标账户增加金额
type CreditStep struct {
	*rte.BaseStep
	repo *domain.MemoryAccountRepository
}

// NewCreditStep 创建入账步骤
func NewCreditStep(repo *domain.MemoryAccountRepository) *CreditStep {
	return &CreditStep{
		BaseStep: rte.NewBaseStep("credit"),
		repo:     repo,
	}
}

// Execute 执行入账
func (s *CreditStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	// 获取输入参数
	accountID, err := rte.GetInputAs[int64](txCtx, "to_account_id")
	if err != nil {
		return fmt.Errorf("missing to_account_id: %w", err)
	}
	amount, err := rte.GetInputAs[float64](txCtx, "amount")
	if err != nil {
		return fmt.Errorf("missing amount: %w", err)
	}
	ticket, err := rte.GetInputAs[string](txCtx, "ticket")
	if err != nil {
		return fmt.Errorf("missing ticket: %w", err)
	}

	// 创建待处理日志
	logID := s.repo.CreateLog(ticket, accountID, "IN", amount)
	txCtx.SetOutput("credit_log_id", logID)

	// 入账
	if err := s.repo.UpdateBalance(accountID, amount); err != nil {
		return fmt.Errorf("failed to credit account: %w", err)
	}

	txCtx.SetOutput("credited", true)
	txCtx.SetOutput("credit_amount", amount)
	return nil
}

// Compensate 补偿入账（扣除已入账金额）
func (s *CreditStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")
	logID, _ := rte.GetOutputAs[int64](txCtx, "credit_log_id")

	// 扣除已入账金额
	if err := s.repo.UpdateBalance(accountID, -amount); err != nil {
		return fmt.Errorf("failed to reverse credit: %w", err)
	}

	// 更新日志状态
	if logID > 0 {
		s.repo.UpdateLogStatus(logID, "FAILED")
	}

	return nil
}

// SupportsCompensation 支持补偿
func (s *CreditStep) SupportsCompensation() bool {
	return true
}

// IdempotencyKey 幂等性键
func (s *CreditStep) IdempotencyKey(txCtx *rte.TxContext) string {
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	return fmt.Sprintf("credit:%s:%d", ticket, accountID)
}

// SupportsIdempotency 支持幂等性
func (s *CreditStep) SupportsIdempotency() bool {
	return true
}
