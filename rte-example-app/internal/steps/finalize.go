package steps

import (
	"context"
	"log"

	"rte"
	"rte-example-app/internal/domain"
)

// FinalizeStep 完成步骤
// 更新所有日志状态为完成
type FinalizeStep struct {
	*rte.BaseStep
	repo *domain.MemoryAccountRepository
}

// NewFinalizeStep 创建完成步骤
func NewFinalizeStep(repo *domain.MemoryAccountRepository) *FinalizeStep {
	return &FinalizeStep{
		BaseStep: rte.NewBaseStep("finalize"),
		repo:     repo,
	}
}

// Execute 执行完成操作
func (s *FinalizeStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")

	// 更新扣款日志
	if debitLogID, err := rte.GetOutputAs[int64](txCtx, "debit_log_id"); err == nil && debitLogID > 0 {
		s.repo.UpdateLogStatus(debitLogID, "COMPLETE")
	}

	// 更新入账日志
	if creditLogID, err := rte.GetOutputAs[int64](txCtx, "credit_log_id"); err == nil && creditLogID > 0 {
		s.repo.UpdateLogStatus(creditLogID, "COMPLETE")
	}

	log.Printf("[Finalize] Transfer completed: ticket=%s", ticket)

	txCtx.SetOutput("finalized", true)
	return nil
}

// SupportsCompensation 不支持补偿（最后一步）
func (s *FinalizeStep) SupportsCompensation() bool {
	return false
}
