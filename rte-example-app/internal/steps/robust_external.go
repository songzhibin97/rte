// Package steps 定义事务步骤
package steps

import (
	"context"
	"errors"
	"fmt"

	"rte"
	"rte-example-app/internal/domain"
)

// ErrorCategory 错误分类
type ErrorCategory int

const (
	// ErrorRetryable 可重试错误
	ErrorRetryable ErrorCategory = iota
	// ErrorNonRetryable 不可重试错误
	ErrorNonRetryable
	// ErrorUncertain 不确定错误（操作可能已成功）
	ErrorUncertain
)

// CategorizeError 对错误进行分类
func CategorizeError(err error) ErrorCategory {
	if err == nil {
		return ErrorRetryable
	}

	// 超时错误 - 不确定（操作可能已成功）
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, domain.ErrExternalTimeout) {
		return ErrorUncertain
	}

	// 可重试错误
	if errors.Is(err, domain.ErrExternalUnavailable) {
		return ErrorRetryable
	}

	// 业务错误 - 不可重试
	if errors.Is(err, domain.ErrInsufficientBalance) ||
		errors.Is(err, domain.ErrAccountNotFound) ||
		errors.Is(err, domain.ErrInvalidAmount) {
		return ErrorNonRetryable
	}

	// 默认可重试
	return ErrorRetryable
}

// RobustExternalStep 健壮的外部服务调用步骤
// 演示正确的错误处理模式
type RobustExternalStep struct {
	*rte.BaseStep
	external domain.ExternalService
	repo     *domain.MemoryAccountRepository
}

// NewRobustExternalStep 创建健壮的外部服务步骤
func NewRobustExternalStep(external domain.ExternalService, repo *domain.MemoryAccountRepository) *RobustExternalStep {
	return &RobustExternalStep{
		BaseStep: rte.NewBaseStep("robust_external"),
		external: external,
		repo:     repo,
	}
}

// Execute 执行外部服务调用
func (s *RobustExternalStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")

	// 先验证输入（不可重试错误）
	if amount <= 0 {
		return fmt.Errorf("%w: amount must be positive", domain.ErrInvalidAmount)
	}

	// 创建待处理日志
	logID := s.repo.CreateLog(ticket, accountID, "IN", amount)
	txCtx.SetOutput("external_log_id", logID)

	// 调用外部服务
	err := s.external.Deposit(ctx, accountID, amount, ticket)
	if err != nil {
		category := CategorizeError(err)

		switch category {
		case ErrorUncertain:
			// 不确定错误（超时），检查操作是否实际成功
			// 这对幂等性至关重要
			completed, checkErr := s.external.CheckTransaction(ctx, accountID, ticket)
			if checkErr == nil && completed {
				// 操作实际成功
				txCtx.SetOutput("external_completed", true)
				txCtx.SetOutput("recovered_from_timeout", true)
				return nil
			}
			// 操作未完成，返回原始错误以便重试
			return fmt.Errorf("external operation uncertain: %w", err)

		case ErrorNonRetryable:
			// 不可重试错误，立即失败
			return fmt.Errorf("non-retryable error: %w", err)

		case ErrorRetryable:
			// 可重试错误，RTE 会根据配置处理重试
			return fmt.Errorf("retryable error: %w", err)
		}
	}

	txCtx.SetOutput("external_completed", true)
	return nil
}

// Compensate 补偿外部操作
func (s *RobustExternalStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")
	logID, _ := rte.GetOutputAs[int64](txCtx, "external_log_id")

	// 只有操作实际完成时才需要补偿
	completed, _ := rte.GetOutputAs[bool](txCtx, "external_completed")
	if !completed {
		// 操作未完成，无需补偿
		if logID > 0 {
			s.repo.UpdateLogStatus(logID, "FAILED")
		}
		return nil
	}

	// 执行补偿取款
	compensateTicket := ticket + "_compensate"
	err := s.external.Withdraw(ctx, accountID, amount, compensateTicket)
	if err != nil {
		// 补偿失败 - 这是严重问题
		// 生产环境应触发告警
		return fmt.Errorf("compensation failed: %w", err)
	}

	if logID > 0 {
		s.repo.UpdateLogStatus(logID, "COMPENSATED")
	}

	return nil
}

// SupportsCompensation 支持补偿
func (s *RobustExternalStep) SupportsCompensation() bool {
	return true
}

// IdempotencyKey 幂等性键
func (s *RobustExternalStep) IdempotencyKey(txCtx *rte.TxContext) string {
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	return fmt.Sprintf("robust_external:%s:%d", ticket, accountID)
}

// SupportsIdempotency 支持幂等性
func (s *RobustExternalStep) SupportsIdempotency() bool {
	return true
}
