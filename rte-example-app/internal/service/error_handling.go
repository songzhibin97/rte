// Package service 提供业务服务
package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"rte"
	"rte-example-app/internal/domain"
	applock "rte-example-app/internal/lock"
	"rte-example-app/internal/steps"
	"rte-example-app/internal/store"
	"rte/circuit/memory"
	"rte/event"
)

// ErrorHandlingService 错误处理演示服务
type ErrorHandlingService struct {
	engine      *rte.Engine
	accountRepo *domain.MemoryAccountRepository
	external    *domain.FailableExternalService
}

// NewErrorHandlingService 创建错误处理演示服务
func NewErrorHandlingService() *ErrorHandlingService {
	accountRepo := domain.NewMemoryAccountRepository()
	external := domain.NewFailableExternalService()

	rteStore := store.NewMemoryStore()
	locker := applock.NewMemoryLocker()
	breaker := memory.NewMemoryBreaker()
	eventBus := event.NewMemoryEventBus()

	engine := rte.NewEngine(
		rte.WithEngineStore(rteStore),
		rte.WithEngineLocker(locker),
		rte.WithEngineBreaker(breaker),
		rte.WithEngineEventBus(eventBus),
		rte.WithEngineConfig(rte.Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    50 * time.Millisecond,
		}),
	)

	// 注册步骤
	engine.RegisterStep(steps.NewDebitStep(accountRepo))
	engine.RegisterStep(steps.NewRobustExternalStep(external, accountRepo))
	engine.RegisterStep(steps.NewFinalizeStep(accountRepo))

	// 订阅事件
	engine.Subscribe(event.EventTxCompleted, func(ctx context.Context, e event.Event) error {
		log.Printf("[Event] Transaction completed: %s", e.TxID)
		return nil
	})

	engine.Subscribe(event.EventTxFailed, func(ctx context.Context, e event.Event) error {
		log.Printf("[Event] Transaction failed: %s, error=%v", e.TxID, e.Error)
		return nil
	})

	engine.Subscribe(event.EventTxCancelled, func(ctx context.Context, e event.Event) error {
		log.Printf("[Event] Transaction cancelled: %s", e.TxID)
		return nil
	})

	return &ErrorHandlingService{
		engine:      engine,
		accountRepo: accountRepo,
		external:    external,
	}
}

// CreateAccount 创建账户
func (s *ErrorHandlingService) CreateAccount(id, userID int64, accountType, currency string, balance float64) {
	s.accountRepo.CreateAccount(&domain.Account{
		ID:        id,
		UserID:    userID,
		Type:      accountType,
		Currency:  currency,
		Balance:   balance,
		UpdatedAt: time.Now(),
	})
}

// GetAccount 获取账户
func (s *ErrorHandlingService) GetAccount(id int64) (*domain.Account, error) {
	return s.accountRepo.GetAccount(id)
}

// SetFailureMode 设置外部服务失败模式
func (s *ErrorHandlingService) SetFailureMode(mode domain.FailureMode) {
	s.external.SetFailureMode(mode)
}

// ResetExternal 重置外部服务状态
func (s *ErrorHandlingService) ResetExternal() {
	s.external.Reset()
}

// Transfer 执行转账
func (s *ErrorHandlingService) Transfer(ctx context.Context, req *domain.TransferRequest) (*rte.TxResult, error) {
	if req.FromAccountID == req.ToAccountID {
		return nil, domain.ErrSameAccount
	}
	if req.Amount <= 0 {
		return nil, domain.ErrInvalidAmount
	}

	if req.Ticket == "" {
		req.Ticket = fmt.Sprintf("TRF-%d", time.Now().UnixNano())
	}

	tx, err := s.engine.NewTransaction("error_handling_transfer").
		WithLockKeys(
			fmt.Sprintf("account:%d", req.FromAccountID),
			fmt.Sprintf("account:%d", req.ToAccountID),
		).
		WithInput(map[string]any{
			"from_account_id": req.FromAccountID,
			"to_account_id":   req.ToAccountID,
			"amount":          req.Amount,
			"ticket":          req.Ticket,
			"currency":        req.Currency,
		}).
		AddStep("debit").
		AddStep("robust_external").
		AddStep("finalize").
		Build()

	if err != nil {
		return nil, fmt.Errorf("failed to build transaction: %w", err)
	}

	return s.engine.Execute(ctx, tx)
}

// RunErrorHandlingExamples 运行错误处理示例
func RunErrorHandlingExamples() {
	fmt.Println("=== 错误处理示例 ===")
	fmt.Println()

	// 示例1: 补偿后恢复
	fmt.Println("1. 永久失败后的补偿:")
	runCompensationExample()
	fmt.Println()

	// 示例2: 从不确定超时中恢复
	fmt.Println("2. 从不确定超时中恢复:")
	runUncertainTimeoutExample()
	fmt.Println()

	fmt.Println("=== 错误处理示例完成 ===")
}

func runCompensationExample() {
	svc := NewErrorHandlingService()

	svc.CreateAccount(1, 100, "saving", "USD", 1000)
	svc.CreateAccount(2, 100, "forex", "USD", 500)

	initialBalance, _ := svc.GetAccount(1)
	fmt.Printf("   初始余额 - 账户1: %.2f\n", initialBalance.Balance)

	// 设置为总是失败
	svc.SetFailureMode(domain.FailureModeAlways)

	req := &domain.TransferRequest{
		UserID:        100,
		FromAccountID: 1,
		ToAccountID:   2,
		Amount:        100.0,
		Currency:      "USD",
		Ticket:        "COMP-001",
	}

	result, _ := svc.Transfer(context.Background(), req)

	finalBalance, _ := svc.GetAccount(1)

	fmt.Printf("   事务状态: %s\n", result.Status)
	fmt.Printf("   最终余额 - 账户1: %.2f (通过补偿恢复)\n", finalBalance.Balance)
}

func runUncertainTimeoutExample() {
	svc := NewErrorHandlingService()

	svc.CreateAccount(1, 100, "saving", "USD", 1000)
	svc.CreateAccount(2, 100, "forex", "USD", 500)

	// 设置为不确定超时（操作实际成功但返回超时）
	svc.SetFailureMode(domain.FailureModeUncertain)

	req := &domain.TransferRequest{
		UserID:        100,
		FromAccountID: 1,
		ToAccountID:   2,
		Amount:        100.0,
		Currency:      "USD",
		Ticket:        "UNCERTAIN-001",
	}

	result, err := svc.Transfer(context.Background(), req)
	if err != nil {
		fmt.Printf("   错误: %v\n", err)
		return
	}

	fmt.Printf("   事务状态: %s\n", result.Status)
	fmt.Printf("   从超时中恢复: 操作已验证完成\n")
}
