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

// TransferService 转账服务
type TransferService struct {
	engine      *rte.Engine
	accountRepo *domain.MemoryAccountRepository
}

// NewTransferService 创建转账服务
func NewTransferService() *TransferService {
	// 创建账户仓储
	accountRepo := domain.NewMemoryAccountRepository()

	// 创建RTE依赖
	rteStore := store.NewMemoryStore()
	locker := applock.NewMemoryLocker()
	breaker := memory.NewMemoryBreaker()
	eventBus := event.NewMemoryEventBus()

	// 创建RTE引擎
	engine := rte.NewEngine(
		rte.WithEngineStore(rteStore),
		rte.WithEngineLocker(locker),
		rte.WithEngineBreaker(breaker),
		rte.WithEngineEventBus(eventBus),
		rte.WithEngineConfig(rte.Config{
			LockTTL:             30 * time.Second,
			LockExtendPeriod:    10 * time.Second,
			StepTimeout:         5 * time.Second,
			TxTimeout:           30 * time.Second,
			MaxRetries:          3,
			RetryInterval:       100 * time.Millisecond,
			RetryMaxInterval:    5 * time.Second,
			RetryMultiplier:     2.0,
			RetryJitter:         0.1,
			CircuitThreshold:    5,
			CircuitTimeout:      30 * time.Second,
			CircuitHalfOpenReqs: 3,
			RecoveryInterval:    30 * time.Second,
			StuckThreshold:      5 * time.Minute,
			IdempotencyTTL:      24 * time.Hour,
		}),
	)

	// 注册步骤
	engine.RegisterStep(steps.NewDebitStep(accountRepo))
	engine.RegisterStep(steps.NewCreditStep(accountRepo))
	engine.RegisterStep(steps.NewNotifyStep())
	engine.RegisterStep(steps.NewFinalizeStep(accountRepo))

	// 订阅事件
	engine.Subscribe(event.EventTxCreated, func(ctx context.Context, e event.Event) error {
		log.Printf("[Event] Transaction created: %s (type=%s)", e.TxID, e.TxType)
		return nil
	})

	engine.Subscribe(event.EventTxCompleted, func(ctx context.Context, e event.Event) error {
		log.Printf("[Event] Transaction completed: %s", e.TxID)
		return nil
	})

	engine.Subscribe(event.EventTxFailed, func(ctx context.Context, e event.Event) error {
		log.Printf("[Event] Transaction failed: %s, error=%v", e.TxID, e.Error)
		return nil
	})

	engine.Subscribe(event.EventStepStarted, func(ctx context.Context, e event.Event) error {
		log.Printf("[Event] Step started: %s (tx=%s)", e.StepName, e.TxID)
		return nil
	})

	engine.Subscribe(event.EventStepCompleted, func(ctx context.Context, e event.Event) error {
		log.Printf("[Event] Step completed: %s (tx=%s)", e.StepName, e.TxID)
		return nil
	})

	engine.Subscribe(event.EventStepFailed, func(ctx context.Context, e event.Event) error {
		log.Printf("[Event] Step failed: %s (tx=%s), error=%v", e.StepName, e.TxID, e.Error)
		return nil
	})

	return &TransferService{
		engine:      engine,
		accountRepo: accountRepo,
	}
}

// CreateAccount 创建账户
func (s *TransferService) CreateAccount(id, userID int64, accountType, currency string, balance float64) {
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
func (s *TransferService) GetAccount(id int64) (*domain.Account, error) {
	return s.accountRepo.GetAccount(id)
}

// Transfer 执行转账
func (s *TransferService) Transfer(ctx context.Context, req *domain.TransferRequest) (*rte.TxResult, error) {
	// 验证请求
	if req.FromAccountID == req.ToAccountID {
		return nil, domain.ErrSameAccount
	}
	if req.Amount <= 0 {
		return nil, domain.ErrInvalidAmount
	}

	// 生成票据号（如果未提供）
	if req.Ticket == "" {
		req.Ticket = fmt.Sprintf("TRF-%d", time.Now().UnixNano())
	}

	// 构建事务
	tx, err := s.engine.NewTransaction("transfer").
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
		WithMetadataValue("user_id", fmt.Sprintf("%d", req.UserID)).
		AddStep("debit").
		AddStep("credit").
		AddStep("notify").
		AddStep("finalize").
		Build()

	if err != nil {
		return nil, fmt.Errorf("failed to build transaction: %w", err)
	}

	// 执行事务
	return s.engine.Execute(ctx, tx)
}

// TransferWithCustomSteps 使用自定义步骤执行转账
func (s *TransferService) TransferWithCustomSteps(ctx context.Context, req *domain.TransferRequest, stepNames []string) (*rte.TxResult, error) {
	// 验证请求
	if req.FromAccountID == req.ToAccountID {
		return nil, domain.ErrSameAccount
	}
	if req.Amount <= 0 {
		return nil, domain.ErrInvalidAmount
	}

	// 生成票据号
	if req.Ticket == "" {
		req.Ticket = fmt.Sprintf("TRF-%d", time.Now().UnixNano())
	}

	// 构建事务
	builder := s.engine.NewTransaction("transfer").
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
		})

	for _, stepName := range stepNames {
		builder.AddStep(stepName)
	}

	tx, err := builder.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to build transaction: %w", err)
	}

	return s.engine.Execute(ctx, tx)
}

// Engine 返回RTE引擎
func (s *TransferService) Engine() *rte.Engine {
	return s.engine
}
