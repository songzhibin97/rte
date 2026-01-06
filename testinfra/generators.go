// Package testinfra provides test infrastructure for RTE production validation.
package testinfra

import (
	"context"
	"fmt"
	"rte"
	"sync"

	"pgregory.net/rapid"
)

// Account represents a test account with balance
type Account struct {
	ID      string
	Balance int64
	mu      sync.Mutex
}

// NewAccount creates a new account with initial balance
func NewAccount(id string, balance int64) *Account {
	return &Account{ID: id, Balance: balance}
}

// Debit debits amount from account, returns error if insufficient balance
func (a *Account) Debit(amount int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.Balance < amount {
		return fmt.Errorf("insufficient balance: have %d, need %d", a.Balance, amount)
	}
	a.Balance -= amount
	return nil
}

// Credit credits amount to account
func (a *Account) Credit(amount int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Balance += amount
}

// GetBalance returns current balance
func (a *Account) GetBalance() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.Balance
}

// MockAccountStore is an in-memory account store for testing
type MockAccountStore struct {
	accounts map[string]*Account
	mu       sync.RWMutex
}

// NewMockAccountStore creates a new mock account store
func NewMockAccountStore() *MockAccountStore {
	return &MockAccountStore{
		accounts: make(map[string]*Account),
	}
}

// CreateAccount creates a new account
func (s *MockAccountStore) CreateAccount(id string, balance int64) *Account {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc := NewAccount(id, balance)
	s.accounts[id] = acc
	return acc
}

// GetAccount returns an account by ID
func (s *MockAccountStore) GetAccount(id string) (*Account, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	acc, ok := s.accounts[id]
	return acc, ok
}

// TotalBalance returns the total balance across all accounts
func (s *MockAccountStore) TotalBalance() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var total int64
	for _, acc := range s.accounts {
		total += acc.GetBalance()
	}
	return total
}

// ============================================================================
// Rapid Generators
// ============================================================================

// AccountGenerator generates random accounts for property testing
func AccountGenerator() *rapid.Generator[*Account] {
	return rapid.Custom(func(t *rapid.T) *Account {
		id := rapid.StringMatching(`^acc-[a-z0-9]{8}$`).Draw(t, "accountID")
		balance := rapid.Int64Range(0, 1000000).Draw(t, "balance")
		return NewAccount(id, balance)
	})
}

// TransferRequest represents a transfer request for testing
type TransferRequest struct {
	FromAccountID string
	ToAccountID   string
	Amount        int64
}

// TransferGenerator generates random transfer requests
func TransferGenerator(fromID, toID string, maxAmount int64) *rapid.Generator[*TransferRequest] {
	return rapid.Custom(func(t *rapid.T) *TransferRequest {
		amount := rapid.Int64Range(1, maxAmount).Draw(t, "amount")
		return &TransferRequest{
			FromAccountID: fromID,
			ToAccountID:   toID,
			Amount:        amount,
		}
	})
}

// StepOutcome represents the outcome of a step execution
type StepOutcome int

const (
	StepOutcomeSuccess StepOutcome = iota
	StepOutcomeFailure
	StepOutcomeTimeout
)

// TestStepConfig configures a test step's behavior
type TestStepConfig struct {
	Name               string
	Outcome            StepOutcome
	SupportsComp       bool
	SupportsIdempotent bool
	ExecuteFunc        func(ctx context.Context, txCtx *rte.TxContext) error
	CompensateFunc     func(ctx context.Context, txCtx *rte.TxContext) error
}

// TestStep is a configurable step for testing
type TestStep struct {
	*rte.BaseStep
	config TestStepConfig
}

// NewTestStep creates a new test step with the given config
func NewTestStep(cfg TestStepConfig) *TestStep {
	return &TestStep{
		BaseStep: rte.NewBaseStep(cfg.Name),
		config:   cfg,
	}
}

// Execute executes the step
func (s *TestStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	if s.config.ExecuteFunc != nil {
		return s.config.ExecuteFunc(ctx, txCtx)
	}
	switch s.config.Outcome {
	case StepOutcomeFailure:
		return fmt.Errorf("step %s failed", s.config.Name)
	case StepOutcomeTimeout:
		<-ctx.Done()
		return ctx.Err()
	default:
		return nil
	}
}

// Compensate compensates the step
func (s *TestStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	if s.config.CompensateFunc != nil {
		return s.config.CompensateFunc(ctx, txCtx)
	}
	if !s.config.SupportsComp {
		return rte.ErrCompensationNotSupported
	}
	return nil
}

// SupportsCompensation returns whether the step supports compensation
func (s *TestStep) SupportsCompensation() bool {
	return s.config.SupportsComp
}

// SupportsIdempotency returns whether the step supports idempotency
func (s *TestStep) SupportsIdempotency() bool {
	return s.config.SupportsIdempotent
}

// IdempotencyKey returns the idempotency key for the step
func (s *TestStep) IdempotencyKey(txCtx *rte.TxContext) string {
	if s.config.SupportsIdempotent {
		return fmt.Sprintf("%s-%s-%d", txCtx.TxID, s.config.Name, txCtx.StepIndex)
	}
	return ""
}

// ============================================================================
// Step Generators for Property Testing
// ============================================================================

// StepConfigGenerator generates random step configurations
func StepConfigGenerator() *rapid.Generator[TestStepConfig] {
	return rapid.Custom(func(t *rapid.T) TestStepConfig {
		name := rapid.StringMatching(`^step-[a-z0-9]{4}$`).Draw(t, "stepName")
		outcome := rapid.SampledFrom([]StepOutcome{
			StepOutcomeSuccess,
			StepOutcomeFailure,
		}).Draw(t, "outcome")
		supportsComp := rapid.Bool().Draw(t, "supportsComp")
		supportsIdem := rapid.Bool().Draw(t, "supportsIdem")

		return TestStepConfig{
			Name:               name,
			Outcome:            outcome,
			SupportsComp:       supportsComp,
			SupportsIdempotent: supportsIdem,
		}
	})
}

// SuccessStepConfigGenerator generates step configs that always succeed
func SuccessStepConfigGenerator() *rapid.Generator[TestStepConfig] {
	return rapid.Custom(func(t *rapid.T) TestStepConfig {
		name := rapid.StringMatching(`^step-[a-z0-9]{4}$`).Draw(t, "stepName")
		supportsComp := rapid.Bool().Draw(t, "supportsComp")
		supportsIdem := rapid.Bool().Draw(t, "supportsIdem")

		return TestStepConfig{
			Name:               name,
			Outcome:            StepOutcomeSuccess,
			SupportsComp:       supportsComp,
			SupportsIdempotent: supportsIdem,
		}
	})
}

// FailureStepConfigGenerator generates step configs that always fail
func FailureStepConfigGenerator() *rapid.Generator[TestStepConfig] {
	return rapid.Custom(func(t *rapid.T) TestStepConfig {
		name := rapid.StringMatching(`^step-[a-z0-9]{4}$`).Draw(t, "stepName")
		supportsComp := rapid.Bool().Draw(t, "supportsComp")

		return TestStepConfig{
			Name:               name,
			Outcome:            StepOutcomeFailure,
			SupportsComp:       supportsComp,
			SupportsIdempotent: false,
		}
	})
}

// CompensatableStepConfigGenerator generates step configs that support compensation
func CompensatableStepConfigGenerator() *rapid.Generator[TestStepConfig] {
	return rapid.Custom(func(t *rapid.T) TestStepConfig {
		name := rapid.StringMatching(`^step-[a-z0-9]{4}$`).Draw(t, "stepName")
		outcome := rapid.SampledFrom([]StepOutcome{
			StepOutcomeSuccess,
			StepOutcomeFailure,
		}).Draw(t, "outcome")

		return TestStepConfig{
			Name:               name,
			Outcome:            outcome,
			SupportsComp:       true,
			SupportsIdempotent: rapid.Bool().Draw(t, "supportsIdem"),
		}
	})
}

// TxIDGenerator generates unique transaction IDs
func TxIDGenerator(prefix string) *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		suffix := rapid.StringMatching(`^[a-z0-9]{8}$`).Draw(t, "txSuffix")
		return fmt.Sprintf("%s-%s", prefix, suffix)
	})
}
