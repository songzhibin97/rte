// Package examples demonstrates error handling patterns in RTE.
//
// This example shows how to handle various error scenarios including:
// - Retryable errors (network timeouts, temporary failures)
// - Non-retryable errors (insufficient balance, invalid input)
// - Uncertain errors (timeout but operation may have succeeded)
// - Compensation on failure
package examples

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"rte"
	"rte/circuit/memory"
	"rte/event"
)

// ============================================================================
// Error Categories
// ============================================================================

// ErrorCategory defines the type of error for handling decisions
type ErrorCategory int

const (
	// ErrorRetryable indicates the error is temporary and can be retried
	ErrorRetryable ErrorCategory = iota
	// ErrorNonRetryable indicates the error is permanent and should not be retried
	ErrorNonRetryable
	// ErrorUncertain indicates the operation may have succeeded despite the error
	ErrorUncertain
)

// Common error types
var (
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrAccountNotFound     = errors.New("account not found")
	ErrInvalidAmount       = errors.New("invalid amount")
	ErrExternalTimeout     = errors.New("external service timeout")
	ErrExternalUnavailable = errors.New("external service unavailable")
)

// CategorizeError determines how to handle an error
func CategorizeError(err error) ErrorCategory {
	if err == nil {
		return ErrorRetryable // No error, shouldn't happen
	}

	// Check for timeout errors (uncertain - operation may have succeeded)
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrExternalTimeout) {
		return ErrorUncertain
	}

	// Check for retryable errors
	if errors.Is(err, ErrExternalUnavailable) {
		return ErrorRetryable
	}

	// Business errors are non-retryable
	if errors.Is(err, ErrInsufficientBalance) ||
		errors.Is(err, ErrAccountNotFound) ||
		errors.Is(err, ErrInvalidAmount) {
		return ErrorNonRetryable
	}

	// Default to retryable for unknown errors
	return ErrorRetryable
}

// ============================================================================
// External Service with Failure Modes
// ============================================================================

// FailureMode defines how the mock service should behave
type FailureMode int

const (
	FailureModeNone      FailureMode = iota // Always succeed
	FailureModeAlways                       // Always fail
	FailureModeOnce                         // Fail once then succeed
	FailureModeTimeout                      // Timeout error
	FailureModeUncertain                    // Timeout but actually succeeded
)

// FailableExternalService simulates an external service with configurable failures
type FailableExternalService struct {
	mu           sync.Mutex
	failureMode  FailureMode
	failureCount int
	transactions map[string]bool
}

// NewFailableExternalService creates a new failable external service
func NewFailableExternalService() *FailableExternalService {
	return &FailableExternalService{
		transactions: make(map[string]bool),
	}
}

// SetFailureMode configures the failure behavior
func (s *FailableExternalService) SetFailureMode(mode FailureMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failureMode = mode
	s.failureCount = 0
}

// Deposit performs a deposit with configurable failure behavior
func (s *FailableExternalService) Deposit(ctx context.Context, accountID int64, amount float64, ticket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.failureMode {
	case FailureModeAlways:
		return ErrExternalUnavailable
	case FailureModeOnce:
		if s.failureCount == 0 {
			s.failureCount++
			return ErrExternalUnavailable
		}
	case FailureModeTimeout:
		return ErrExternalTimeout
	case FailureModeUncertain:
		// Actually succeed but return timeout
		s.transactions[ticket] = true
		return ErrExternalTimeout
	}

	s.transactions[ticket] = true
	return nil
}

// Withdraw performs a withdrawal with configurable failure behavior
func (s *FailableExternalService) Withdraw(ctx context.Context, accountID int64, amount float64, ticket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.failureMode {
	case FailureModeAlways:
		return ErrExternalUnavailable
	case FailureModeOnce:
		if s.failureCount == 0 {
			s.failureCount++
			return ErrExternalUnavailable
		}
	case FailureModeTimeout:
		return ErrExternalTimeout
	case FailureModeUncertain:
		s.transactions[ticket] = true
		return ErrExternalTimeout
	}

	s.transactions[ticket] = true
	return nil
}

// CheckTransaction checks if a transaction exists (for idempotency verification)
func (s *FailableExternalService) CheckTransaction(ctx context.Context, accountID int64, ticket string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transactions[ticket], nil
}

// Reset clears all state
func (s *FailableExternalService) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failureMode = FailureModeNone
	s.failureCount = 0
	s.transactions = make(map[string]bool)
}

// ============================================================================
// Step with Error Handling
// ============================================================================

// RobustExternalStep demonstrates proper error handling in a step
type RobustExternalStep struct {
	*rte.BaseStep
	external *FailableExternalService
	store    *AccountStore
}

// NewRobustExternalStep creates a new robust external step
func NewRobustExternalStep(external *FailableExternalService, store *AccountStore) *RobustExternalStep {
	return &RobustExternalStep{
		BaseStep: rte.NewBaseStep("robust_external"),
		external: external,
		store:    store,
	}
}

// Execute performs the external operation with proper error handling
func (s *RobustExternalStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")

	// Validate input first (non-retryable errors)
	if amount <= 0 {
		return fmt.Errorf("%w: amount must be positive", ErrInvalidAmount)
	}

	// Create pending log
	logID := s.store.CreatePendingLog(ticket, accountID, "IN", amount)
	txCtx.SetOutput("external_log_id", logID)

	// Call external service
	err := s.external.Deposit(ctx, accountID, amount, ticket)
	if err != nil {
		category := CategorizeError(err)

		switch category {
		case ErrorUncertain:
			// On uncertain error (timeout), check if operation actually succeeded
			// This is critical for idempotency
			completed, checkErr := s.external.CheckTransaction(ctx, accountID, ticket)
			if checkErr == nil && completed {
				// Operation succeeded despite timeout
				txCtx.SetOutput("external_completed", true)
				txCtx.SetOutput("recovered_from_timeout", true)
				return nil
			}
			// Operation did not complete, return original error for retry
			return fmt.Errorf("external operation uncertain: %w", err)

		case ErrorNonRetryable:
			// Non-retryable error, fail immediately
			return fmt.Errorf("non-retryable error: %w", err)

		case ErrorRetryable:
			// Retryable error, RTE will handle retry based on config
			return fmt.Errorf("retryable error: %w", err)
		}
	}

	txCtx.SetOutput("external_completed", true)
	return nil
}

// Compensate reverses the external operation
func (s *RobustExternalStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")
	logID, _ := rte.GetOutputAs[int64](txCtx, "external_log_id")

	// Only compensate if the operation actually completed
	completed, _ := rte.GetOutputAs[bool](txCtx, "external_completed")
	if !completed {
		// Operation never completed, no compensation needed
		if logID > 0 {
			s.store.UpdateLogStatus(logID, "FAILED")
		}
		return nil
	}

	// Perform compensation withdrawal
	compensateTicket := ticket + "_compensate"
	err := s.external.Withdraw(ctx, accountID, amount, compensateTicket)
	if err != nil {
		// Compensation failed - this is critical
		// In production, this should trigger alerts
		return fmt.Errorf("compensation failed: %w", err)
	}

	if logID > 0 {
		s.store.UpdateLogStatus(logID, "COMPENSATED")
	}

	return nil
}

// SupportsCompensation returns true
func (s *RobustExternalStep) SupportsCompensation() bool {
	return true
}

// IdempotencyKey returns a unique key for idempotency
func (s *RobustExternalStep) IdempotencyKey(txCtx *rte.TxContext) string {
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	return fmt.Sprintf("robust_external:%s:%d", ticket, accountID)
}

// SupportsIdempotency returns true
func (s *RobustExternalStep) SupportsIdempotency() bool {
	return true
}

// ============================================================================
// Example: Handling Different Error Scenarios
// ============================================================================

// RunErrorHandlingExample demonstrates various error handling scenarios
func RunErrorHandlingExample() error {
	fmt.Println("=== Error Handling Examples ===")

	// Example 1: Successful retry after temporary failure
	fmt.Println("1. Retry after temporary failure:")
	if err := runRetryExample(); err != nil {
		fmt.Printf("   Error: %v\n", err)
	}
	fmt.Println()

	// Example 2: Compensation after permanent failure
	fmt.Println("2. Compensation after permanent failure:")
	if err := runCompensationExample(); err != nil {
		fmt.Printf("   Error: %v\n", err)
	}
	fmt.Println()

	// Example 3: Recovery from uncertain timeout
	fmt.Println("3. Recovery from uncertain timeout:")
	if err := runUncertainTimeoutExample(); err != nil {
		fmt.Printf("   Error: %v\n", err)
	}
	fmt.Println()

	return nil
}

// runRetryExample demonstrates that RTE handles temporary failures
// Note: RTE's retry mechanism works at the transaction level via the recovery worker,
// not at the step level within a single execution. For step-level retry,
// implement retry logic within the step itself.
func runRetryExample() error {
	accountStore := NewAccountStore()
	externalService := NewFailableExternalService()

	// Configure to fail once then succeed - but since RTE doesn't retry steps
	// within a single execution, we'll show the compensation path instead
	externalService.SetFailureMode(FailureModeOnce)

	accountStore.CreateAccount(&Account{ID: 1, Balance: 1000})
	accountStore.CreateAccount(&Account{ID: 2, Balance: 500})

	engine := createTestEngine(accountStore, externalService)

	// Register steps
	engine.RegisterStep(NewDebitSavingStep(accountStore))
	engine.RegisterStep(NewRobustExternalStep(externalService, accountStore))
	engine.RegisterStep(NewFinalizeTransferStep(accountStore))

	tx, _ := engine.NewTransaction("retry_test").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          100.0,
			"ticket":          "RETRY-001",
		}).
		AddStep("debit_saving").
		AddStep("robust_external").
		AddStep("finalize_transfer").
		Build()

	result, _ := engine.Execute(context.Background(), tx)

	// The transaction will be compensated since the step failed
	// In production, the recovery worker would retry failed transactions
	fmt.Printf("   Status: %s\n", result.Status)
	fmt.Printf("   Note: Step-level retry requires custom implementation or recovery worker\n")
	return nil
}

// runCompensationExample demonstrates compensation after failure
func runCompensationExample() error {
	accountStore := NewAccountStore()
	externalService := NewFailableExternalService()

	// Configure to always fail
	externalService.SetFailureMode(FailureModeAlways)

	accountStore.CreateAccount(&Account{ID: 1, Balance: 1000})
	accountStore.CreateAccount(&Account{ID: 2, Balance: 500})

	initialBalance, _ := accountStore.GetAccount(1)

	engine := createTestEngine(accountStore, externalService)

	engine.RegisterStep(NewDebitSavingStep(accountStore))
	engine.RegisterStep(NewRobustExternalStep(externalService, accountStore))
	engine.RegisterStep(NewFinalizeTransferStep(accountStore))

	tx, _ := engine.NewTransaction("compensation_test").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          100.0,
			"ticket":          "COMP-001",
		}).
		AddStep("debit_saving").
		AddStep("robust_external").
		AddStep("finalize_transfer").
		Build()

	result, _ := engine.Execute(context.Background(), tx)

	finalBalance, _ := accountStore.GetAccount(1)

	fmt.Printf("   Status: %s\n", result.Status)
	fmt.Printf("   Account 1 balance: %.2f -> %.2f (restored via compensation)\n",
		initialBalance.Balance, finalBalance.Balance)

	return nil
}

// runUncertainTimeoutExample demonstrates recovery from uncertain timeout
func runUncertainTimeoutExample() error {
	accountStore := NewAccountStore()
	externalService := NewFailableExternalService()

	// Configure uncertain timeout (operation succeeds but returns timeout)
	externalService.SetFailureMode(FailureModeUncertain)

	accountStore.CreateAccount(&Account{ID: 1, Balance: 1000})
	accountStore.CreateAccount(&Account{ID: 2, Balance: 500})

	engine := createTestEngine(accountStore, externalService)

	engine.RegisterStep(NewDebitSavingStep(accountStore))
	engine.RegisterStep(NewRobustExternalStep(externalService, accountStore))
	engine.RegisterStep(NewFinalizeTransferStep(accountStore))

	tx, _ := engine.NewTransaction("uncertain_test").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          100.0,
			"ticket":          "UNCERTAIN-001",
		}).
		AddStep("debit_saving").
		AddStep("robust_external").
		AddStep("finalize_transfer").
		Build()

	result, err := engine.Execute(context.Background(), tx)
	if err != nil {
		return err
	}

	fmt.Printf("   Status: %s\n", result.Status)
	fmt.Printf("   Recovered from timeout: operation was verified as completed\n")

	return nil
}

// createTestEngine creates an RTE engine for testing
func createTestEngine(accountStore *AccountStore, externalService *FailableExternalService) *rte.Engine {
	rteStore := newMockRTEStore()
	locker := newMockLocker()
	breaker := memory.NewMemoryBreaker()
	eventBus := event.NewMemoryEventBus()

	return rte.NewEngine(
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
			RetryInterval:    50 * time.Millisecond, // Fast retry for demo
		}),
	)
}

// ============================================================================
// Best Practices Summary
// ============================================================================

/*
Error Handling Best Practices in RTE:

1. CATEGORIZE ERRORS
   - Retryable: Network issues, temporary unavailability
   - Non-retryable: Business logic errors, validation failures
   - Uncertain: Timeouts where operation may have succeeded

2. HANDLE UNCERTAIN ERRORS
   - On timeout, always check if operation actually completed
   - Use idempotency keys to verify operation status
   - This prevents duplicate operations on retry

3. IMPLEMENT PROPER COMPENSATION
   - Only compensate operations that actually completed
   - Use idempotency for compensation operations too
   - Log compensation attempts for audit trail

4. CONFIGURE RETRY APPROPRIATELY
   - Set reasonable MaxRetries (typically 3-5)
   - Use exponential backoff for production
   - Set appropriate timeouts per step

5. MONITOR AND ALERT
   - Subscribe to EventTxFailed and EventTxCompensationFailed
   - Alert on compensation failures (critical)
   - Track retry rates for service health

Example Configuration:
```go
rte.Config{
    MaxRetries:    3,
    RetryInterval: 1 * time.Second,  // Consider exponential backoff
    StepTimeout:   10 * time.Second,
    TxTimeout:     5 * time.Minute,
}
```
*/
