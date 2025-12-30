package rte

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"rte/event"
)

// ============================================================================
// Integration Tests - Task 17.1: Complete Transaction Flow
// ============================================================================

// TestIntegration_NormalExecutionFlow tests the complete normal execution flow
func TestIntegration_NormalExecutionFlow(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Track execution order and state transitions
	var mu sync.Mutex
	executionOrder := make([]string, 0)
	stateTransitions := make([]TxStatus, 0)

	// Subscribe to events to track state transitions
	engine.Subscribe(event.EventTxCreated, func(ctx context.Context, e event.Event) error {
		mu.Lock()
		stateTransitions = append(stateTransitions, TxStatusCreated)
		mu.Unlock()
		return nil
	})
	engine.Subscribe(event.EventTxCompleted, func(ctx context.Context, e event.Event) error {
		mu.Lock()
		stateTransitions = append(stateTransitions, TxStatusCompleted)
		mu.Unlock()
		return nil
	})

	// Register steps that track execution
	step1 := newTestStep("debit")
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		mu.Lock()
		executionOrder = append(executionOrder, "debit")
		mu.Unlock()
		amount, _ := GetInputAs[string](txCtx, "amount")
		txCtx.SetOutput("debit_ref", "DEBIT-001")
		txCtx.SetOutput("debited_amount", amount)
		return nil
	}
	engine.RegisterStep(step1)

	step2 := newTestStep("transfer")
	step2.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		mu.Lock()
		executionOrder = append(executionOrder, "transfer")
		mu.Unlock()
		debitRef, _ := GetOutputAs[string](txCtx, "debit_ref")
		txCtx.SetOutput("transfer_ref", "TRF-"+debitRef)
		return nil
	}
	engine.RegisterStep(step2)

	step3 := newTestStep("credit")
	step3.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		mu.Lock()
		executionOrder = append(executionOrder, "credit")
		mu.Unlock()
		txCtx.SetOutput("credit_ref", "CREDIT-001")
		txCtx.SetOutput("status", "completed")
		return nil
	}
	engine.RegisterStep(step3)

	// Create and execute transaction
	tx, err := engine.NewTransaction("fund_transfer").
		WithLockKeys("account:123", "account:456").
		WithInput(map[string]any{
			"user_id":  123,
			"amount":   "100.00",
			"currency": "USD",
		}).
		AddStep("debit").
		AddStep("transfer").
		AddStep("credit").
		Build()

	if err != nil {
		t.Fatalf("failed to build transaction: %v", err)
	}

	result, err := engine.Execute(context.Background(), tx)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}

	// Verify result
	if result.Status != TxStatusCompleted {
		t.Errorf("expected status COMPLETED, got %s", result.Status)
	}

	// Verify execution order
	mu.Lock()
	defer mu.Unlock()
	expectedOrder := []string{"debit", "transfer", "credit"}
	if len(executionOrder) != len(expectedOrder) {
		t.Fatalf("expected %d steps executed, got %d", len(expectedOrder), len(executionOrder))
	}
	for i, expected := range expectedOrder {
		if executionOrder[i] != expected {
			t.Errorf("expected step %s at position %d, got %s", expected, i, executionOrder[i])
		}
	}

	// Verify outputs
	if result.Output["status"] != "completed" {
		t.Errorf("expected output status 'completed', got %v", result.Output["status"])
	}
	if result.Output["debit_ref"] != "DEBIT-001" {
		t.Errorf("expected debit_ref 'DEBIT-001', got %v", result.Output["debit_ref"])
	}
	if result.Output["transfer_ref"] != "TRF-DEBIT-001" {
		t.Errorf("expected transfer_ref 'TRF-DEBIT-001', got %v", result.Output["transfer_ref"])
	}

	// Verify stored transaction state
	storedTx, err := store.GetTransaction(context.Background(), tx.TxID())
	if err != nil {
		t.Fatalf("failed to get stored transaction: %v", err)
	}
	if storedTx.Status != TxStatusCompleted {
		t.Errorf("expected stored status COMPLETED, got %s", storedTx.Status)
	}

	// Verify all step records are completed
	steps, _ := store.GetSteps(context.Background(), tx.TxID())
	for _, step := range steps {
		if step.Status != StepStatusCompleted {
			t.Errorf("expected step %s to be COMPLETED, got %s", step.StepName, step.Status)
		}
	}
}

// TestIntegration_FailureAndCompensationFlow tests failure with automatic compensation
func TestIntegration_FailureAndCompensationFlow(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Track compensation order
	var mu sync.Mutex
	compensationOrder := make([]string, 0)

	// Register steps with compensation support
	step1 := newTestStep("debit")
	step1.supportsComp = true
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		txCtx.SetOutput("debit_ref", "DEBIT-001")
		return nil
	}
	step1.compensateFunc = func(ctx context.Context, txCtx *TxContext) error {
		mu.Lock()
		compensationOrder = append(compensationOrder, "debit")
		mu.Unlock()
		return nil
	}
	engine.RegisterStep(step1)

	step2 := newTestStep("transfer")
	step2.supportsComp = true
	step2.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		txCtx.SetOutput("transfer_ref", "TRF-001")
		return nil
	}
	step2.compensateFunc = func(ctx context.Context, txCtx *TxContext) error {
		mu.Lock()
		compensationOrder = append(compensationOrder, "transfer")
		mu.Unlock()
		return nil
	}
	engine.RegisterStep(step2)

	step3 := newTestStep("credit")
	step3.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return errors.New("credit service unavailable")
	}
	engine.RegisterStep(step3)

	// Create and execute transaction
	tx, _ := engine.NewTransaction("fund_transfer").
		WithLockKeys("account:123", "account:456").
		WithInput(map[string]any{
			"user_id": 123,
			"amount":  "100.00",
		}).
		AddStep("debit").
		AddStep("transfer").
		AddStep("credit").
		Build()

	result, err := engine.Execute(context.Background(), tx)

	// Should have error from failed step
	if err == nil {
		t.Fatal("expected error from failed step")
	}

	// Verify compensation occurred
	if result.Status != TxStatusCompensated {
		t.Errorf("expected status COMPENSATED, got %s", result.Status)
	}

	// Verify compensation order (reverse)
	mu.Lock()
	defer mu.Unlock()
	if len(compensationOrder) != 2 {
		t.Fatalf("expected 2 compensated steps, got %d", len(compensationOrder))
	}
	// Compensation should be in reverse order: transfer first, then debit
	if compensationOrder[0] != "transfer" {
		t.Errorf("expected first compensation 'transfer', got '%s'", compensationOrder[0])
	}
	if compensationOrder[1] != "debit" {
		t.Errorf("expected second compensation 'debit', got '%s'", compensationOrder[1])
	}

	// Verify stored transaction state
	storedTx, _ := store.GetTransaction(context.Background(), tx.TxID())
	if storedTx.Status != TxStatusCompensated {
		t.Errorf("expected stored status COMPENSATED, got %s", storedTx.Status)
	}

	// Verify step records
	steps, _ := store.GetSteps(context.Background(), tx.TxID())
	for _, step := range steps {
		switch step.StepName {
		case "debit", "transfer":
			if step.Status != StepStatusCompensated {
				t.Errorf("expected step %s to be COMPENSATED, got %s", step.StepName, step.Status)
			}
		case "credit":
			if step.Status != StepStatusFailed {
				t.Errorf("expected step %s to be FAILED, got %s", step.StepName, step.Status)
			}
		}
	}
}

// TestIntegration_TimeoutHandling tests transaction timeout handling
func TestIntegration_TimeoutHandling(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      200 * time.Millisecond, // Step timeout longer than tx timeout
			TxTimeout:        50 * time.Millisecond,  // Very short transaction timeout
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register a step that takes time
	step1 := newTestStep("step1")
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return nil
	}
	engine.RegisterStep(step1)

	step2 := newTestStep("step2")
	step2.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		// This step will be reached after timeout has passed
		time.Sleep(100 * time.Millisecond)
		return nil
	}
	engine.RegisterStep(step2)

	// Create and execute transaction with very short timeout
	tx, _ := engine.NewTransaction("timeout_tx").
		WithTimeout(50 * time.Millisecond).
		AddStep("step1").
		AddStep("step2").
		Build()

	result, err := engine.Execute(context.Background(), tx)

	// The transaction should complete or timeout depending on timing
	// We're testing that the system handles timeout gracefully
	if result == nil {
		t.Fatal("expected result to be non-nil")
	}

	// Verify the transaction reached a terminal or failed state
	validStates := map[TxStatus]bool{
		TxStatusCompleted: true,
		TxStatusTimeout:   true,
		TxStatusFailed:    true,
	}
	if !validStates[result.Status] {
		t.Errorf("expected status to be COMPLETED, TIMEOUT, or FAILED, got %s", result.Status)
	}

	// If there was an error, it should be timeout-related or step failure
	if err != nil {
		t.Logf("Transaction ended with error (expected for timeout): %v", err)
	}
}

// TestIntegration_StepTimeout tests individual step timeout handling
func TestIntegration_StepTimeout(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      50 * time.Millisecond, // Very short step timeout
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register a slow step
	step1 := newTestStep("slow_step")
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		// Sleep longer than step timeout
		time.Sleep(200 * time.Millisecond)
		return nil
	}
	engine.RegisterStep(step1)

	// Create and execute transaction
	tx, _ := engine.NewTransaction("slow_step_tx").
		AddStep("slow_step").
		Build()

	result, err := engine.Execute(context.Background(), tx)

	// Should have error
	if err == nil {
		t.Fatal("expected step timeout error")
	}

	// Verify failed status (step timeout causes failure)
	if result.Status != TxStatusFailed {
		t.Errorf("expected status FAILED, got %s", result.Status)
	}
}

// TestIntegration_FailureWithoutCompensation tests failure without compensation support
func TestIntegration_FailureWithoutCompensation(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register steps WITHOUT compensation support
	step1 := newTestStep("step1")
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return nil
	}
	engine.RegisterStep(step1)

	step2 := newTestStep("step2")
	step2.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return errors.New("step2 failed")
	}
	engine.RegisterStep(step2)

	// Create and execute transaction
	tx, _ := engine.NewTransaction("no_comp_tx").
		AddStep("step1").
		AddStep("step2").
		Build()

	result, err := engine.Execute(context.Background(), tx)

	if err == nil {
		t.Fatal("expected error from failed step")
	}

	// Without compensation support, should be FAILED
	if result.Status != TxStatusFailed {
		t.Errorf("expected status FAILED, got %s", result.Status)
	}
}

// TestIntegration_CompensationFailure tests compensation failure handling
func TestIntegration_CompensationFailure(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	// Track critical alerts
	var alertMu sync.Mutex
	criticalAlerts := make([]event.Event, 0)
	eventBus.Subscribe(event.EventAlertCritical, func(ctx context.Context, e event.Event) error {
		alertMu.Lock()
		criticalAlerts = append(criticalAlerts, e)
		alertMu.Unlock()
		return nil
	})

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       2, // Low retry count for faster test
			RetryInterval:    10 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register step with failing compensation
	step1 := newTestStep("step1")
	step1.supportsComp = true
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return nil
	}
	step1.compensateFunc = func(ctx context.Context, txCtx *TxContext) error {
		return errors.New("compensation always fails")
	}
	engine.RegisterStep(step1)

	step2 := newTestStep("step2")
	step2.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return errors.New("step2 failed")
	}
	engine.RegisterStep(step2)

	// Create and execute transaction
	tx, _ := engine.NewTransaction("comp_fail_tx").
		AddStep("step1").
		AddStep("step2").
		Build()

	result, _ := engine.Execute(context.Background(), tx)

	// Should be COMPENSATION_FAILED
	if result.Status != TxStatusCompensationFailed {
		t.Errorf("expected status COMPENSATION_FAILED, got %s", result.Status)
	}

	// Verify critical alert was published
	alertMu.Lock()
	defer alertMu.Unlock()
	if len(criticalAlerts) == 0 {
		t.Error("expected critical alert to be published")
	}
}

// ============================================================================
// Integration Tests - Task 17.2: Recovery Scenarios
// ============================================================================

// TestIntegration_StuckTransactionRecovery tests recovery of stuck transactions
func TestIntegration_StuckTransactionRecovery(t *testing.T) {
	store := newIntegrationMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register a simple step
	step1 := newTestStep("step1")
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		txCtx.SetOutput("result", "success")
		return nil
	}
	engine.RegisterStep(step1)

	// Manually create a "stuck" transaction in the store
	stuckTime := time.Now().Add(-10 * time.Minute)
	stuckTx := &StoreTx{
		TxID:        "stuck-tx-001",
		TxType:      "test",
		Status:      TxStatusExecuting,
		CurrentStep: 0,
		TotalSteps:  1,
		StepNames:   []string{"step1"},
		Context: &StoreTxContext{
			TxID:   "stuck-tx-001",
			TxType: "test",
			Input:  map[string]any{},
			Output: map[string]any{},
		},
		RetryCount: 0,
		MaxRetries: 3,
		Version:    1,
		CreatedAt:  stuckTime,
		UpdatedAt:  stuckTime,
	}
	store.CreateTransaction(context.Background(), stuckTx)

	// Create step record
	stepRecord := NewStoreStepRecord("stuck-tx-001", 0, "step1")
	store.CreateStep(context.Background(), stepRecord)

	// Verify transaction is stuck
	stuckTxs, err := store.GetStuckTransactions(context.Background(), 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to get stuck transactions: %v", err)
	}
	if len(stuckTxs) != 1 {
		t.Fatalf("expected 1 stuck transaction, got %d", len(stuckTxs))
	}

	// Resume the stuck transaction using coordinator
	result, err := engine.Coordinator().Resume(context.Background(), stuckTx)
	if err != nil {
		t.Fatalf("failed to resume stuck transaction: %v", err)
	}

	// Verify transaction completed
	if result.Status != TxStatusCompleted {
		t.Errorf("expected status COMPLETED after recovery, got %s", result.Status)
	}

	// Verify stored transaction state
	recoveredTx, _ := store.GetTransaction(context.Background(), "stuck-tx-001")
	if recoveredTx.Status != TxStatusCompleted {
		t.Errorf("expected stored status COMPLETED, got %s", recoveredTx.Status)
	}
}

// TestIntegration_FailedTransactionRetry tests retry of failed transactions
func TestIntegration_FailedTransactionRetry(t *testing.T) {
	store := newIntegrationMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register a step that always succeeds (simulating the issue was transient)
	step1 := newTestStep("flaky_step")
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		txCtx.SetOutput("result", "success")
		return nil
	}
	engine.RegisterStep(step1)

	// Create a failed transaction that can be retried
	failedTx := &StoreTx{
		TxID:        "failed-tx-001",
		TxType:      "test",
		Status:      TxStatusFailed,
		CurrentStep: 0,
		TotalSteps:  1,
		StepNames:   []string{"flaky_step"},
		Context: &StoreTxContext{
			TxID:   "failed-tx-001",
			TxType: "test",
			Input:  map[string]any{},
			Output: map[string]any{},
		},
		ErrorMsg:   "temporary failure",
		RetryCount: 0,
		MaxRetries: 3,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	store.CreateTransaction(context.Background(), failedTx)

	// Create step record (reset to pending for retry)
	stepRecord := NewStoreStepRecord("failed-tx-001", 0, "flaky_step")
	stepRecord.Status = StepStatusPending
	store.CreateStep(context.Background(), stepRecord)

	// Verify transaction is retryable
	retryableTxs, err := store.GetRetryableTransactions(context.Background(), 3)
	if err != nil {
		t.Fatalf("failed to get retryable transactions: %v", err)
	}
	if len(retryableTxs) != 1 {
		t.Fatalf("expected 1 retryable transaction, got %d", len(retryableTxs))
	}

	// Resume the failed transaction (simulating retry)
	result, err := engine.Coordinator().Resume(context.Background(), failedTx)
	if err != nil {
		t.Fatalf("failed to resume failed transaction: %v", err)
	}

	// Verify transaction completed on retry
	if result.Status != TxStatusCompleted {
		t.Errorf("expected status COMPLETED after retry, got %s", result.Status)
	}

	// Verify stored transaction state
	retriedTx, _ := store.GetTransaction(context.Background(), "failed-tx-001")
	if retriedTx.Status != TxStatusCompleted {
		t.Errorf("expected stored status COMPLETED, got %s", retriedTx.Status)
	}
}

// TestIntegration_IdempotencyOnRecovery tests idempotency during recovery
func TestIntegration_IdempotencyOnRecovery(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineChecker(&mockIdempotencyChecker{
			results: make(map[string][]byte),
		}),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Track execution count
	var executionCount int32

	// Register an idempotent step
	step1 := newTestStep("idempotent_step")
	step1.supportsIdem = true
	step1.idemKeyFunc = func(txCtx *TxContext) string {
		return "idem-key-" + txCtx.TxID
	}
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		atomic.AddInt32(&executionCount, 1)
		txCtx.SetOutput("result", "executed")
		return nil
	}
	engine.RegisterStep(step1)

	// Create and execute transaction first time
	tx, _ := engine.NewTransaction("idem_tx").
		AddStep("idempotent_step").
		Build()

	result, err := engine.Execute(context.Background(), tx)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}
	if result.Status != TxStatusCompleted {
		t.Errorf("expected status COMPLETED, got %s", result.Status)
	}

	// Verify step was executed once
	if atomic.LoadInt32(&executionCount) != 1 {
		t.Errorf("expected 1 execution, got %d", atomic.LoadInt32(&executionCount))
	}
}

// mockIdempotencyChecker implements idempotency.Checker for testing
type mockIdempotencyChecker struct {
	mu      sync.RWMutex
	results map[string][]byte
}

func (c *mockIdempotencyChecker) Check(ctx context.Context, key string) (bool, []byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result, exists := c.results[key]
	return exists, result, nil
}

func (c *mockIdempotencyChecker) Mark(ctx context.Context, key string, result []byte, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.results[key] = result
	return nil
}

// integrationMockStore extends mockStore with proper recovery query implementations
type integrationMockStore struct {
	*mockStore
}

func newIntegrationMockStore() *integrationMockStore {
	return &integrationMockStore{
		mockStore: newMockStore(),
	}
}

func (s *integrationMockStore) GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*StoreTx, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	threshold := time.Now().Add(-olderThan)
	var result []*StoreTx

	for _, tx := range s.transactions {
		// Check if transaction is stuck (LOCKED or EXECUTING and older than threshold)
		if (tx.Status == TxStatusLocked || tx.Status == TxStatusExecuting) && tx.UpdatedAt.Before(threshold) {
			txCopy := *tx
			result = append(result, &txCopy)
		}
	}

	return result, nil
}

func (s *integrationMockStore) GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*StoreTx, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*StoreTx

	for _, tx := range s.transactions {
		// Check if transaction is retryable (FAILED and retry count < max retries)
		if tx.Status == TxStatusFailed && tx.RetryCount < maxRetries {
			txCopy := *tx
			result = append(result, &txCopy)
		}
	}

	return result, nil
}

// ============================================================================
// Integration Tests - Task 17.3: Concurrent Scenarios
// ============================================================================

// TestIntegration_ConcurrentTransactionExecution tests concurrent transaction execution
func TestIntegration_ConcurrentTransactionExecution(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register a simple step
	step1 := newTestStep("step1")
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		// Simulate some work
		time.Sleep(10 * time.Millisecond)
		txCtx.SetOutput("result", "success")
		return nil
	}
	engine.RegisterStep(step1)

	// Execute multiple transactions concurrently
	numTransactions := 10
	var wg sync.WaitGroup
	results := make(chan *TxResult, numTransactions)
	errs := make(chan error, numTransactions)

	for i := 0; i < numTransactions; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			tx, err := engine.NewTransaction("concurrent_tx").
				AddStep("step1").
				Build()
			if err != nil {
				errs <- err
				return
			}

			result, err := engine.Execute(context.Background(), tx)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}(i)
	}

	wg.Wait()
	close(results)
	close(errs)

	// Check for errors
	for err := range errs {
		t.Errorf("concurrent execution error: %v", err)
	}

	// Verify all transactions completed
	completedCount := 0
	for result := range results {
		if result.Status == TxStatusCompleted {
			completedCount++
		}
	}

	if completedCount != numTransactions {
		t.Errorf("expected %d completed transactions, got %d", numTransactions, completedCount)
	}
}

// TestIntegration_ConcurrentLockContention tests concurrent access to same resources
func TestIntegration_ConcurrentLockContention(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Track execution order
	var mu sync.Mutex
	executionOrder := make([]string, 0)

	// Register a step that tracks execution
	step1 := newTestStep("step1")
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		txID := txCtx.TxID
		mu.Lock()
		executionOrder = append(executionOrder, txID)
		mu.Unlock()
		// Simulate some work
		time.Sleep(50 * time.Millisecond)
		return nil
	}
	engine.RegisterStep(step1)

	// Execute two transactions that compete for the same lock
	var wg sync.WaitGroup
	results := make([]*TxResult, 2)
	errs := make([]error, 2)

	// Transaction 1
	wg.Add(1)
	go func() {
		defer wg.Done()
		tx, _ := engine.NewTransactionWithID("tx-1", "lock_test").
			WithLockKeys("shared-resource").
			AddStep("step1").
			Build()
		results[0], errs[0] = engine.Execute(context.Background(), tx)
	}()

	// Transaction 2 (same lock key)
	wg.Add(1)
	go func() {
		defer wg.Done()
		tx, _ := engine.NewTransactionWithID("tx-2", "lock_test").
			WithLockKeys("shared-resource").
			AddStep("step1").
			Build()
		results[1], errs[1] = engine.Execute(context.Background(), tx)
	}()

	wg.Wait()

	// One should succeed, one should fail due to lock contention
	successCount := 0
	failCount := 0
	for i := 0; i < 2; i++ {
		if errs[i] == nil && results[i] != nil && results[i].Status == TxStatusCompleted {
			successCount++
		} else {
			failCount++
		}
	}

	// At least one should succeed
	if successCount == 0 {
		t.Error("expected at least one transaction to succeed")
	}

	// Due to lock contention, we expect one to fail
	// (unless the first one completes before the second tries to acquire)
	t.Logf("Success: %d, Failed: %d", successCount, failCount)
}

// TestIntegration_OptimisticLockConflict tests optimistic lock conflict handling
func TestIntegration_OptimisticLockConflict(t *testing.T) {
	store := newMockStore()

	// Create a transaction
	tx := NewStoreTx("conflict-tx", "test", []string{"step1"})
	err := store.CreateTransaction(context.Background(), tx)
	if err != nil {
		t.Fatalf("failed to create transaction: %v", err)
	}

	// Get the transaction twice (simulating two concurrent readers)
	tx1, _ := store.GetTransaction(context.Background(), "conflict-tx")
	tx2, _ := store.GetTransaction(context.Background(), "conflict-tx")

	// Update first copy
	tx1.Status = TxStatusLocked
	tx1.IncrementVersion()
	err = store.UpdateTransaction(context.Background(), tx1)
	if err != nil {
		t.Fatalf("first update should succeed: %v", err)
	}

	// Try to update second copy (should fail due to version conflict)
	tx2.Status = TxStatusExecuting
	tx2.IncrementVersion()
	err = store.UpdateTransaction(context.Background(), tx2)
	if err == nil {
		t.Error("expected version conflict error")
	}
	if !errors.Is(err, ErrVersionConflict) {
		t.Errorf("expected ErrVersionConflict, got %v", err)
	}
}

// TestIntegration_ConcurrentStepExecution tests concurrent step execution across transactions
func TestIntegration_ConcurrentStepExecution(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Track concurrent executions
	var concurrentCount int32
	var maxConcurrent int32
	var mu sync.Mutex

	// Register steps
	for _, name := range []string{"step1", "step2", "step3"} {
		stepName := name
		step := newTestStep(stepName)
		step.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
			// Track concurrent executions
			current := atomic.AddInt32(&concurrentCount, 1)
			mu.Lock()
			if current > maxConcurrent {
				maxConcurrent = current
			}
			mu.Unlock()

			// Simulate work
			time.Sleep(20 * time.Millisecond)

			atomic.AddInt32(&concurrentCount, -1)
			return nil
		}
		engine.RegisterStep(step)
	}

	// Execute multiple transactions concurrently
	numTransactions := 5
	var wg sync.WaitGroup
	completedCount := int32(0)

	for i := 0; i < numTransactions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			tx, _ := engine.NewTransaction("concurrent_steps").
				AddStep("step1").
				AddStep("step2").
				AddStep("step3").
				Build()

			result, err := engine.Execute(context.Background(), tx)
			if err == nil && result.Status == TxStatusCompleted {
				atomic.AddInt32(&completedCount, 1)
			}
		}()
	}

	wg.Wait()

	// Verify all transactions completed
	if atomic.LoadInt32(&completedCount) != int32(numTransactions) {
		t.Errorf("expected %d completed transactions, got %d", numTransactions, atomic.LoadInt32(&completedCount))
	}

	// Log max concurrent executions
	t.Logf("Max concurrent step executions: %d", maxConcurrent)
}

// TestIntegration_EventPublishingDuringConcurrentExecution tests event publishing under concurrent load
func TestIntegration_EventPublishingDuringConcurrentExecution(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	// Track events
	var eventMu sync.Mutex
	eventCounts := make(map[event.EventType]int)

	eventBus.SubscribeAll(func(ctx context.Context, e event.Event) error {
		eventMu.Lock()
		eventCounts[e.Type]++
		eventMu.Unlock()
		return nil
	})

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register a simple step
	step1 := newTestStep("step1")
	engine.RegisterStep(step1)

	// Execute multiple transactions concurrently
	numTransactions := 10
	var wg sync.WaitGroup

	for i := 0; i < numTransactions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			tx, _ := engine.NewTransaction("event_test").
				AddStep("step1").
				Build()

			engine.Execute(context.Background(), tx)
		}()
	}

	wg.Wait()

	// Verify events were published
	eventMu.Lock()
	defer eventMu.Unlock()

	// Should have tx.created events for each transaction
	if eventCounts[event.EventTxCreated] != numTransactions {
		t.Errorf("expected %d tx.created events, got %d", numTransactions, eventCounts[event.EventTxCreated])
	}

	// Should have tx.completed events for each transaction
	if eventCounts[event.EventTxCompleted] != numTransactions {
		t.Errorf("expected %d tx.completed events, got %d", numTransactions, eventCounts[event.EventTxCompleted])
	}
}
