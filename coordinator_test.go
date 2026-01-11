package rte

import (
	"context"
	"errors"
	"fmt"
	"rte/circuit"
	"rte/event"
	"rte/lock"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// ============================================================================
// Test Helpers - Mock Implementations
// ============================================================================

var errMockFailure = errors.New("mock failure")

// mockStore implements TxStore for testing
type mockStore struct {
	mu           sync.RWMutex
	transactions map[string]*StoreTx
	steps        map[string]map[int]*StoreStepRecord
	idempotency  map[string][]byte
}

func newMockStore() *mockStore {
	return &mockStore{
		transactions: make(map[string]*StoreTx),
		steps:        make(map[string]map[int]*StoreStepRecord),
		idempotency:  make(map[string][]byte),
	}
}

func (s *mockStore) CreateTransaction(ctx context.Context, tx *StoreTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.transactions[tx.TxID]; exists {
		return ErrTransactionAlreadyExists
	}
	txCopy := *tx
	s.transactions[tx.TxID] = &txCopy
	return nil
}

func (s *mockStore) UpdateTransaction(ctx context.Context, tx *StoreTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, exists := s.transactions[tx.TxID]
	if !exists {
		return ErrTransactionNotFound
	}
	// Check version for optimistic locking
	if existing.Version != tx.Version-1 {
		return ErrVersionConflict
	}
	txCopy := *tx
	s.transactions[tx.TxID] = &txCopy
	return nil
}

func (s *mockStore) GetTransaction(ctx context.Context, txID string) (*StoreTx, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tx, exists := s.transactions[txID]
	if !exists {
		return nil, ErrTransactionNotFound
	}
	txCopy := *tx
	return &txCopy, nil
}

func (s *mockStore) CreateStep(ctx context.Context, step *StoreStepRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.steps[step.TxID] == nil {
		s.steps[step.TxID] = make(map[int]*StoreStepRecord)
	}
	stepCopy := *step
	s.steps[step.TxID][step.StepIndex] = &stepCopy
	return nil
}

func (s *mockStore) UpdateStep(ctx context.Context, step *StoreStepRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.steps[step.TxID] == nil {
		return ErrStepNotFound
	}
	stepCopy := *step
	s.steps[step.TxID][step.StepIndex] = &stepCopy
	return nil
}

func (s *mockStore) GetStep(ctx context.Context, txID string, stepIndex int) (*StoreStepRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.steps[txID] == nil {
		return nil, ErrStepNotFound
	}
	step, exists := s.steps[txID][stepIndex]
	if !exists {
		return nil, ErrStepNotFound
	}
	stepCopy := *step
	return &stepCopy, nil
}

func (s *mockStore) GetSteps(ctx context.Context, txID string) ([]*StoreStepRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.steps[txID] == nil {
		return nil, nil
	}
	result := make([]*StoreStepRecord, 0, len(s.steps[txID]))
	for _, step := range s.steps[txID] {
		stepCopy := *step
		result = append(result, &stepCopy)
	}
	return result, nil
}

func (s *mockStore) GetPendingTransactions(ctx context.Context, olderThan time.Duration) ([]*StoreTx, error) {
	return nil, nil
}

func (s *mockStore) GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*StoreTx, error) {
	return nil, nil
}

func (s *mockStore) GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*StoreTx, error) {
	return nil, nil
}

func (s *mockStore) ListTransactions(ctx context.Context, filter *StoreTxFilter) ([]*StoreTx, int64, error) {
	return nil, 0, nil
}

func (s *mockStore) CheckIdempotency(ctx context.Context, key string) (bool, []byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result, exists := s.idempotency[key]
	return exists, result, nil
}

func (s *mockStore) MarkIdempotency(ctx context.Context, key string, result []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idempotency[key] = result
	return nil
}

func (s *mockStore) DeleteExpiredIdempotency(ctx context.Context) (int64, error) {
	return 0, nil
}

// mockLocker implements lock.Locker for testing
type mockLocker struct {
	mu    sync.Mutex
	locks map[string]bool
}

func newMockLocker() *mockLocker {
	return &mockLocker{
		locks: make(map[string]bool),
	}
}

func (l *mockLocker) Acquire(ctx context.Context, keys []string, ttl time.Duration) (lock.LockHandle, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, key := range keys {
		if l.locks[key] {
			return nil, ErrLockAcquisitionFailed
		}
	}
	for _, key := range keys {
		l.locks[key] = true
	}
	return &mockLockHandle{locker: l, keys: keys}, nil
}

type mockLockHandle struct {
	locker *mockLocker
	keys   []string
}

func (h *mockLockHandle) Extend(ctx context.Context, ttl time.Duration) error {
	return nil
}

func (h *mockLockHandle) Release(ctx context.Context) error {
	h.locker.mu.Lock()
	defer h.locker.mu.Unlock()
	for _, key := range h.keys {
		delete(h.locker.locks, key)
	}
	return nil
}

func (h *mockLockHandle) Keys() []string {
	return h.keys
}

// mockBreaker implements circuit.Breaker for testing
type mockBreaker struct {
	breakers map[string]*mockCircuitBreaker
	mu       sync.RWMutex
}

func newMockBreaker() *mockBreaker {
	return &mockBreaker{
		breakers: make(map[string]*mockCircuitBreaker),
	}
}

func (b *mockBreaker) Get(service string) circuit.CircuitBreaker {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cb, exists := b.breakers[service]; exists {
		return cb
	}
	cb := &mockCircuitBreaker{state: circuit.StateClosed}
	b.breakers[service] = cb
	return cb
}

func (b *mockBreaker) GetWithConfig(service string, config circuit.BreakerConfig) circuit.CircuitBreaker {
	return b.Get(service)
}

type mockCircuitBreaker struct {
	state  circuit.State
	counts circuit.BreakerCounts
	mu     sync.Mutex
}

func (cb *mockCircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	cb.mu.Lock()
	if cb.state == circuit.StateOpen {
		cb.mu.Unlock()
		return ErrCircuitOpen
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.counts.Requests++
	if err != nil {
		cb.counts.TotalFailures++
		cb.counts.ConsecutiveFailures++
		cb.counts.ConsecutiveSuccesses = 0
	} else {
		cb.counts.TotalSuccesses++
		cb.counts.ConsecutiveSuccesses++
		cb.counts.ConsecutiveFailures = 0
	}
	return err
}

func (cb *mockCircuitBreaker) State() circuit.State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

func (cb *mockCircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = circuit.StateClosed
	cb.counts = circuit.BreakerCounts{}
}

func (cb *mockCircuitBreaker) Counts() circuit.BreakerCounts {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.counts
}

// testStep is a configurable step for testing
type testStep struct {
	*BaseStep
	executeFunc    func(ctx context.Context, txCtx *TxContext) error
	compensateFunc func(ctx context.Context, txCtx *TxContext) error
	supportsComp   bool
	supportsIdem   bool
	idemKeyFunc    func(txCtx *TxContext) string
}

func newTestStep(name string) *testStep {
	return &testStep{
		BaseStep: NewBaseStep(name),
	}
}

func (s *testStep) Execute(ctx context.Context, txCtx *TxContext) error {
	if s.executeFunc != nil {
		return s.executeFunc(ctx, txCtx)
	}
	return nil
}

func (s *testStep) Compensate(ctx context.Context, txCtx *TxContext) error {
	if s.compensateFunc != nil {
		return s.compensateFunc(ctx, txCtx)
	}
	return ErrCompensationNotSupported
}

func (s *testStep) SupportsCompensation() bool {
	return s.supportsComp
}

func (s *testStep) SupportsIdempotency() bool {
	return s.supportsIdem
}

func (s *testStep) IdempotencyKey(txCtx *TxContext) string {
	if s.idemKeyFunc != nil {
		return s.idemKeyFunc(txCtx)
	}
	return ""
}

// ============================================================================
// Unit Tests
// ============================================================================

func TestCoordinator_NewCoordinator(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
	)

	if coord == nil {
		t.Fatal("expected coordinator to be created")
	}
	if coord.store == nil {
		t.Error("expected store to be set")
	}
	if coord.locker == nil {
		t.Error("expected locker to be set")
	}
}

func TestCoordinator_RegisterStep(t *testing.T) {
	coord := NewCoordinator()
	step := newTestStep("test-step")

	coord.RegisterStep(step)

	if !coord.HasStep("test-step") {
		t.Error("expected step to be registered")
	}
	if coord.GetStep("test-step") != step {
		t.Error("expected to get the same step")
	}
}

func TestCoordinator_ExecuteSimpleTransaction(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
		WithCoordinatorConfig(Config{
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
	step := newTestStep("step1")
	step.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		txCtx.SetOutput("result", "success")
		return nil
	}
	coord.RegisterStep(step)

	// Create and execute transaction
	tx, err := NewTransaction("test-tx").
		WithStepRegistry(coord).
		AddStep("step1").
		Build()
	if err != nil {
		t.Fatalf("failed to build transaction: %v", err)
	}

	result, err := coord.Execute(context.Background(), tx)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}

	if result.Status != TxStatusCompleted {
		t.Errorf("expected status COMPLETED, got %s", result.Status)
	}
	if result.Output["result"] != "success" {
		t.Errorf("expected output 'success', got %v", result.Output["result"])
	}
}

func TestCoordinator_ExecuteWithFailure(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
		WithCoordinatorConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register a failing step
	step := newTestStep("failing-step")
	step.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return errMockFailure
	}
	coord.RegisterStep(step)

	// Create and execute transaction
	tx, _ := NewTransaction("test-tx").
		WithStepRegistry(coord).
		AddStep("failing-step").
		Build()

	result, err := coord.Execute(context.Background(), tx)
	if err == nil {
		t.Fatal("expected error from failing step")
	}

	if result.Status != TxStatusFailed {
		t.Errorf("expected status FAILED, got %s", result.Status)
	}
}

func TestCoordinator_ExecuteWithCompensation(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
		WithCoordinatorConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	compensated := false

	// Register steps
	step1 := newTestStep("step1")
	step1.supportsComp = true
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return nil
	}
	step1.compensateFunc = func(ctx context.Context, txCtx *TxContext) error {
		compensated = true
		return nil
	}
	coord.RegisterStep(step1)

	step2 := newTestStep("step2")
	step2.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return errMockFailure
	}
	coord.RegisterStep(step2)

	// Create and execute transaction
	tx, _ := NewTransaction("test-tx").
		WithStepRegistry(coord).
		AddStep("step1").
		AddStep("step2").
		Build()

	result, _ := coord.Execute(context.Background(), tx)

	if result.Status != TxStatusCompensated {
		t.Errorf("expected status COMPENSATED, got %s", result.Status)
	}
	if !compensated {
		t.Error("expected step1 to be compensated")
	}
}

// ============================================================================
// Property-Based Tests
// ============================================================================


// For any transaction, the state transitions SHALL only follow the defined state machine paths.
func TestProperty_TransactionStateConsistency(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockStore()
		locker := newMockLocker()
		breaker := newMockBreaker()
		eventBus := event.NewMemoryEventBus()

		coord := NewCoordinator(
			WithStore(store),
			WithLocker(locker),
			WithBreaker(breaker),
			WithEventBus(eventBus),
			WithCoordinatorConfig(Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       3,
				RetryInterval:    100 * time.Millisecond,
				IdempotencyTTL:   24 * time.Hour,
			}),
		)

		// Generate random number of steps
		numSteps := rapid.IntRange(1, 5).Draw(t, "numSteps")
		failAtStep := rapid.IntRange(-1, numSteps-1).Draw(t, "failAtStep") // -1 means no failure
		supportsCompensation := rapid.Bool().Draw(t, "supportsCompensation")

		// Create step names deterministically
		stepNames := make([]string, numSteps)
		for i := 0; i < numSteps; i++ {
			stepNames[i] = fmt.Sprintf("step-%d", i)
		}

		// Register steps with the coordinator
		for i := 0; i < numSteps; i++ {
			step := newTestStep(stepNames[i])
			step.supportsComp = supportsCompensation

			stepIndex := i
			step.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
				if stepIndex == failAtStep {
					return errMockFailure
				}
				return nil
			}
			step.compensateFunc = func(ctx context.Context, txCtx *TxContext) error {
				return nil
			}
			coord.RegisterStep(step)
		}

		// Build transaction with all steps
		builder := NewTransaction("test-tx").WithStepRegistry(coord)
		for _, name := range stepNames {
			builder.AddStep(name)
		}

		tx, err := builder.Build()
		if err != nil {
			t.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction
		result, _ := coord.Execute(context.Background(), tx)

		// Verify state consistency
		storedTx, err := store.GetTransaction(context.Background(), tx.TxID())
		if err != nil {
			t.Fatalf("failed to get stored transaction: %v", err)
		}

		
		// FAILED is a valid end state for Execute() but not terminal in state machine sense
		isValidFinalState := IsTxTerminal(storedTx.Status) || storedTx.Status == TxStatusFailed
		if !isValidFinalState {
			t.Fatalf("final state %s is not a valid final state", storedTx.Status)
		}

		
		if result.Status != storedTx.Status {
			t.Fatalf("result status %s does not match stored status %s", result.Status, storedTx.Status)
		}

		
		if failAtStep == -1 && storedTx.Status != TxStatusCompleted {
			t.Fatalf("expected COMPLETED for successful transaction, got %s", storedTx.Status)
		}

		
		if failAtStep > 0 && supportsCompensation {
			if storedTx.Status != TxStatusCompensated {
				t.Fatalf("expected COMPENSATED for failed transaction with compensation, got %s", storedTx.Status)
			}
		}

		
		if failAtStep == 0 || (failAtStep >= 0 && !supportsCompensation) {
			if storedTx.Status != TxStatusFailed {
				t.Fatalf("expected FAILED for failed transaction at first step or without compensation, got %s", storedTx.Status)
			}
		}
	})
}


// For any failed transaction with compensation support, all completed steps SHALL be compensated in reverse order.
func TestProperty_CompensationCompleteness(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockStore()
		locker := newMockLocker()
		breaker := newMockBreaker()
		eventBus := event.NewMemoryEventBus()

		coord := NewCoordinator(
			WithStore(store),
			WithLocker(locker),
			WithBreaker(breaker),
			WithEventBus(eventBus),
			WithCoordinatorConfig(Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       3,
				RetryInterval:    100 * time.Millisecond,
				IdempotencyTTL:   24 * time.Hour,
			}),
		)

		// Generate random number of steps (at least 2 to have something to compensate)
		numSteps := rapid.IntRange(2, 5).Draw(t, "numSteps")
		// Fail at step 1 or later (so at least step 0 completes and needs compensation)
		failAtStep := rapid.IntRange(1, numSteps-1).Draw(t, "failAtStep")

		// Track compensation order
		var compensationMu sync.Mutex
		compensationOrder := make([]int, 0)

		// Create step names deterministically
		stepNames := make([]string, numSteps)
		for i := 0; i < numSteps; i++ {
			stepNames[i] = fmt.Sprintf("step-%d", i)
		}

		// Register steps with compensation support
		for i := 0; i < numSteps; i++ {
			step := newTestStep(stepNames[i])
			step.supportsComp = true

			stepIndex := i
			step.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
				if stepIndex == failAtStep {
					return errMockFailure
				}
				return nil
			}
			step.compensateFunc = func(ctx context.Context, txCtx *TxContext) error {
				compensationMu.Lock()
				compensationOrder = append(compensationOrder, stepIndex)
				compensationMu.Unlock()
				return nil
			}
			coord.RegisterStep(step)
		}

		// Build transaction with all steps
		builder := NewTransaction("test-tx").WithStepRegistry(coord)
		for _, name := range stepNames {
			builder.AddStep(name)
		}

		tx, err := builder.Build()
		if err != nil {
			t.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction (should fail and compensate)
		result, _ := coord.Execute(context.Background(), tx)

		
		if result.Status != TxStatusCompensated {
			t.Fatalf("expected COMPENSATED status, got %s", result.Status)
		}

		
		// Steps 0 to failAtStep-1 should be compensated (failAtStep failed, so it wasn't completed)
		expectedCompensations := failAtStep // Steps 0 to failAtStep-1
		if len(compensationOrder) != expectedCompensations {
			t.Fatalf("expected %d compensations, got %d: %v", expectedCompensations, len(compensationOrder), compensationOrder)
		}

		
		// The first compensation should be for the step just before the failed step
		for i := 0; i < len(compensationOrder); i++ {
			expectedStepIndex := failAtStep - 1 - i
			if compensationOrder[i] != expectedStepIndex {
				t.Fatalf("compensation order incorrect: expected step %d at position %d, got step %d. Full order: %v",
					expectedStepIndex, i, compensationOrder[i], compensationOrder)
			}
		}

		
		steps, err := store.GetSteps(context.Background(), tx.TxID())
		if err != nil {
			t.Fatalf("failed to get steps: %v", err)
		}

		for _, stepRecord := range steps {
			if stepRecord.StepIndex < failAtStep {
				// Completed steps should be compensated
				if stepRecord.Status != StepStatusCompensated {
					t.Fatalf("step %d should be COMPENSATED, got %s", stepRecord.StepIndex, stepRecord.Status)
				}
			} else if stepRecord.StepIndex == failAtStep {
				// Failed step should be FAILED
				if stepRecord.Status != StepStatusFailed {
					t.Fatalf("step %d should be FAILED, got %s", stepRecord.StepIndex, stepRecord.Status)
				}
			} else {
				// Steps after failed step should be PENDING
				if stepRecord.Status != StepStatusPending {
					t.Fatalf("step %d should be PENDING, got %s", stepRecord.StepIndex, stepRecord.Status)
				}
			}
		}
	})
}

// ============================================================================
// Unit Tests for Resume function
// Tests LOCKED, EXECUTING, FAILED (retryable and non-retryable), and default status branches
// ============================================================================

func TestResume_LockedStatus(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
		WithCoordinatorConfig(Config{
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
	step := newTestStep("step1")
	step.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		txCtx.SetOutput("result", "resumed")
		return nil
	}
	coord.RegisterStep(step)

	// Create a transaction in LOCKED status
	storeTx := NewStoreTx("test-tx", "test-type", []string{"step1"})
	storeTx.Status = TxStatusLocked
	storeTx.Context = &StoreTxContext{
		TxID:   "test-tx",
		TxType: "test-type",
		Input:  make(map[string]any),
		Output: make(map[string]any),
	}
	store.CreateTransaction(context.Background(), storeTx)

	// Create step record
	stepRecord := NewStoreStepRecord("test-tx", 0, "step1")
	store.CreateStep(context.Background(), stepRecord)

	// Resume the transaction
	result, err := coord.Resume(context.Background(), storeTx)
	if err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	if result.Status != TxStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", result.Status)
	}
}

func TestResume_ExecutingStatus(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
		WithCoordinatorConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register steps
	step1 := newTestStep("step1")
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return nil
	}
	coord.RegisterStep(step1)

	step2 := newTestStep("step2")
	step2.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		txCtx.SetOutput("result", "step2-done")
		return nil
	}
	coord.RegisterStep(step2)

	// Create a transaction in EXECUTING status at step 1
	storeTx := NewStoreTx("test-tx", "test-type", []string{"step1", "step2"})
	storeTx.Status = TxStatusExecuting
	storeTx.CurrentStep = 1 // Already completed step 0
	storeTx.Context = &StoreTxContext{
		TxID:      "test-tx",
		TxType:    "test-type",
		StepIndex: 1,
		Input:     make(map[string]any),
		Output:    make(map[string]any),
	}
	store.CreateTransaction(context.Background(), storeTx)

	// Create step records
	step1Record := NewStoreStepRecord("test-tx", 0, "step1")
	step1Record.Status = StepStatusCompleted
	store.CreateStep(context.Background(), step1Record)

	step2Record := NewStoreStepRecord("test-tx", 1, "step2")
	store.CreateStep(context.Background(), step2Record)

	// Resume the transaction
	result, err := coord.Resume(context.Background(), storeTx)
	if err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	if result.Status != TxStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", result.Status)
	}
}

func TestResume_FailedStatus_Retryable(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
		WithCoordinatorConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register a step that succeeds on retry
	retryCount := 0
	step := newTestStep("step1")
	step.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		retryCount++
		txCtx.SetOutput("result", "success-on-retry")
		return nil
	}
	coord.RegisterStep(step)

	// Create a transaction in FAILED status that can be retried
	storeTx := NewStoreTx("test-tx", "test-type", []string{"step1"})
	storeTx.Status = TxStatusFailed
	storeTx.RetryCount = 0
	storeTx.MaxRetries = 3
	storeTx.ErrorMsg = "previous failure"
	storeTx.Context = &StoreTxContext{
		TxID:   "test-tx",
		TxType: "test-type",
		Input:  make(map[string]any),
		Output: make(map[string]any),
	}
	store.CreateTransaction(context.Background(), storeTx)

	// Create step record
	stepRecord := NewStoreStepRecord("test-tx", 0, "step1")
	stepRecord.Status = StepStatusFailed
	store.CreateStep(context.Background(), stepRecord)

	// Resume the transaction (should retry)
	result, err := coord.Resume(context.Background(), storeTx)
	if err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	if result.Status != TxStatusCompleted {
		t.Errorf("expected COMPLETED after retry, got %s", result.Status)
	}

	if retryCount != 1 {
		t.Errorf("expected step to be executed once on retry, got %d", retryCount)
	}
}

func TestResume_FailedStatus_NotRetryable_WithCompensation(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
		WithCoordinatorConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	compensated := false

	// Register steps
	step1 := newTestStep("step1")
	step1.supportsComp = true
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return nil
	}
	step1.compensateFunc = func(ctx context.Context, txCtx *TxContext) error {
		compensated = true
		return nil
	}
	coord.RegisterStep(step1)

	step2 := newTestStep("step2")
	step2.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return errMockFailure
	}
	coord.RegisterStep(step2)

	// Create a transaction in FAILED status that cannot be retried (max retries exceeded)
	storeTx := NewStoreTx("test-tx", "test-type", []string{"step1", "step2"})
	storeTx.Status = TxStatusFailed
	storeTx.RetryCount = 3 // Max retries reached
	storeTx.MaxRetries = 3
	storeTx.CurrentStep = 1 // Failed at step 1, step 0 completed
	storeTx.ErrorMsg = "step2 failed"
	storeTx.Context = &StoreTxContext{
		TxID:   "test-tx",
		TxType: "test-type",
		Input:  make(map[string]any),
		Output: make(map[string]any),
	}
	store.CreateTransaction(context.Background(), storeTx)

	// Create step records
	step1Record := NewStoreStepRecord("test-tx", 0, "step1")
	step1Record.Status = StepStatusCompleted
	store.CreateStep(context.Background(), step1Record)

	step2Record := NewStoreStepRecord("test-tx", 1, "step2")
	step2Record.Status = StepStatusFailed
	store.CreateStep(context.Background(), step2Record)

	// Resume the transaction (should trigger compensation)
	result, _ := coord.Resume(context.Background(), storeTx)

	if result.Status != TxStatusCompensated {
		t.Errorf("expected COMPENSATED, got %s", result.Status)
	}

	if !compensated {
		t.Error("expected step1 to be compensated")
	}
}

func TestResume_FailedStatus_NotRetryable_NoCompensation(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
		WithCoordinatorConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register a step without compensation support
	step := newTestStep("step1")
	step.supportsComp = false
	step.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return errMockFailure
	}
	coord.RegisterStep(step)

	// Create a transaction in FAILED status that cannot be retried
	storeTx := NewStoreTx("test-tx", "test-type", []string{"step1"})
	storeTx.Status = TxStatusFailed
	storeTx.RetryCount = 3 // Max retries reached
	storeTx.MaxRetries = 3
	storeTx.CurrentStep = 0
	storeTx.ErrorMsg = "step1 failed"
	storeTx.Context = &StoreTxContext{
		TxID:   "test-tx",
		TxType: "test-type",
		Input:  make(map[string]any),
		Output: make(map[string]any),
	}
	store.CreateTransaction(context.Background(), storeTx)

	// Create step record
	stepRecord := NewStoreStepRecord("test-tx", 0, "step1")
	stepRecord.Status = StepStatusFailed
	store.CreateStep(context.Background(), stepRecord)

	// Resume the transaction (should remain FAILED)
	result, _ := coord.Resume(context.Background(), storeTx)

	if result.Status != TxStatusFailed {
		t.Errorf("expected FAILED, got %s", result.Status)
	}
}

func TestResume_DefaultStatus_Completed(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
	)

	// Create a transaction in COMPLETED status (terminal state)
	storeTx := NewStoreTx("test-tx", "test-type", []string{"step1"})
	storeTx.Status = TxStatusCompleted
	storeTx.Context = &StoreTxContext{
		TxID:   "test-tx",
		TxType: "test-type",
		Input:  make(map[string]any),
		Output: make(map[string]any),
	}
	store.CreateTransaction(context.Background(), storeTx)

	// Resume should return immediately with current status
	result, err := coord.Resume(context.Background(), storeTx)
	if err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	if result.Status != TxStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", result.Status)
	}
}

func TestResume_DefaultStatus_Compensated(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
	)

	// Create a transaction in COMPENSATED status (terminal state)
	storeTx := NewStoreTx("test-tx", "test-type", []string{"step1"})
	storeTx.Status = TxStatusCompensated
	storeTx.Context = &StoreTxContext{
		TxID:   "test-tx",
		TxType: "test-type",
		Input:  make(map[string]any),
		Output: make(map[string]any),
	}
	store.CreateTransaction(context.Background(), storeTx)

	// Resume should return immediately with current status
	result, err := coord.Resume(context.Background(), storeTx)
	if err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	if result.Status != TxStatusCompensated {
		t.Errorf("expected COMPENSATED, got %s", result.Status)
	}
}

func TestResume_DefaultStatus_Cancelled(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
	)

	// Create a transaction in CANCELLED status (terminal state)
	storeTx := NewStoreTx("test-tx", "test-type", []string{"step1"})
	storeTx.Status = TxStatusCancelled
	storeTx.Context = &StoreTxContext{
		TxID:   "test-tx",
		TxType: "test-type",
		Input:  make(map[string]any),
		Output: make(map[string]any),
	}
	store.CreateTransaction(context.Background(), storeTx)

	// Resume should return immediately with current status
	result, err := coord.Resume(context.Background(), storeTx)
	if err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	if result.Status != TxStatusCancelled {
		t.Errorf("expected CANCELLED, got %s", result.Status)
	}
}

func TestResume_DefaultStatus_Created(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
	)

	// Create a transaction in CREATED status (not handled by Resume)
	storeTx := NewStoreTx("test-tx", "test-type", []string{"step1"})
	storeTx.Status = TxStatusCreated
	storeTx.Context = &StoreTxContext{
		TxID:   "test-tx",
		TxType: "test-type",
		Input:  make(map[string]any),
		Output: make(map[string]any),
	}
	store.CreateTransaction(context.Background(), storeTx)

	// Resume should return immediately with current status (default case)
	result, err := coord.Resume(context.Background(), storeTx)
	if err != nil {
		t.Fatalf("Resume failed: %v", err)
	}

	if result.Status != TxStatusCreated {
		t.Errorf("expected CREATED, got %s", result.Status)
	}
}

// ============================================================================
// Tests for noOpLockHandle - Coverage improvement
// ============================================================================

func TestNoOpLockHandle_Extend(t *testing.T) {
	handle := &noOpLockHandle{}
	err := handle.Extend(context.Background(), 30*time.Second)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestNoOpLockHandle_Keys(t *testing.T) {
	handle := &noOpLockHandle{}
	keys := handle.Keys()
	if keys != nil {
		t.Errorf("expected nil keys, got %v", keys)
	}
}

// ============================================================================
// Additional tests for coordinator coverage improvement
// ============================================================================

func TestCoordinator_ExecuteWithNoLocks(t *testing.T) {
	store := newMockStore()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	// Create coordinator without locker to use noOpLockHandle
	coord := NewCoordinator(
		WithStore(store),
		WithBreaker(breaker),
		WithEventBus(eventBus),
		WithCoordinatorConfig(Config{
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
	step := newTestStep("step1")
	step.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		txCtx.SetOutput("result", "success")
		return nil
	}
	coord.RegisterStep(step)

	// Create transaction without lock keys
	tx, err := NewTransaction("test-tx").
		WithStepRegistry(coord).
		AddStep("step1").
		Build()
	if err != nil {
		t.Fatalf("failed to build transaction: %v", err)
	}

	result, err := coord.Execute(context.Background(), tx)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}

	if result.Status != TxStatusCompleted {
		t.Errorf("expected status COMPLETED, got %s", result.Status)
	}
}

func TestCoordinator_ExecuteWithContextCancellation(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := NewCoordinator(
		WithStore(store),
		WithLocker(locker),
		WithBreaker(breaker),
		WithEventBus(eventBus),
		WithCoordinatorConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register a step that blocks
	step := newTestStep("blocking-step")
	step.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return nil
		}
	}
	coord.RegisterStep(step)

	// Create transaction
	tx, _ := NewTransaction("test-tx").
		WithStepRegistry(coord).
		AddStep("blocking-step").
		Build()

	// Create a context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	result, _ := coord.Execute(ctx, tx)

	// Transaction should fail due to context cancellation
	if result.Status != TxStatusFailed {
		t.Errorf("expected status FAILED due to cancellation, got %s", result.Status)
	}
}

// ============================================================================

// compensation SHALL be executed in reverse order from step N-1 down to step 0.
// ============================================================================

func TestProperty_CompensationReverseOrder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockStore()
		locker := newMockLocker()
		breaker := newMockBreaker()
		eventBus := event.NewMemoryEventBus()

		coord := NewCoordinator(
			WithStore(store),
			WithLocker(locker),
			WithBreaker(breaker),
			WithEventBus(eventBus),
			WithCoordinatorConfig(Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       0, // No retries for compensation
				RetryInterval:    100 * time.Millisecond,
				IdempotencyTTL:   24 * time.Hour,
			}),
		)

		// Generate random number of steps (at least 2 to have something to compensate)
		numSteps := rapid.IntRange(2, 6).Draw(t, "numSteps")
		// Fail at step 1 or later (so at least step 0 completes and needs compensation)
		failAtStep := rapid.IntRange(1, numSteps-1).Draw(t, "failAtStep")

		// Track compensation order
		var compensationMu sync.Mutex
		compensationOrder := make([]int, 0)

		// Create step names deterministically
		stepNames := make([]string, numSteps)
		for i := 0; i < numSteps; i++ {
			stepNames[i] = fmt.Sprintf("step-%d", i)
		}

		// Register steps with compensation support
		for i := 0; i < numSteps; i++ {
			step := newTestStep(stepNames[i])
			step.supportsComp = true

			stepIndex := i
			step.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
				if stepIndex == failAtStep {
					return errMockFailure
				}
				return nil
			}
			step.compensateFunc = func(ctx context.Context, txCtx *TxContext) error {
				compensationMu.Lock()
				compensationOrder = append(compensationOrder, stepIndex)
				compensationMu.Unlock()
				return nil
			}
			coord.RegisterStep(step)
		}

		// Build transaction with all steps
		builder := NewTransaction("test-tx").WithStepRegistry(coord)
		for _, name := range stepNames {
			builder.AddStep(name)
		}

		tx, err := builder.Build()
		if err != nil {
			t.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction (should fail and compensate)
		result, _ := coord.Execute(context.Background(), tx)

		
		if result.Status != TxStatusCompensated {
			t.Fatalf("expected COMPENSATED status, got %s", result.Status)
		}

		
		// Steps 0 to failAtStep-1 should be compensated in reverse order
		expectedCompensations := failAtStep // Steps 0 to failAtStep-1
		if len(compensationOrder) != expectedCompensations {
			t.Fatalf("expected %d compensations, got %d: %v", expectedCompensations, len(compensationOrder), compensationOrder)
		}

		// Verify reverse order: first compensation should be for step failAtStep-1,
		// second for failAtStep-2, etc.
		for i := 0; i < len(compensationOrder); i++ {
			expectedStepIndex := failAtStep - 1 - i
			if compensationOrder[i] != expectedStepIndex {
				t.Fatalf("compensation order incorrect: expected step %d at position %d, got step %d. Full order: %v",
					expectedStepIndex, i, compensationOrder[i], compensationOrder)
			}
		}
	})
}

// ============================================================================

// method SHALL be called exactly once.
// ============================================================================

func TestProperty_CompensationExactlyOnce(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockStore()
		locker := newMockLocker()
		breaker := newMockBreaker()
		eventBus := event.NewMemoryEventBus()

		coord := NewCoordinator(
			WithStore(store),
			WithLocker(locker),
			WithBreaker(breaker),
			WithEventBus(eventBus),
			WithCoordinatorConfig(Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       0, // No retries for compensation
				RetryInterval:    100 * time.Millisecond,
				IdempotencyTTL:   24 * time.Hour,
			}),
		)

		// Generate random number of steps (at least 2 to have something to compensate)
		numSteps := rapid.IntRange(2, 6).Draw(t, "numSteps")
		// Fail at step 1 or later (so at least step 0 completes and needs compensation)
		failAtStep := rapid.IntRange(1, numSteps-1).Draw(t, "failAtStep")

		// Track compensation call counts per step
		var compensationMu sync.Mutex
		compensationCounts := make(map[int]int)

		// Create step names deterministically
		stepNames := make([]string, numSteps)
		for i := 0; i < numSteps; i++ {
			stepNames[i] = fmt.Sprintf("step-%d", i)
		}

		// Register steps with compensation support
		for i := 0; i < numSteps; i++ {
			step := newTestStep(stepNames[i])
			step.supportsComp = true

			stepIndex := i
			step.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
				if stepIndex == failAtStep {
					return errMockFailure
				}
				return nil
			}
			step.compensateFunc = func(ctx context.Context, txCtx *TxContext) error {
				compensationMu.Lock()
				compensationCounts[stepIndex]++
				compensationMu.Unlock()
				return nil
			}
			coord.RegisterStep(step)
		}

		// Build transaction with all steps
		builder := NewTransaction("test-tx").WithStepRegistry(coord)
		for _, name := range stepNames {
			builder.AddStep(name)
		}

		tx, err := builder.Build()
		if err != nil {
			t.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction (should fail and compensate)
		result, _ := coord.Execute(context.Background(), tx)

		
		if result.Status != TxStatusCompensated {
			t.Fatalf("expected COMPENSATED status, got %s", result.Status)
		}

		
		for stepIdx := 0; stepIdx < failAtStep; stepIdx++ {
			count := compensationCounts[stepIdx]
			if count != 1 {
				t.Fatalf("step %d compensation called %d times, expected exactly 1", stepIdx, count)
			}
		}

		
		for stepIdx := failAtStep; stepIdx < numSteps; stepIdx++ {
			count := compensationCounts[stepIdx]
			if count != 0 {
				t.Fatalf("step %d (failed or not executed) compensation called %d times, expected 0", stepIdx, count)
			}
		}

		
		totalCompensations := 0
		for _, count := range compensationCounts {
			totalCompensations += count
		}
		expectedCompensations := failAtStep // Steps 0 to failAtStep-1
		if totalCompensations != expectedCompensations {
			t.Fatalf("total compensations %d does not match expected %d", totalCompensations, expectedCompensations)
		}
	})
}
