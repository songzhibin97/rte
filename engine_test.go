package rte

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"rte/circuit"
	"rte/event"
	"rte/lock"
)

// ============================================================================
// Engine Unit Tests
// ============================================================================

func TestEngine_NewEngine(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
	)

	if engine == nil {
		t.Fatal("expected engine to be created")
	}
	if engine.store == nil {
		t.Error("expected store to be set")
	}
	if engine.locker == nil {
		t.Error("expected locker to be set")
	}
	if engine.breaker == nil {
		t.Error("expected breaker to be set")
	}
	if engine.events == nil {
		t.Error("expected event bus to be set")
	}
	if engine.coordinator == nil {
		t.Error("expected coordinator to be created")
	}
}

func TestEngine_NewEngineWithConfig(t *testing.T) {
	config := Config{
		LockTTL:          60 * time.Second,
		LockExtendPeriod: 20 * time.Second,
		MaxRetries:       5,
		RetryInterval:    10 * time.Second,
		StepTimeout:      15 * time.Second,
		TxTimeout:        10 * time.Minute,
		IdempotencyTTL:   48 * time.Hour,
	}

	engine := NewEngine(
		WithEngineStore(newMockStore()),
		WithEngineConfig(config),
	)

	if engine.config.LockTTL != 60*time.Second {
		t.Errorf("expected LockTTL 60s, got %v", engine.config.LockTTL)
	}
	if engine.config.MaxRetries != 5 {
		t.Errorf("expected MaxRetries 5, got %d", engine.config.MaxRetries)
	}
}

func TestEngine_RegisterStep(t *testing.T) {
	engine := NewEngine(WithEngineStore(newMockStore()))
	step := newTestStep("test-step")

	engine.RegisterStep(step)

	if !engine.HasStep("test-step") {
		t.Error("expected step to be registered in engine")
	}
	if engine.GetStep("test-step") != step {
		t.Error("expected to get the same step from engine")
	}
	// Also verify it's registered in coordinator
	if !engine.coordinator.HasStep("test-step") {
		t.Error("expected step to be registered in coordinator")
	}
}

func TestEngine_NewTransaction(t *testing.T) {
	engine := NewEngine(WithEngineStore(newMockStore()))
	step := newTestStep("step1")
	engine.RegisterStep(step)

	// Create transaction using engine's NewTransaction
	tx, err := engine.NewTransaction("test-tx").
		AddStep("step1").
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.TxType() != "test-tx" {
		t.Errorf("expected txType 'test-tx', got '%s'", tx.TxType())
	}
}

func TestEngine_NewTransaction_ValidatesSteps(t *testing.T) {
	engine := NewEngine(WithEngineStore(newMockStore()))
	// Don't register any steps

	// Try to create transaction with unregistered step
	_, err := engine.NewTransaction("test-tx").
		AddStep("unregistered-step").
		Build()

	if err == nil {
		t.Fatal("expected error for unregistered step")
	}
	if !errors.Is(err, ErrStepNotRegistered) {
		t.Errorf("expected ErrStepNotRegistered, got %v", err)
	}
}

func TestEngine_NewTransactionWithID(t *testing.T) {
	engine := NewEngine(WithEngineStore(newMockStore()))
	step := newTestStep("step1")
	engine.RegisterStep(step)

	tx, err := engine.NewTransactionWithID("custom-id", "test-tx").
		AddStep("step1").
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.TxID() != "custom-id" {
		t.Errorf("expected txID 'custom-id', got '%s'", tx.TxID())
	}
}

// ============================================================================
// Engine Integration Tests
// ============================================================================

func TestEngine_ExecuteCompleteTransaction(t *testing.T) {
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

	// Register steps
	step1 := newTestStep("local_debit")
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		amount, _ := GetInputAs[string](txCtx, "amount")
		txCtx.SetOutput("debit_ref", "DEBIT-001")
		txCtx.SetOutput("debited_amount", amount)
		return nil
	}
	engine.RegisterStep(step1)

	step2 := newTestStep("platform_b_deposit")
	step2.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		debitRef, _ := GetOutputAs[string](txCtx, "debit_ref")
		txCtx.SetOutput("deposit_ref", "PlatformB-"+debitRef)
		return nil
	}
	engine.RegisterStep(step2)

	step3 := newTestStep("local_credit")
	step3.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		txCtx.SetOutput("credit_ref", "CREDIT-001")
		txCtx.SetOutput("status", "completed")
		return nil
	}
	engine.RegisterStep(step3)

	// Create and execute transaction
	tx, err := engine.NewTransaction("saving_to_forex").
		WithLockKeys("account:saving:123", "account:forex:456").
		WithInput(map[string]any{
			"user_id":   123,
			"amount":    "100.00",
			"currency":  "USD",
			"saving_id": 123,
			"forex_id":  456,
		}).
		AddStep("local_debit").
		AddStep("platform_b_deposit").
		AddStep("local_credit").
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
	if result.Output["status"] != "completed" {
		t.Errorf("expected output status 'completed', got %v", result.Output["status"])
	}
	if result.Output["debit_ref"] != "DEBIT-001" {
		t.Errorf("expected debit_ref 'DEBIT-001', got %v", result.Output["debit_ref"])
	}
	if result.Output["deposit_ref"] != "PlatformB-DEBIT-001" {
		t.Errorf("expected deposit_ref 'PlatformB-DEBIT-001', got %v", result.Output["deposit_ref"])
	}

	// Verify stored transaction
	storedTx, err := store.GetTransaction(context.Background(), tx.TxID())
	if err != nil {
		t.Fatalf("failed to get stored transaction: %v", err)
	}
	if storedTx.Status != TxStatusCompleted {
		t.Errorf("expected stored status COMPLETED, got %s", storedTx.Status)
	}
}

func TestEngine_ExecuteWithFailureAndCompensation(t *testing.T) {
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

	// Track compensation
	var compensationMu sync.Mutex
	compensatedSteps := make([]string, 0)

	// Register steps
	step1 := newTestStep("local_debit")
	step1.supportsComp = true
	step1.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		txCtx.SetOutput("debit_ref", "DEBIT-001")
		return nil
	}
	step1.compensateFunc = func(ctx context.Context, txCtx *TxContext) error {
		compensationMu.Lock()
		compensatedSteps = append(compensatedSteps, "local_debit")
		compensationMu.Unlock()
		return nil
	}
	engine.RegisterStep(step1)

	step2 := newTestStep("platform_b_deposit")
	step2.supportsComp = true
	step2.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		txCtx.SetOutput("deposit_ref", "PlatformB-001")
		return nil
	}
	step2.compensateFunc = func(ctx context.Context, txCtx *TxContext) error {
		compensationMu.Lock()
		compensatedSteps = append(compensatedSteps, "platform_b_deposit")
		compensationMu.Unlock()
		return nil
	}
	engine.RegisterStep(step2)

	step3 := newTestStep("local_credit")
	step3.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
		return errors.New("credit service unavailable")
	}
	engine.RegisterStep(step3)

	// Create and execute transaction
	tx, _ := engine.NewTransaction("saving_to_forex").
		WithLockKeys("account:saving:123", "account:forex:456").
		WithInput(map[string]any{
			"user_id": 123,
			"amount":  "100.00",
		}).
		AddStep("local_debit").
		AddStep("platform_b_deposit").
		AddStep("local_credit").
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
	compensationMu.Lock()
	defer compensationMu.Unlock()
	if len(compensatedSteps) != 2 {
		t.Fatalf("expected 2 compensated steps, got %d", len(compensatedSteps))
	}
	if compensatedSteps[0] != "platform_b_deposit" {
		t.Errorf("expected first compensation 'platform_b_deposit', got '%s'", compensatedSteps[0])
	}
	if compensatedSteps[1] != "local_debit" {
		t.Errorf("expected second compensation 'local_debit', got '%s'", compensatedSteps[1])
	}
}

func TestEngine_ExecuteWithFailureNoCompensation(t *testing.T) {
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

	// Register steps without compensation support
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
	tx, _ := engine.NewTransaction("test-tx").
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

func TestEngine_Subscribe(t *testing.T) {
	eventBus := event.NewMemoryEventBus()
	engine := NewEngine(
		WithEngineStore(newMockStore()),
		WithEngineLocker(newMockLocker()),
		WithEngineBreaker(newMockBreaker()),
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

	// Track events
	var eventsMu sync.Mutex
	receivedEvents := make([]event.EventType, 0)

	// Subscribe to events
	engine.Subscribe(event.EventTxCreated, func(ctx context.Context, e event.Event) error {
		eventsMu.Lock()
		receivedEvents = append(receivedEvents, e.Type)
		eventsMu.Unlock()
		return nil
	})

	engine.Subscribe(event.EventTxCompleted, func(ctx context.Context, e event.Event) error {
		eventsMu.Lock()
		receivedEvents = append(receivedEvents, e.Type)
		eventsMu.Unlock()
		return nil
	})

	// Register and execute a simple step
	step := newTestStep("step1")
	engine.RegisterStep(step)

	tx, _ := engine.NewTransaction("test-tx").
		AddStep("step1").
		Build()

	engine.Execute(context.Background(), tx)

	// Verify events were received
	eventsMu.Lock()
	defer eventsMu.Unlock()

	hasCreated := false
	hasCompleted := false
	for _, et := range receivedEvents {
		if et == event.EventTxCreated {
			hasCreated = true
		}
		if et == event.EventTxCompleted {
			hasCompleted = true
		}
	}

	if !hasCreated {
		t.Error("expected to receive tx.created event")
	}
	if !hasCompleted {
		t.Error("expected to receive tx.completed event")
	}
}

func TestEngine_SubscribeAll(t *testing.T) {
	eventBus := event.NewMemoryEventBus()
	engine := NewEngine(
		WithEngineStore(newMockStore()),
		WithEngineLocker(newMockLocker()),
		WithEngineBreaker(newMockBreaker()),
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

	// Track all events
	var eventsMu sync.Mutex
	allEvents := make([]event.EventType, 0)

	engine.SubscribeAll(func(ctx context.Context, e event.Event) error {
		eventsMu.Lock()
		allEvents = append(allEvents, e.Type)
		eventsMu.Unlock()
		return nil
	})

	// Register and execute
	step := newTestStep("step1")
	engine.RegisterStep(step)

	tx, _ := engine.NewTransaction("test-tx").
		AddStep("step1").
		Build()

	engine.Execute(context.Background(), tx)

	// Should have received multiple events
	eventsMu.Lock()
	defer eventsMu.Unlock()

	if len(allEvents) < 2 {
		t.Errorf("expected at least 2 events, got %d", len(allEvents))
	}
}

func TestEngine_SubscribeWithNilEventBus(t *testing.T) {
	// Engine without event bus
	engine := NewEngine(WithEngineStore(newMockStore()))

	// Subscribe should not panic
	err := engine.Subscribe(event.EventTxCreated, func(ctx context.Context, e event.Event) error {
		return nil
	})

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	err = engine.SubscribeAll(func(ctx context.Context, e event.Event) error {
		return nil
	})

	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestEngine_Coordinator(t *testing.T) {
	engine := NewEngine(WithEngineStore(newMockStore()))

	coord := engine.Coordinator()
	if coord == nil {
		t.Error("expected non-nil coordinator")
	}
}

func TestEngine_Store(t *testing.T) {
	store := newMockStore()
	engine := NewEngine(WithEngineStore(store))

	if engine.Store() != store {
		t.Error("expected same store instance")
	}
}

func TestEngine_Config(t *testing.T) {
	config := Config{
		MaxRetries: 10,
	}
	engine := NewEngine(
		WithEngineStore(newMockStore()),
		WithEngineConfig(config),
	)

	if engine.Config().MaxRetries != 10 {
		t.Errorf("expected MaxRetries 10, got %d", engine.Config().MaxRetries)
	}
}

// ============================================================================
// Engine Integration Test - Full Flow
// ============================================================================

func TestEngine_FullIntegrationFlow(t *testing.T) {
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
	var executionMu sync.Mutex
	executionOrder := make([]string, 0)

	// Register steps that track execution
	for _, name := range []string{"step1", "step2", "step3"} {
		stepName := name
		step := newTestStep(stepName)
		step.executeFunc = func(ctx context.Context, txCtx *TxContext) error {
			executionMu.Lock()
			executionOrder = append(executionOrder, stepName)
			executionMu.Unlock()
			txCtx.SetOutput(stepName+"_done", true)
			return nil
		}
		engine.RegisterStep(step)
	}

	// Create transaction
	tx, err := engine.NewTransaction("integration-test").
		WithLockKeys("resource:1", "resource:2").
		WithInput(map[string]any{
			"test_id": "integration-001",
		}).
		WithMetadataValue("trace_id", "trace-integration-001").
		WithTimeout(1 * time.Minute).
		AddStep("step1").
		AddStep("step2").
		AddStep("step3").
		Build()

	if err != nil {
		t.Fatalf("failed to build transaction: %v", err)
	}

	// Execute
	result, err := engine.Execute(context.Background(), tx)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}

	// Verify result
	if result.Status != TxStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", result.Status)
	}

	// Verify execution order
	executionMu.Lock()
	defer executionMu.Unlock()
	if len(executionOrder) != 3 {
		t.Fatalf("expected 3 steps executed, got %d", len(executionOrder))
	}
	for i, expected := range []string{"step1", "step2", "step3"} {
		if executionOrder[i] != expected {
			t.Errorf("expected step %s at position %d, got %s", expected, i, executionOrder[i])
		}
	}

	// Verify outputs
	for _, name := range []string{"step1", "step2", "step3"} {
		if result.Output[name+"_done"] != true {
			t.Errorf("expected %s_done to be true", name)
		}
	}

	// Verify stored transaction
	storedTx, _ := store.GetTransaction(context.Background(), tx.TxID())
	if storedTx.Status != TxStatusCompleted {
		t.Errorf("expected stored status COMPLETED, got %s", storedTx.Status)
	}
	if storedTx.TotalSteps != 3 {
		t.Errorf("expected 3 total steps, got %d", storedTx.TotalSteps)
	}

	// Verify step records
	steps, _ := store.GetSteps(context.Background(), tx.TxID())
	if len(steps) != 3 {
		t.Fatalf("expected 3 step records, got %d", len(steps))
	}
	for _, step := range steps {
		if step.Status != StepStatusCompleted {
			t.Errorf("expected step %s to be COMPLETED, got %s", step.StepName, step.Status)
		}
	}
}

// ============================================================================
// Mock implementations for engine tests (reusing from coordinator_test.go)
// ============================================================================

// engineMockLocker implements lock.Locker for testing
type engineMockLocker struct {
	mu    sync.Mutex
	locks map[string]bool
}

func newEngineMockLocker() *engineMockLocker {
	return &engineMockLocker{
		locks: make(map[string]bool),
	}
}

func (l *engineMockLocker) Acquire(ctx context.Context, keys []string, ttl time.Duration) (lock.LockHandle, error) {
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
	return &engineMockLockHandle{locker: l, keys: keys}, nil
}

type engineMockLockHandle struct {
	locker *engineMockLocker
	keys   []string
}

func (h *engineMockLockHandle) Extend(ctx context.Context, ttl time.Duration) error {
	return nil
}

func (h *engineMockLockHandle) Release(ctx context.Context) error {
	h.locker.mu.Lock()
	defer h.locker.mu.Unlock()
	for _, key := range h.keys {
		delete(h.locker.locks, key)
	}
	return nil
}

func (h *engineMockLockHandle) Keys() []string {
	return h.keys
}

// engineMockBreaker implements circuit.Breaker for testing
type engineMockBreaker struct {
	breakers map[string]*engineMockCircuitBreaker
	mu       sync.RWMutex
}

func newEngineMockBreaker() *engineMockBreaker {
	return &engineMockBreaker{
		breakers: make(map[string]*engineMockCircuitBreaker),
	}
}

func (b *engineMockBreaker) Get(service string) circuit.CircuitBreaker {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cb, exists := b.breakers[service]; exists {
		return cb
	}
	cb := &engineMockCircuitBreaker{state: circuit.StateClosed}
	b.breakers[service] = cb
	return cb
}

func (b *engineMockBreaker) GetWithConfig(service string, config circuit.BreakerConfig) circuit.CircuitBreaker {
	return b.Get(service)
}

type engineMockCircuitBreaker struct {
	state  circuit.State
	counts circuit.BreakerCounts
	mu     sync.Mutex
}

func (cb *engineMockCircuitBreaker) Execute(ctx context.Context, fn func() error) error {
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
	} else {
		cb.counts.TotalSuccesses++
	}
	return err
}

func (cb *engineMockCircuitBreaker) State() circuit.State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

func (cb *engineMockCircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = circuit.StateClosed
	cb.counts = circuit.BreakerCounts{}
}

func (cb *engineMockCircuitBreaker) Counts() circuit.BreakerCounts {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.counts
}
