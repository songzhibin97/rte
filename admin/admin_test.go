package admin

import (
	"context"
	"errors"
	"rte/circuit"
	"rte/event"
	"rte/lock"
	"sync"
	"testing"
	"time"

	"rte"
)

// ============================================================================
// Test Helpers - Mock Implementations
// ============================================================================

var errMockFailure = errors.New("mock failure")

// mockStore implements rte.TxStore for testing
type mockStore struct {
	mu           sync.RWMutex
	transactions map[string]*rte.StoreTx
	steps        map[string]map[int]*rte.StoreStepRecord
	idempotency  map[string][]byte
}

func newMockStore() *mockStore {
	return &mockStore{
		transactions: make(map[string]*rte.StoreTx),
		steps:        make(map[string]map[int]*rte.StoreStepRecord),
		idempotency:  make(map[string][]byte),
	}
}

func (s *mockStore) CreateTransaction(ctx context.Context, tx *rte.StoreTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.transactions[tx.TxID]; exists {
		return rte.ErrTransactionAlreadyExists
	}
	txCopy := *tx
	s.transactions[tx.TxID] = &txCopy
	return nil
}

func (s *mockStore) UpdateTransaction(ctx context.Context, tx *rte.StoreTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, exists := s.transactions[tx.TxID]
	if !exists {
		return rte.ErrTransactionNotFound
	}
	// Check version for optimistic locking
	if existing.Version != tx.Version-1 {
		return rte.ErrVersionConflict
	}
	txCopy := *tx
	s.transactions[tx.TxID] = &txCopy
	return nil
}

func (s *mockStore) GetTransaction(ctx context.Context, txID string) (*rte.StoreTx, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tx, exists := s.transactions[txID]
	if !exists {
		return nil, nil
	}
	txCopy := *tx
	return &txCopy, nil
}

func (s *mockStore) CreateStep(ctx context.Context, step *rte.StoreStepRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.steps[step.TxID] == nil {
		s.steps[step.TxID] = make(map[int]*rte.StoreStepRecord)
	}
	stepCopy := *step
	s.steps[step.TxID][step.StepIndex] = &stepCopy
	return nil
}

func (s *mockStore) UpdateStep(ctx context.Context, step *rte.StoreStepRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.steps[step.TxID] == nil {
		return rte.ErrStepNotFound
	}
	stepCopy := *step
	s.steps[step.TxID][step.StepIndex] = &stepCopy
	return nil
}

func (s *mockStore) GetStep(ctx context.Context, txID string, stepIndex int) (*rte.StoreStepRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.steps[txID] == nil {
		return nil, rte.ErrStepNotFound
	}
	step, exists := s.steps[txID][stepIndex]
	if !exists {
		return nil, rte.ErrStepNotFound
	}
	stepCopy := *step
	return &stepCopy, nil
}

func (s *mockStore) GetSteps(ctx context.Context, txID string) ([]*rte.StoreStepRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.steps[txID] == nil {
		return nil, nil
	}
	result := make([]*rte.StoreStepRecord, 0, len(s.steps[txID]))
	for i := 0; i < len(s.steps[txID]); i++ {
		if step, exists := s.steps[txID][i]; exists {
			stepCopy := *step
			result = append(result, &stepCopy)
		}
	}
	return result, nil
}

func (s *mockStore) GetPendingTransactions(ctx context.Context, olderThan time.Duration) ([]*rte.StoreTx, error) {
	return nil, nil
}

func (s *mockStore) GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*rte.StoreTx, error) {
	return nil, nil
}

func (s *mockStore) GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*rte.StoreTx, error) {
	return nil, nil
}

func (s *mockStore) ListTransactions(ctx context.Context, filter *rte.StoreTxFilter) ([]*rte.StoreTx, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*rte.StoreTx
	for _, tx := range s.transactions {
		// Apply status filter
		if len(filter.Status) > 0 {
			matched := false
			for _, status := range filter.Status {
				if tx.Status == status {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}

		// Apply type filter
		if filter.TxType != "" && tx.TxType != filter.TxType {
			continue
		}

		// Apply time range filter
		if !filter.StartTime.IsZero() && tx.CreatedAt.Before(filter.StartTime) {
			continue
		}
		if !filter.EndTime.IsZero() && tx.CreatedAt.After(filter.EndTime) {
			continue
		}

		txCopy := *tx
		result = append(result, &txCopy)
	}

	total := int64(len(result))

	// Apply pagination
	if filter.Offset > 0 && filter.Offset < len(result) {
		result = result[filter.Offset:]
	} else if filter.Offset >= len(result) {
		result = nil
	}

	if filter.Limit > 0 && filter.Limit < len(result) {
		result = result[:filter.Limit]
	}

	return result, total, nil
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
			return nil, rte.ErrLockAcquisitionFailed
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
		return rte.ErrCircuitOpen
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
	*rte.BaseStep
	executeFunc    func(ctx context.Context, txCtx *rte.TxContext) error
	compensateFunc func(ctx context.Context, txCtx *rte.TxContext) error
	supportsComp   bool
	supportsIdem   bool
	idemKeyFunc    func(txCtx *rte.TxContext) string
}

func newTestStep(name string) *testStep {
	return &testStep{
		BaseStep: rte.NewBaseStep(name),
	}
}

func (s *testStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	if s.executeFunc != nil {
		return s.executeFunc(ctx, txCtx)
	}
	return nil
}

func (s *testStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	if s.compensateFunc != nil {
		return s.compensateFunc(ctx, txCtx)
	}
	return rte.ErrCompensationNotSupported
}

func (s *testStep) SupportsCompensation() bool {
	return s.supportsComp
}

func (s *testStep) SupportsIdempotency() bool {
	return s.supportsIdem
}

func (s *testStep) IdempotencyKey(txCtx *rte.TxContext) string {
	if s.idemKeyFunc != nil {
		return s.idemKeyFunc(txCtx)
	}
	return ""
}

// ============================================================================
// Helper Functions
// ============================================================================

func createTestTransaction(store *mockStore, txID, txType string, status rte.TxStatus, stepNames []string) *rte.StoreTx {
	tx := rte.NewStoreTx(txID, txType, stepNames)
	tx.Status = status
	store.CreateTransaction(context.Background(), tx)

	// Create step records
	for i, name := range stepNames {
		step := rte.NewStoreStepRecord(txID, i, name)
		store.CreateStep(context.Background(), step)
	}

	return tx
}

// ============================================================================
// Unit Tests
// ============================================================================

func TestAdmin_NewAdmin(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	admin := NewAdmin(
		WithAdminStore(store),
		WithAdminLocker(locker),
		WithAdminBreaker(breaker),
		WithAdminEventBus(eventBus),
	)

	if admin == nil {
		t.Fatal("expected admin to be created")
	}
	if admin.store == nil {
		t.Error("expected store to be set")
	}
	if admin.locker == nil {
		t.Error("expected locker to be set")
	}
}

// Test ListTransactions
func TestAdmin_ListTransactions(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	// Create test transactions
	createTestTransaction(store, "tx-1", "type-a", rte.TxStatusCompleted, []string{"step1"})
	createTestTransaction(store, "tx-2", "type-a", rte.TxStatusFailed, []string{"step1"})
	createTestTransaction(store, "tx-3", "type-b", rte.TxStatusCompleted, []string{"step1"})

	t.Run("list all transactions", func(t *testing.T) {
		result, err := admin.ListTransactions(context.Background(), nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Total != 3 {
			t.Errorf("expected 3 transactions, got %d", result.Total)
		}
	})

	t.Run("filter by status", func(t *testing.T) {
		filter := &rte.StoreTxFilter{
			Status: []rte.TxStatus{rte.TxStatusCompleted},
			Limit:  100,
		}
		result, err := admin.ListTransactions(context.Background(), filter)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Total != 2 {
			t.Errorf("expected 2 completed transactions, got %d", result.Total)
		}
	})

	t.Run("filter by type", func(t *testing.T) {
		filter := &rte.StoreTxFilter{
			TxType: "type-a",
			Limit:  100,
		}
		result, err := admin.ListTransactions(context.Background(), filter)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Total != 2 {
			t.Errorf("expected 2 type-a transactions, got %d", result.Total)
		}
	})

	t.Run("no store configured", func(t *testing.T) {
		adminNoStore := NewAdmin()
		_, err := adminNoStore.ListTransactions(context.Background(), nil)
		if err == nil {
			t.Error("expected error when store not configured")
		}
	})
}

// Test GetTransaction
func TestAdmin_GetTransaction(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	// Create test transaction with steps
	createTestTransaction(store, "tx-1", "test-type", rte.TxStatusExecuting, []string{"step1", "step2"})

	t.Run("get existing transaction", func(t *testing.T) {
		detail, err := admin.GetTransaction(context.Background(), "tx-1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if detail.Transaction == nil {
			t.Fatal("expected transaction to be returned")
		}
		if detail.Transaction.TxID != "tx-1" {
			t.Errorf("expected tx-1, got %s", detail.Transaction.TxID)
		}
		if len(detail.Steps) != 2 {
			t.Errorf("expected 2 steps, got %d", len(detail.Steps))
		}
	})

	t.Run("get non-existent transaction", func(t *testing.T) {
		_, err := admin.GetTransaction(context.Background(), "non-existent")
		if err == nil {
			t.Error("expected error for non-existent transaction")
		}
		if !errors.Is(err, rte.ErrTransactionNotFound) {
			t.Errorf("expected ErrTransactionNotFound, got %v", err)
		}
	})

	t.Run("no store configured", func(t *testing.T) {
		adminNoStore := NewAdmin()
		_, err := adminNoStore.GetTransaction(context.Background(), "tx-1")
		if err == nil {
			t.Error("expected error when store not configured")
		}
	})
}

// Test ForceComplete
func TestAdmin_ForceComplete(t *testing.T) {
	store := newMockStore()
	eventBus := event.NewMemoryEventBus()
	admin := NewAdmin(
		WithAdminStore(store),
		WithAdminEventBus(eventBus),
	)

	t.Run("force complete stuck transaction", func(t *testing.T) {
		createTestTransaction(store, "tx-stuck", "test-type", rte.TxStatusExecuting, []string{"step1"})

		err := admin.ForceComplete(context.Background(), "tx-stuck", "manual intervention")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify status updated
		tx, _ := store.GetTransaction(context.Background(), "tx-stuck")
		if tx.Status != rte.TxStatusCompleted {
			t.Errorf("expected COMPLETED status, got %s", tx.Status)
		}
		if tx.CompletedAt == nil {
			t.Error("expected CompletedAt to be set")
		}
	})

	t.Run("force complete locked transaction", func(t *testing.T) {
		createTestTransaction(store, "tx-locked", "test-type", rte.TxStatusLocked, []string{"step1"})

		err := admin.ForceComplete(context.Background(), "tx-locked", "manual intervention")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		tx, _ := store.GetTransaction(context.Background(), "tx-locked")
		if tx.Status != rte.TxStatusCompleted {
			t.Errorf("expected COMPLETED status, got %s", tx.Status)
		}
	})

	t.Run("cannot force complete already completed transaction", func(t *testing.T) {
		createTestTransaction(store, "tx-completed", "test-type", rte.TxStatusCompleted, []string{"step1"})

		err := admin.ForceComplete(context.Background(), "tx-completed", "manual intervention")
		if err == nil {
			t.Error("expected error for already completed transaction")
		}
	})

	t.Run("cannot force complete failed transaction", func(t *testing.T) {
		createTestTransaction(store, "tx-failed", "test-type", rte.TxStatusFailed, []string{"step1"})

		err := admin.ForceComplete(context.Background(), "tx-failed", "manual intervention")
		if err == nil {
			t.Error("expected error for failed transaction")
		}
	})

	t.Run("force complete non-existent transaction", func(t *testing.T) {
		err := admin.ForceComplete(context.Background(), "non-existent", "manual intervention")
		if err == nil {
			t.Error("expected error for non-existent transaction")
		}
	})
}

// Test ForceCancel
func TestAdmin_ForceCancel(t *testing.T) {
	store := newMockStore()
	eventBus := event.NewMemoryEventBus()
	admin := NewAdmin(
		WithAdminStore(store),
		WithAdminEventBus(eventBus),
	)

	t.Run("force cancel created transaction", func(t *testing.T) {
		createTestTransaction(store, "tx-created", "test-type", rte.TxStatusCreated, []string{"step1"})

		err := admin.ForceCancel(context.Background(), "tx-created", "manual cancellation")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		tx, _ := store.GetTransaction(context.Background(), "tx-created")
		if tx.Status != rte.TxStatusCancelled {
			t.Errorf("expected CANCELLED status, got %s", tx.Status)
		}
	})

	t.Run("force cancel executing transaction without completed steps", func(t *testing.T) {
		createTestTransaction(store, "tx-exec", "test-type", rte.TxStatusExecuting, []string{"step1"})

		err := admin.ForceCancel(context.Background(), "tx-exec", "manual cancellation")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		tx, _ := store.GetTransaction(context.Background(), "tx-exec")
		if tx.Status != rte.TxStatusCancelled {
			t.Errorf("expected CANCELLED status, got %s", tx.Status)
		}
	})

	t.Run("cannot force cancel completed transaction", func(t *testing.T) {
		createTestTransaction(store, "tx-done", "test-type", rte.TxStatusCompleted, []string{"step1"})

		err := admin.ForceCancel(context.Background(), "tx-done", "manual cancellation")
		if err == nil {
			t.Error("expected error for completed transaction")
		}
	})

	t.Run("force cancel non-existent transaction", func(t *testing.T) {
		err := admin.ForceCancel(context.Background(), "non-existent", "manual cancellation")
		if err == nil {
			t.Error("expected error for non-existent transaction")
		}
	})
}

// Test RetryTransaction
func TestAdmin_RetryTransaction(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	// Create coordinator with test step
	coord := rte.NewCoordinator(
		rte.WithStore(store),
		rte.WithLocker(locker),
		rte.WithBreaker(breaker),
		rte.WithEventBus(eventBus),
		rte.WithCoordinatorConfig(rte.Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
			IdempotencyTTL:   24 * time.Hour,
		}),
	)

	// Register a step that always succeeds (for retry test)
	step := newTestStep("step1")
	step.executeFunc = func(ctx context.Context, txCtx *rte.TxContext) error {
		return nil
	}
	coord.RegisterStep(step)

	admin := NewAdmin(
		WithAdminStore(store),
		WithAdminEventBus(eventBus),
		WithAdminCoordinator(coord),
	)

	t.Run("retry failed transaction", func(t *testing.T) {
		// Create a failed transaction
		tx := rte.NewStoreTx("tx-retry", "test-type", []string{"step1"})
		tx.Status = rte.TxStatusFailed
		tx.RetryCount = 0
		tx.MaxRetries = 3
		tx.Context = &rte.StoreTxContext{
			TxID:   "tx-retry",
			TxType: "test-type",
			Input:  make(map[string]any),
			Output: make(map[string]any),
		}
		store.CreateTransaction(context.Background(), tx)

		// Create step record
		stepRecord := rte.NewStoreStepRecord("tx-retry", 0, "step1")
		store.CreateStep(context.Background(), stepRecord)

		err := admin.RetryTransaction(context.Background(), "tx-retry")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify transaction was retried and completed
		updatedTx, _ := store.GetTransaction(context.Background(), "tx-retry")
		// RetryCount is incremented by both admin.RetryTransaction and coordinator.Resume
		if updatedTx.RetryCount < 1 {
			t.Errorf("expected retry count >= 1, got %d", updatedTx.RetryCount)
		}
		if updatedTx.Status != rte.TxStatusCompleted {
			t.Errorf("expected COMPLETED status after retry, got %s", updatedTx.Status)
		}
	})

	t.Run("cannot retry non-failed transaction", func(t *testing.T) {
		createTestTransaction(store, "tx-completed", "test-type", rte.TxStatusCompleted, []string{"step1"})

		err := admin.RetryTransaction(context.Background(), "tx-completed")
		if err == nil {
			t.Error("expected error for non-failed transaction")
		}
	})

	t.Run("retry non-existent transaction", func(t *testing.T) {
		err := admin.RetryTransaction(context.Background(), "non-existent")
		if err == nil {
			t.Error("expected error for non-existent transaction")
		}
	})

	t.Run("no coordinator configured", func(t *testing.T) {
		adminNoCoord := NewAdmin(WithAdminStore(store))
		createTestTransaction(store, "tx-no-coord", "test-type", rte.TxStatusFailed, []string{"step1"})

		err := adminNoCoord.RetryTransaction(context.Background(), "tx-no-coord")
		if err == nil {
			t.Error("expected error when coordinator not configured")
		}
	})
}

// Test GetStats
func TestAdmin_GetStats(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	// Create test transactions with various statuses
	createTestTransaction(store, "tx-1", "type-a", rte.TxStatusCompleted, []string{"step1"})
	createTestTransaction(store, "tx-2", "type-a", rte.TxStatusCompleted, []string{"step1"})
	createTestTransaction(store, "tx-3", "type-b", rte.TxStatusFailed, []string{"step1"})
	createTestTransaction(store, "tx-4", "type-b", rte.TxStatusExecuting, []string{"step1"})
	createTestTransaction(store, "tx-5", "type-c", rte.TxStatusCompensated, []string{"step1"})

	t.Run("get stats", func(t *testing.T) {
		stats, err := admin.GetStats(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if stats.TotalTransactions != 5 {
			t.Errorf("expected 5 total transactions, got %d", stats.TotalTransactions)
		}
		if stats.CompletedTransactions != 2 {
			t.Errorf("expected 2 completed transactions, got %d", stats.CompletedTransactions)
		}
		if stats.FailedTransactions != 1 {
			t.Errorf("expected 1 failed transaction, got %d", stats.FailedTransactions)
		}
		if stats.PendingTransactions != 1 {
			t.Errorf("expected 1 pending transaction, got %d", stats.PendingTransactions)
		}
		if stats.CompensatedTransactions != 1 {
			t.Errorf("expected 1 compensated transaction, got %d", stats.CompensatedTransactions)
		}
	})

	t.Run("no store configured", func(t *testing.T) {
		adminNoStore := NewAdmin()
		_, err := adminNoStore.GetStats(context.Background())
		if err == nil {
			t.Error("expected error when store not configured")
		}
	})
}

// Test Filter Helper Functions
func TestAdmin_FilterHelpers(t *testing.T) {
	t.Run("NewTxFilter", func(t *testing.T) {
		filter := NewTxFilter()
		if filter.Limit != 100 {
			t.Errorf("expected default limit 100, got %d", filter.Limit)
		}
		if filter.Offset != 0 {
			t.Errorf("expected default offset 0, got %d", filter.Offset)
		}
	})

	t.Run("FilterWithStatus", func(t *testing.T) {
		filter := NewTxFilter()
		FilterWithStatus(filter, rte.TxStatusCompleted, rte.TxStatusFailed)
		if len(filter.Status) != 2 {
			t.Errorf("expected 2 statuses, got %d", len(filter.Status))
		}
	})

	t.Run("FilterWithTxType", func(t *testing.T) {
		filter := NewTxFilter()
		FilterWithTxType(filter, "test-type")
		if filter.TxType != "test-type" {
			t.Errorf("expected test-type, got %s", filter.TxType)
		}
	})

	t.Run("FilterWithTimeRange", func(t *testing.T) {
		filter := NewTxFilter()
		start := time.Now().Add(-24 * time.Hour)
		end := time.Now()
		FilterWithTimeRange(filter, start, end)
		if filter.StartTime != start {
			t.Error("expected start time to be set")
		}
		if filter.EndTime != end {
			t.Error("expected end time to be set")
		}
	})

	t.Run("FilterWithPagination", func(t *testing.T) {
		filter := NewTxFilter()
		FilterWithPagination(filter, 50, 10)
		if filter.Limit != 50 {
			t.Errorf("expected limit 50, got %d", filter.Limit)
		}
		if filter.Offset != 10 {
			t.Errorf("expected offset 10, got %d", filter.Offset)
		}
	})
}
