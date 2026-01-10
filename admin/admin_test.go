package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/http/httptest"
	"rte/circuit"
	"rte/event"
	"rte/lock"
	"rte/recovery"
	"strings"
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

// ============================================================================
// AdminServer Tests
// ============================================================================

func TestAdminServer_NewAdminServer(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	server := NewAdminServer(
		WithAddr(":8081"),
		WithAdminImpl(admin),
		WithServerStore(store),
	)

	if server == nil {
		t.Fatal("expected server to be created")
	}
	if server.addr != ":8081" {
		t.Errorf("expected addr :8081, got %s", server.addr)
	}
	if server.admin == nil {
		t.Error("expected admin to be set")
	}
}

func TestAdminServer_Routes(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))
	breaker := newMockBreaker()

	server := NewAdminServer(
		WithAdminImpl(admin),
		WithServerBreaker(breaker),
	)

	handler := server.Handler()
	if handler == nil {
		t.Fatal("expected handler to be returned")
	}
}

// ============================================================================
// API Handler Tests
// ============================================================================

func TestAPIHandler_HandleListTransactions(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	// Create test transactions
	createTestTransaction(store, "tx-1", "type-a", rte.TxStatusCompleted, []string{"step1"})
	createTestTransaction(store, "tx-2", "type-b", rte.TxStatusFailed, []string{"step1"})

	server := NewAdminServer(WithAdminImpl(admin))
	handler := server.Handler()

	t.Run("list all transactions", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if !resp.Success {
			t.Error("expected success to be true")
		}
	})

	t.Run("filter by status", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions?status=COMPLETED", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})
}

func TestAPIHandler_HandleGetTransaction(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	createTestTransaction(store, "tx-1", "type-a", rte.TxStatusCompleted, []string{"step1"})

	server := NewAdminServer(WithAdminImpl(admin))
	handler := server.Handler()

	t.Run("get existing transaction", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions/tx-1", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if !resp.Success {
			t.Error("expected success to be true")
		}
	})

	t.Run("get non-existent transaction", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions/non-existent", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Code)
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if resp.Success {
			t.Error("expected success to be false")
		}
		if resp.Error == nil {
			t.Error("expected error to be set")
		}
		if resp.Error.Code != ErrCodeTransactionNotFound {
			t.Errorf("expected error code %s, got %s", ErrCodeTransactionNotFound, resp.Error.Code)
		}
	})
}

func TestAPIHandler_HandleGetStats(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	createTestTransaction(store, "tx-1", "type-a", rte.TxStatusCompleted, []string{"step1"})
	createTestTransaction(store, "tx-2", "type-b", rte.TxStatusFailed, []string{"step1"})

	server := NewAdminServer(WithAdminImpl(admin))
	handler := server.Handler()

	req := httptest.NewRequest("GET", "/api/stats", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp APIResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if !resp.Success {
		t.Error("expected success to be true")
	}
}

func TestAPIHandler_HandleForceComplete(t *testing.T) {
	store := newMockStore()
	eventBus := event.NewMemoryEventBus()
	admin := NewAdmin(
		WithAdminStore(store),
		WithAdminEventBus(eventBus),
	)

	createTestTransaction(store, "tx-stuck", "type-a", rte.TxStatusExecuting, []string{"step1"})

	server := NewAdminServer(WithAdminImpl(admin))
	handler := server.Handler()

	t.Run("force complete stuck transaction", func(t *testing.T) {
		body := strings.NewReader(`{"reason": "manual intervention"}`)
		req := httptest.NewRequest("POST", "/api/transactions/tx-stuck/force-complete", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if !resp.Success {
			t.Error("expected success to be true")
		}
	})

	t.Run("force complete non-existent transaction", func(t *testing.T) {
		body := strings.NewReader(`{"reason": "test"}`)
		req := httptest.NewRequest("POST", "/api/transactions/non-existent/force-complete", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Code)
		}
	})
}

func TestAPIHandler_HandleResetCircuitBreaker(t *testing.T) {
	breaker := newMockBreaker()

	server := NewAdminServer(WithServerBreaker(breaker))
	handler := server.Handler()

	req := httptest.NewRequest("POST", "/api/circuit-breakers/test-service/reset", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp APIResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if !resp.Success {
		t.Error("expected success to be true")
	}
}

// ============================================================================
// Page Handler Tests
// ============================================================================

func TestPageHandler_HandleIndex(t *testing.T) {
	server := NewAdminServer()
	handler := server.Handler()

	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	if !strings.Contains(w.Body.String(), "RTE 管理控制台") {
		t.Error("expected page to contain 'RTE 管理控制台'")
	}
}

func TestPageHandler_HandleTransactions(t *testing.T) {
	server := NewAdminServer()
	handler := server.Handler()

	req := httptest.NewRequest("GET", "/transactions", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	if !strings.Contains(w.Body.String(), "事务列表") {
		t.Error("expected page to contain '事务列表'")
	}
}

// ============================================================================
// AdminServer Start/Stop Tests
// ============================================================================

func TestAdminServer_StartStop(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	// Use a random high port to avoid conflicts
	server := NewAdminServer(
		WithAddr(":0"), // Let the system assign a port
		WithAdminImpl(admin),
	)

	t.Run("start and stop server", func(t *testing.T) {
		// Start server in a goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- server.Start()
		}()

		// Give the server time to start
		time.Sleep(100 * time.Millisecond)

		// Stop the server
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := server.Stop(ctx)
		if err != nil {
			t.Errorf("unexpected error stopping server: %v", err)
		}

		// Check that Start returned (either nil or http.ErrServerClosed)
		select {
		case err := <-errCh:
			if err != nil && err != http.ErrServerClosed {
				t.Errorf("unexpected error from Start: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Start did not return after Stop")
		}
	})

	t.Run("stop already stopped server", func(t *testing.T) {
		// Create a new server that was never started
		server2 := NewAdminServer(WithAdminImpl(admin))

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Should not error when stopping a server that was never started
		err := server2.Stop(ctx)
		if err != nil {
			t.Errorf("unexpected error stopping non-running server: %v", err)
		}
	})

	t.Run("start already running server", func(t *testing.T) {
		server3 := NewAdminServer(
			WithAddr(":0"),
			WithAdminImpl(admin),
		)

		// Start server
		go func() {
			server3.Start()
		}()

		// Give the server time to start
		time.Sleep(100 * time.Millisecond)

		// Try to start again - should return error
		err := server3.Start()
		if err == nil {
			t.Error("expected error when starting already running server")
		}

		// Clean up
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		server3.Stop(ctx)
	})
}

// ============================================================================
// Admin Compensation Path Tests
// ============================================================================

func TestAdmin_WithAdminConfig(t *testing.T) {
	config := rte.Config{
		LockTTL:          60 * time.Second,
		MaxRetries:       5,
		CircuitThreshold: 10,
	}

	admin := NewAdmin(WithAdminConfig(config))

	if admin.config.LockTTL != 60*time.Second {
		t.Errorf("expected LockTTL 60s, got %v", admin.config.LockTTL)
	}
	if admin.config.MaxRetries != 5 {
		t.Errorf("expected MaxRetries 5, got %d", admin.config.MaxRetries)
	}
	if admin.config.CircuitThreshold != 10 {
		t.Errorf("expected CircuitThreshold 10, got %d", admin.config.CircuitThreshold)
	}
}

func TestAdmin_ForceCancel_WithCompensation(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	// Create coordinator with compensatable step
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

	// Register a compensatable step
	compensateCalled := false
	step := newTestStep("step1")
	step.supportsComp = true
	step.executeFunc = func(ctx context.Context, txCtx *rte.TxContext) error {
		return nil
	}
	step.compensateFunc = func(ctx context.Context, txCtx *rte.TxContext) error {
		compensateCalled = true
		return nil
	}
	coord.RegisterStep(step)

	admin := NewAdmin(
		WithAdminStore(store),
		WithAdminEventBus(eventBus),
		WithAdminCoordinator(coord),
	)

	t.Run("force cancel with compensation needed", func(t *testing.T) {
		// Create a transaction with completed steps
		tx := rte.NewStoreTx("tx-comp", "test-type", []string{"step1"})
		tx.Status = rte.TxStatusExecuting
		tx.CurrentStep = 1 // Step 0 completed
		tx.Context = &rte.StoreTxContext{
			TxID:   "tx-comp",
			TxType: "test-type",
			Input:  make(map[string]any),
			Output: make(map[string]any),
		}
		store.CreateTransaction(context.Background(), tx)

		// Create completed step record
		stepRecord := rte.NewStoreStepRecord("tx-comp", 0, "step1")
		stepRecord.Status = rte.StepStatusCompleted
		store.CreateStep(context.Background(), stepRecord)

		compensateCalled = false
		err := admin.ForceCancel(context.Background(), "tx-comp", "test compensation")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify compensation was called
		if !compensateCalled {
			t.Error("expected compensation to be called")
		}

		// Verify transaction status
		updatedTx, _ := store.GetTransaction(context.Background(), "tx-comp")
		if updatedTx.Status != rte.TxStatusCompensated {
			t.Errorf("expected COMPENSATED status, got %s", updatedTx.Status)
		}
	})

	t.Run("force cancel with compensation failure", func(t *testing.T) {
		// Create a new transaction
		tx := rte.NewStoreTx("tx-comp-fail", "test-type", []string{"step1"})
		tx.Status = rte.TxStatusExecuting
		tx.CurrentStep = 1
		tx.Context = &rte.StoreTxContext{
			TxID:   "tx-comp-fail",
			TxType: "test-type",
			Input:  make(map[string]any),
			Output: make(map[string]any),
		}
		store.CreateTransaction(context.Background(), tx)

		// Create completed step record
		stepRecord := rte.NewStoreStepRecord("tx-comp-fail", 0, "step1")
		stepRecord.Status = rte.StepStatusCompleted
		store.CreateStep(context.Background(), stepRecord)

		// Make compensation fail
		step.compensateFunc = func(ctx context.Context, txCtx *rte.TxContext) error {
			return errors.New("compensation failed")
		}

		err := admin.ForceCancel(context.Background(), "tx-comp-fail", "test compensation failure")
		if err == nil {
			t.Error("expected error when compensation fails")
		}

		// Verify transaction status is COMPENSATION_FAILED
		updatedTx, _ := store.GetTransaction(context.Background(), "tx-comp-fail")
		if updatedTx.Status != rte.TxStatusCompensationFailed {
			t.Errorf("expected COMPENSATION_FAILED status, got %s", updatedTx.Status)
		}
	})
}

func TestAdmin_CompensateTransaction(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	// Create coordinator with multiple compensatable steps
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

	// Track compensation order
	var compensationOrder []string
	var mu sync.Mutex

	// Register multiple compensatable steps
	for i := 0; i < 3; i++ {
		stepName := fmt.Sprintf("step%d", i)
		step := newTestStep(stepName)
		step.supportsComp = true
		step.executeFunc = func(ctx context.Context, txCtx *rte.TxContext) error {
			return nil
		}
		// Capture stepName in closure
		name := stepName
		step.compensateFunc = func(ctx context.Context, txCtx *rte.TxContext) error {
			mu.Lock()
			compensationOrder = append(compensationOrder, name)
			mu.Unlock()
			return nil
		}
		coord.RegisterStep(step)
	}

	admin := NewAdmin(
		WithAdminStore(store),
		WithAdminEventBus(eventBus),
		WithAdminCoordinator(coord),
	)

	t.Run("compensate multiple steps in reverse order", func(t *testing.T) {
		// Create transaction with multiple completed steps
		tx := rte.NewStoreTx("tx-multi-comp", "test-type", []string{"step0", "step1", "step2"})
		tx.Status = rte.TxStatusCompensating
		tx.CurrentStep = 3
		tx.Context = &rte.StoreTxContext{
			TxID:   "tx-multi-comp",
			TxType: "test-type",
			Input:  make(map[string]any),
			Output: make(map[string]any),
		}
		store.CreateTransaction(context.Background(), tx)

		// Create completed step records
		for i := 0; i < 3; i++ {
			stepRecord := rte.NewStoreStepRecord("tx-multi-comp", i, fmt.Sprintf("step%d", i))
			stepRecord.Status = rte.StepStatusCompleted
			store.CreateStep(context.Background(), stepRecord)
		}

		compensationOrder = nil
		txCtx := tx.Context.ToTxContext()
		_, err := admin.compensateTransaction(context.Background(), tx, txCtx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify compensation order (should be reverse: step2, step1, step0)
		if len(compensationOrder) != 3 {
			t.Fatalf("expected 3 compensations, got %d", len(compensationOrder))
		}
		if compensationOrder[0] != "step2" || compensationOrder[1] != "step1" || compensationOrder[2] != "step0" {
			t.Errorf("expected reverse order [step2, step1, step0], got %v", compensationOrder)
		}
	})

	t.Run("skip non-completed steps during compensation", func(t *testing.T) {
		// Create transaction with mixed step statuses
		tx := rte.NewStoreTx("tx-skip-comp", "test-type", []string{"step0", "step1", "step2"})
		tx.Status = rte.TxStatusCompensating
		tx.CurrentStep = 3
		tx.Context = &rte.StoreTxContext{
			TxID:   "tx-skip-comp",
			TxType: "test-type",
			Input:  make(map[string]any),
			Output: make(map[string]any),
		}
		store.CreateTransaction(context.Background(), tx)

		// Create step records with different statuses
		step0 := rte.NewStoreStepRecord("tx-skip-comp", 0, "step0")
		step0.Status = rte.StepStatusCompleted
		store.CreateStep(context.Background(), step0)

		step1 := rte.NewStoreStepRecord("tx-skip-comp", 1, "step1")
		step1.Status = rte.StepStatusPending // Not completed, should be skipped
		store.CreateStep(context.Background(), step1)

		step2 := rte.NewStoreStepRecord("tx-skip-comp", 2, "step2")
		step2.Status = rte.StepStatusCompleted
		store.CreateStep(context.Background(), step2)

		compensationOrder = nil
		txCtx := tx.Context.ToTxContext()
		_, err := admin.compensateTransaction(context.Background(), tx, txCtx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify only completed steps were compensated
		if len(compensationOrder) != 2 {
			t.Fatalf("expected 2 compensations, got %d", len(compensationOrder))
		}
		if compensationOrder[0] != "step2" || compensationOrder[1] != "step0" {
			t.Errorf("expected [step2, step0], got %v", compensationOrder)
		}
	})
}

// ============================================================================
// Server HTTP Handler Tests
// ============================================================================

func TestAPIHandler_HandleRetry(t *testing.T) {
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

	// Register a step that always succeeds
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

	server := NewAdminServer(WithAdminImpl(admin))
	handler := server.Handler()

	t.Run("retry failed transaction", func(t *testing.T) {
		// Create a failed transaction
		tx := rte.NewStoreTx("tx-retry-api", "test-type", []string{"step1"})
		tx.Status = rte.TxStatusFailed
		tx.RetryCount = 0
		tx.MaxRetries = 3
		tx.Context = &rte.StoreTxContext{
			TxID:   "tx-retry-api",
			TxType: "test-type",
			Input:  make(map[string]any),
			Output: make(map[string]any),
		}
		store.CreateTransaction(context.Background(), tx)

		// Create step record
		stepRecord := rte.NewStoreStepRecord("tx-retry-api", 0, "step1")
		store.CreateStep(context.Background(), stepRecord)

		req := httptest.NewRequest("POST", "/api/transactions/tx-retry-api/retry", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if !resp.Success {
			t.Error("expected success to be true")
		}
	})

	t.Run("retry non-existent transaction", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/transactions/non-existent/retry", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Code)
		}
	})

	t.Run("retry non-failed transaction", func(t *testing.T) {
		// Create a completed transaction
		tx := rte.NewStoreTx("tx-completed-api", "test-type", []string{"step1"})
		tx.Status = rte.TxStatusCompleted
		tx.Context = &rte.StoreTxContext{
			TxID:   "tx-completed-api",
			TxType: "test-type",
			Input:  make(map[string]any),
			Output: make(map[string]any),
		}
		store.CreateTransaction(context.Background(), tx)

		req := httptest.NewRequest("POST", "/api/transactions/tx-completed-api/retry", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}
	})

	t.Run("retry without admin configured", func(t *testing.T) {
		serverNoAdmin := NewAdminServer()
		handlerNoAdmin := serverNoAdmin.Handler()

		req := httptest.NewRequest("POST", "/api/transactions/tx-1/retry", nil)
		w := httptest.NewRecorder()

		handlerNoAdmin.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})
}

func TestAPIHandler_HandleGetStats_Extended(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	// Create test transactions with various statuses
	createTestTransaction(store, "tx-stat-1", "type-a", rte.TxStatusCompleted, []string{"step1"})
	createTestTransaction(store, "tx-stat-2", "type-a", rte.TxStatusFailed, []string{"step1"})
	createTestTransaction(store, "tx-stat-3", "type-b", rte.TxStatusExecuting, []string{"step1"})
	createTestTransaction(store, "tx-stat-4", "type-b", rte.TxStatusCompensated, []string{"step1"})
	createTestTransaction(store, "tx-stat-5", "type-c", rte.TxStatusTimeout, []string{"step1"})

	server := NewAdminServer(WithAdminImpl(admin))
	handler := server.Handler()

	t.Run("get stats with all transaction types", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/stats", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if !resp.Success {
			t.Error("expected success to be true")
		}

		// Verify stats data
		data, ok := resp.Data.(map[string]interface{})
		if !ok {
			t.Fatal("expected data to be a map")
		}

		if data["total_transactions"].(float64) != 5 {
			t.Errorf("expected 5 total transactions, got %v", data["total_transactions"])
		}
	})

	t.Run("get stats without admin configured", func(t *testing.T) {
		serverNoAdmin := NewAdminServer()
		handlerNoAdmin := serverNoAdmin.Handler()

		req := httptest.NewRequest("GET", "/api/stats", nil)
		w := httptest.NewRecorder()

		handlerNoAdmin.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})
}

func TestAPIHandler_HandleGetRecoveryStats(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	// Create recovery worker - need to create a mock that implements recovery.TxStore
	recoveryWorker := recovery.NewWorker()

	server := NewAdminServer(
		WithAdminImpl(admin),
		WithServerRecovery(recoveryWorker),
	)
	handler := server.Handler()

	t.Run("get recovery stats", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/recovery/stats", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if !resp.Success {
			t.Error("expected success to be true")
		}

		// Verify stats data structure
		data, ok := resp.Data.(map[string]interface{})
		if !ok {
			t.Fatal("expected data to be a map")
		}

		// Check that expected fields exist
		if _, exists := data["is_running"]; !exists {
			t.Error("expected is_running field")
		}
		if _, exists := data["scanned_count"]; !exists {
			t.Error("expected scanned_count field")
		}
		if _, exists := data["processed_count"]; !exists {
			t.Error("expected processed_count field")
		}
		if _, exists := data["failed_count"]; !exists {
			t.Error("expected failed_count field")
		}
	})

	t.Run("get recovery stats without recovery worker configured", func(t *testing.T) {
		serverNoRecovery := NewAdminServer(WithAdminImpl(admin))
		handlerNoRecovery := serverNoRecovery.Handler()

		req := httptest.NewRequest("GET", "/api/recovery/stats", nil)
		w := httptest.NewRecorder()

		handlerNoRecovery.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if resp.Success {
			t.Error("expected success to be false")
		}
		if resp.Error == nil || resp.Error.Code != ErrCodeInternalError {
			t.Error("expected internal error code")
		}
	})
}

func TestAPIHandler_HandleForceCancel(t *testing.T) {
	store := newMockStore()
	eventBus := event.NewMemoryEventBus()
	admin := NewAdmin(
		WithAdminStore(store),
		WithAdminEventBus(eventBus),
	)

	server := NewAdminServer(WithAdminImpl(admin))
	handler := server.Handler()

	t.Run("force cancel created transaction", func(t *testing.T) {
		createTestTransaction(store, "tx-cancel-api", "test-type", rte.TxStatusCreated, []string{"step1"})

		body := strings.NewReader(`{"reason": "test cancellation"}`)
		req := httptest.NewRequest("POST", "/api/transactions/tx-cancel-api/force-cancel", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if !resp.Success {
			t.Error("expected success to be true")
		}

		// Verify transaction was cancelled
		tx, _ := store.GetTransaction(context.Background(), "tx-cancel-api")
		if tx.Status != rte.TxStatusCancelled {
			t.Errorf("expected CANCELLED status, got %s", tx.Status)
		}
	})

	t.Run("force cancel non-existent transaction", func(t *testing.T) {
		body := strings.NewReader(`{"reason": "test"}`)
		req := httptest.NewRequest("POST", "/api/transactions/non-existent/force-cancel", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Code)
		}
	})

	t.Run("force cancel completed transaction", func(t *testing.T) {
		createTestTransaction(store, "tx-cancel-completed", "test-type", rte.TxStatusCompleted, []string{"step1"})

		body := strings.NewReader(`{"reason": "test"}`)
		req := httptest.NewRequest("POST", "/api/transactions/tx-cancel-completed/force-cancel", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}
	})

	t.Run("force cancel with invalid request body", func(t *testing.T) {
		createTestTransaction(store, "tx-cancel-invalid", "test-type", rte.TxStatusCreated, []string{"step1"})

		body := strings.NewReader(`invalid json`)
		req := httptest.NewRequest("POST", "/api/transactions/tx-cancel-invalid/force-cancel", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}
	})

	t.Run("force cancel without admin configured", func(t *testing.T) {
		serverNoAdmin := NewAdminServer()
		handlerNoAdmin := serverNoAdmin.Handler()

		body := strings.NewReader(`{"reason": "test"}`)
		req := httptest.NewRequest("POST", "/api/transactions/tx-1/force-cancel", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handlerNoAdmin.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})
}

// ============================================================================
// Page Handler Tests
// ============================================================================

func TestPageHandler_LoadTemplatesFromDir(t *testing.T) {
	t.Run("load templates from valid directory", func(t *testing.T) {
		// Use the embedded templates directory
		templates, err := LoadTemplates()
		if err != nil {
			t.Fatalf("unexpected error loading templates: %v", err)
		}
		if templates == nil {
			t.Fatal("expected templates to be loaded")
		}

		// Verify some templates exist
		if templates.Lookup("layout.html") == nil {
			t.Error("expected layout.html template")
		}
		if templates.Lookup("index.html") == nil {
			t.Error("expected index.html template")
		}
	})

	t.Run("load templates from invalid directory", func(t *testing.T) {
		_, err := LoadTemplatesFromDir("/nonexistent/directory")
		if err == nil {
			t.Error("expected error loading templates from invalid directory")
		}
	})
}

func TestPageHandler_SetGetTemplates(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	pageHandler, err := NewPageHandler(admin, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating page handler: %v", err)
	}

	t.Run("get templates returns initial templates", func(t *testing.T) {
		templates := pageHandler.GetTemplates()
		if templates == nil {
			t.Error("expected templates to be set")
		}
	})

	t.Run("set and get custom templates", func(t *testing.T) {
		// Create a simple custom template
		customTemplate := template.New("custom")
		customTemplate.Parse("{{define \"test\"}}test content{{end}}")

		pageHandler.SetTemplates(customTemplate)

		retrieved := pageHandler.GetTemplates()
		if retrieved != customTemplate {
			t.Error("expected retrieved templates to match set templates")
		}
	})
}

func TestPageHandler_RenderTemplateErrors(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	server := NewAdminServer(WithAdminImpl(admin))
	handler := server.Handler()

	t.Run("render index page", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("render transactions page", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/transactions", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("render recovery page", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/recovery", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("render circuit breakers page", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/circuit-breakers", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("render events page", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/events", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})
}

func TestPageHandler_TransactionDetail(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	// Create test transaction
	createTestTransaction(store, "tx-detail-page", "test-type", rte.TxStatusExecuting, []string{"step1", "step2"})

	server := NewAdminServer(WithAdminImpl(admin))
	handler := server.Handler()

	t.Run("render transaction detail page", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/transactions/tx-detail-page", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
		}
	})

	t.Run("render non-existent transaction detail", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/transactions/non-existent", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Code)
		}
	})

	t.Run("render transaction detail without admin", func(t *testing.T) {
		serverNoAdmin := NewAdminServer()
		handlerNoAdmin := serverNoAdmin.Handler()

		req := httptest.NewRequest("GET", "/transactions/tx-1", nil)
		w := httptest.NewRecorder()

		handlerNoAdmin.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})
}

func TestPageHandler_TransactionsWithFilters(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	// Create test transactions
	createTestTransaction(store, "tx-filter-1", "type-a", rte.TxStatusCompleted, []string{"step1"})
	createTestTransaction(store, "tx-filter-2", "type-b", rte.TxStatusFailed, []string{"step1"})

	server := NewAdminServer(WithAdminImpl(admin))
	handler := server.Handler()

	t.Run("filter by status", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/transactions?status=COMPLETED", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("filter by tx_type", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/transactions?tx_type=type-a", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("filter by time range", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/transactions?start_time=2024-01-01T00:00&end_time=2025-12-31T23:59", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("pagination", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/transactions?page=1", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})
}

func TestPageHandler_EventsWithFilters(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))
	eventStore := NewEventStore(100)

	// Add some test events
	eventStore.Store(event.NewEvent(event.EventTxCreated).WithTxID("tx-1").WithTxType("test"))
	eventStore.Store(event.NewEvent(event.EventTxCompleted).WithTxID("tx-1").WithTxType("test"))

	server := NewAdminServer(
		WithAdminImpl(admin),
		WithEventStore(eventStore),
	)
	handler := server.Handler()

	t.Run("filter events by type", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/events?type=tx_created", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("filter events by tx_id", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/events?tx_id=tx-1", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("events pagination", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/events?limit=10&offset=0", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})
}

// ============================================================================
// Page Data Tests
// ============================================================================

func TestPageData_GetStatusDisplay(t *testing.T) {
	t.Run("get known transaction status", func(t *testing.T) {
		info := GetStatusDisplay(TransactionStatusDisplay, "COMPLETED")
		if info.Label != "已完成" {
			t.Errorf("expected label '已完成', got '%s'", info.Label)
		}
		if info.CSSClass != "status-completed" {
			t.Errorf("expected class 'status-completed', got '%s'", info.CSSClass)
		}
	})

	t.Run("get unknown status", func(t *testing.T) {
		info := GetStatusDisplay(TransactionStatusDisplay, "UNKNOWN_STATUS")
		if info.Label != "UNKNOWN_STATUS" {
			t.Errorf("expected label 'UNKNOWN_STATUS', got '%s'", info.Label)
		}
		if info.CSSClass != "status-unknown" {
			t.Errorf("expected class 'status-unknown', got '%s'", info.CSSClass)
		}
	})

	t.Run("get step status", func(t *testing.T) {
		info := GetStatusDisplay(StepStatusDisplay, "PENDING")
		if info.Label != "待执行" {
			t.Errorf("expected label '待执行', got '%s'", info.Label)
		}
	})
}

func TestPageData_CBStateClass(t *testing.T) {
	tests := []struct {
		state    string
		expected string
	}{
		{"CLOSED", "cb-closed"},
		{"OPEN", "cb-open"},
		{"HALF_OPEN", "cb-half-open"},
		{"UNKNOWN", "status-unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			result := CBStateClass(tt.state)
			if result != tt.expected {
				t.Errorf("CBStateClass(%s) = %s, expected %s", tt.state, result, tt.expected)
			}
		})
	}
}

func TestPageData_CBStateLabel(t *testing.T) {
	tests := []struct {
		state    string
		expected string
	}{
		{"CLOSED", "关闭"},
		{"OPEN", "打开"},
		{"HALF_OPEN", "半开"},
		{"UNKNOWN", "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			result := CBStateLabel(tt.state)
			if result != tt.expected {
				t.Errorf("CBStateLabel(%s) = %s, expected %s", tt.state, result, tt.expected)
			}
		})
	}
}

func TestPageData_ToJSON(t *testing.T) {
	t.Run("convert map to JSON", func(t *testing.T) {
		data := map[string]interface{}{
			"key": "value",
			"num": 123,
		}
		result := ToJSON(data)
		if !strings.Contains(result, "key") || !strings.Contains(result, "value") {
			t.Errorf("expected JSON to contain key and value, got %s", result)
		}
	})

	t.Run("convert nil to JSON", func(t *testing.T) {
		result := ToJSON(nil)
		if result != "{}" {
			t.Errorf("expected '{}', got '%s'", result)
		}
	})

	t.Run("convert struct to JSON", func(t *testing.T) {
		data := struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}{
			Name: "test",
			Age:  25,
		}
		result := ToJSON(data)
		if !strings.Contains(result, "name") || !strings.Contains(result, "test") {
			t.Errorf("expected JSON to contain name and test, got %s", result)
		}
	})
}

func TestPageData_PaginationURL(t *testing.T) {
	t.Run("generate URL with status filter", func(t *testing.T) {
		filter := TransactionFilter{
			Status: []string{"COMPLETED", "FAILED"},
		}
		url := PaginationURL(filter, 2)
		if !strings.Contains(url, "page=2") {
			t.Errorf("expected URL to contain page=2, got %s", url)
		}
		if !strings.Contains(url, "status=COMPLETED") {
			t.Errorf("expected URL to contain status=COMPLETED, got %s", url)
		}
		if !strings.Contains(url, "status=FAILED") {
			t.Errorf("expected URL to contain status=FAILED, got %s", url)
		}
	})

	t.Run("generate URL with tx_type filter", func(t *testing.T) {
		filter := TransactionFilter{
			TxType: "test-type",
		}
		url := PaginationURL(filter, 1)
		if !strings.Contains(url, "tx_type=test-type") {
			t.Errorf("expected URL to contain tx_type=test-type, got %s", url)
		}
	})

	t.Run("generate URL with time range", func(t *testing.T) {
		filter := TransactionFilter{
			StartTime: "2024-01-01T00:00",
			EndTime:   "2024-12-31T23:59",
		}
		url := PaginationURL(filter, 1)
		if !strings.Contains(url, "start_time=") {
			t.Errorf("expected URL to contain start_time, got %s", url)
		}
		if !strings.Contains(url, "end_time=") {
			t.Errorf("expected URL to contain end_time, got %s", url)
		}
	})
}

func TestPageData_PaginationRange(t *testing.T) {
	t.Run("pagination range at start", func(t *testing.T) {
		pages := PaginationRange(1, 10)
		if len(pages) == 0 {
			t.Fatal("expected non-empty page range")
		}
		if pages[0] != 1 {
			t.Errorf("expected first page to be 1, got %d", pages[0])
		}
	})

	t.Run("pagination range in middle", func(t *testing.T) {
		pages := PaginationRange(5, 10)
		// Should include pages around 5
		found := false
		for _, p := range pages {
			if p == 5 {
				found = true
				break
			}
		}
		if !found {
			t.Error("expected current page 5 to be in range")
		}
	})

	t.Run("pagination range at end", func(t *testing.T) {
		pages := PaginationRange(10, 10)
		if len(pages) == 0 {
			t.Fatal("expected non-empty page range")
		}
		// Last page should be 10
		if pages[len(pages)-1] != 10 {
			t.Errorf("expected last page to be 10, got %d", pages[len(pages)-1])
		}
	})

	t.Run("pagination range with few pages", func(t *testing.T) {
		pages := PaginationRange(1, 3)
		if len(pages) != 3 {
			t.Errorf("expected 3 pages, got %d", len(pages))
		}
	})
}

func TestPageData_MathHelpers(t *testing.T) {
	t.Run("add", func(t *testing.T) {
		result := Add(5, 3)
		if result != 8 {
			t.Errorf("Add(5, 3) = %d, expected 8", result)
		}
	})

	t.Run("sub", func(t *testing.T) {
		result := Sub(10, 4)
		if result != 6 {
			t.Errorf("Sub(10, 4) = %d, expected 6", result)
		}
	})

	t.Run("mul", func(t *testing.T) {
		result := Mul(3.0, 4.0)
		if result != 12.0 {
			t.Errorf("Mul(3.0, 4.0) = %f, expected 12.0", result)
		}
	})

	t.Run("div", func(t *testing.T) {
		result := Div(10.0, 2.0)
		if result != 5.0 {
			t.Errorf("Div(10.0, 2.0) = %f, expected 5.0", result)
		}
	})

	t.Run("div by zero", func(t *testing.T) {
		result := Div(10.0, 0.0)
		if result != 0.0 {
			t.Errorf("Div(10.0, 0.0) = %f, expected 0.0", result)
		}
	})

	t.Run("toFloat64", func(t *testing.T) {
		result := ToFloat64(100)
		if result != 100.0 {
			t.Errorf("ToFloat64(100) = %f, expected 100.0", result)
		}
	})
}

func TestPageData_StatusHelpers(t *testing.T) {
	t.Run("StatusClass", func(t *testing.T) {
		tests := []struct {
			status   string
			expected string
		}{
			{"COMPLETED", "status-completed"},
			{"FAILED", "status-failed"},
			{"EXECUTING", "status-executing"},
			{"UNKNOWN", "status-unknown"},
		}

		for _, tt := range tests {
			result := StatusClass(tt.status)
			if result != tt.expected {
				t.Errorf("StatusClass(%s) = %s, expected %s", tt.status, result, tt.expected)
			}
		}
	})

	t.Run("StatusLabel", func(t *testing.T) {
		tests := []struct {
			status   string
			expected string
		}{
			{"COMPLETED", "已完成"},
			{"FAILED", "失败"},
			{"EXECUTING", "执行中"},
			{"UNKNOWN", "UNKNOWN"},
		}

		for _, tt := range tests {
			result := StatusLabel(tt.status)
			if result != tt.expected {
				t.Errorf("StatusLabel(%s) = %s, expected %s", tt.status, result, tt.expected)
			}
		}
	})

	t.Run("StepStatusClass", func(t *testing.T) {
		result := StepStatusClass("COMPLETED")
		if result != "step-completed" {
			t.Errorf("StepStatusClass(COMPLETED) = %s, expected step-completed", result)
		}
	})

	t.Run("StepStatusLabel", func(t *testing.T) {
		result := StepStatusLabel("COMPLETED")
		if result != "已完成" {
			t.Errorf("StepStatusLabel(COMPLETED) = %s, expected 已完成", result)
		}
	})
}

func TestPageData_EventTypeHelpers(t *testing.T) {
	t.Run("EventTypeClass", func(t *testing.T) {
		tests := []struct {
			eventType string
			expected  string
		}{
			{"tx_created", "status-created"},
			{"tx_completed", "status-completed"},
			{"tx_failed", "status-failed"},
			{"tx_cancelled", "status-cancelled"},
			{"tx_compensating", "status-compensating"},
			{"tx_compensated", "status-compensated"},
			{"unknown_event", "status-unknown"},
		}

		for _, tt := range tests {
			result := EventTypeClass(tt.eventType)
			if result != tt.expected {
				t.Errorf("EventTypeClass(%s) = %s, expected %s", tt.eventType, result, tt.expected)
			}
		}
	})

	t.Run("EventTypeLabel", func(t *testing.T) {
		tests := []struct {
			eventType string
			expected  string
		}{
			{"tx_created", "事务创建"},
			{"tx_completed", "事务完成"},
			{"tx_failed", "事务失败"},
			{"unknown_event", "unknown_event"},
		}

		for _, tt := range tests {
			result := EventTypeLabel(tt.eventType)
			if result != tt.expected {
				t.Errorf("EventTypeLabel(%s) = %s, expected %s", tt.eventType, result, tt.expected)
			}
		}
	})
}

func TestPageData_FormatTime(t *testing.T) {
	t.Run("format valid time", func(t *testing.T) {
		tm := time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC)
		result := FormatTime(tm)
		if result != "2024-06-15 10:30:45" {
			t.Errorf("FormatTime() = %s, expected 2024-06-15 10:30:45", result)
		}
	})

	t.Run("format zero time", func(t *testing.T) {
		result := FormatTime(time.Time{})
		if result != "-" {
			t.Errorf("FormatTime(zero) = %s, expected '-'", result)
		}
	})

	t.Run("format time pointer", func(t *testing.T) {
		tm := time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC)
		result := FormatTimePtr(&tm)
		if result != "2024-06-15 10:30:45" {
			t.Errorf("FormatTimePtr() = %s, expected 2024-06-15 10:30:45", result)
		}
	})

	t.Run("format nil time pointer", func(t *testing.T) {
		result := FormatTimePtr(nil)
		if result != "-" {
			t.Errorf("FormatTimePtr(nil) = %s, expected '-'", result)
		}
	})
}

func TestPageData_HasStatus(t *testing.T) {
	statuses := []string{"COMPLETED", "FAILED", "EXECUTING"}

	t.Run("has existing status", func(t *testing.T) {
		if !HasStatus(statuses, "COMPLETED") {
			t.Error("expected HasStatus to return true for COMPLETED")
		}
	})

	t.Run("does not have status", func(t *testing.T) {
		if HasStatus(statuses, "PENDING") {
			t.Error("expected HasStatus to return false for PENDING")
		}
	})

	t.Run("empty statuses", func(t *testing.T) {
		if HasStatus([]string{}, "COMPLETED") {
			t.Error("expected HasStatus to return false for empty list")
		}
	})
}

func TestPageData_NewPageDataFunctions(t *testing.T) {
	t.Run("NewPageData", func(t *testing.T) {
		data := NewPageData("Test Title", "index")
		if data.Title != "Test Title" {
			t.Errorf("expected title 'Test Title', got '%s'", data.Title)
		}
		if data.CurrentPage != "index" {
			t.Errorf("expected current page 'index', got '%s'", data.CurrentPage)
		}
		if len(data.NavItems) == 0 {
			t.Error("expected nav items to be populated")
		}
	})

	t.Run("NewIndexPageData", func(t *testing.T) {
		stats := StatsResponse{TotalTransactions: 100}
		data := NewIndexPageData(stats)
		if data.Stats.TotalTransactions != 100 {
			t.Errorf("expected 100 total transactions, got %d", data.Stats.TotalTransactions)
		}
	})

	t.Run("NewPaginationData", func(t *testing.T) {
		data := NewPaginationData(2, 10, 20, 200)
		if data.CurrentPage != 2 {
			t.Errorf("expected current page 2, got %d", data.CurrentPage)
		}
		if data.TotalPages != 10 {
			t.Errorf("expected 10 total pages, got %d", data.TotalPages)
		}
		if !data.HasPrev {
			t.Error("expected HasPrev to be true")
		}
		if !data.HasNext {
			t.Error("expected HasNext to be true")
		}
	})

	t.Run("NewTransactionsPageData", func(t *testing.T) {
		transactions := []TransactionSummary{{TxID: "tx-1"}}
		filter := TransactionFilter{TxType: "test"}
		pagination := NewPaginationData(1, 1, 20, 1)
		data := NewTransactionsPageData(transactions, filter, pagination)
		if len(data.Transactions) != 1 {
			t.Errorf("expected 1 transaction, got %d", len(data.Transactions))
		}
	})

	t.Run("NewTransactionDetailPageData", func(t *testing.T) {
		tx := TransactionDetail{TxID: "tx-1", Status: "EXECUTING"}
		steps := []StepDetail{{StepName: "step1"}}
		data := NewTransactionDetailPageData(tx, steps)
		if data.Transaction.TxID != "tx-1" {
			t.Errorf("expected tx-1, got %s", data.Transaction.TxID)
		}
		if !data.CanForceComplete {
			t.Error("expected CanForceComplete to be true for EXECUTING status")
		}
	})

	t.Run("NewRecoveryPageData", func(t *testing.T) {
		stats := RecoveryStatsResponse{IsRunning: true}
		data := NewRecoveryPageData(stats)
		if !data.Stats.IsRunning {
			t.Error("expected IsRunning to be true")
		}
	})

	t.Run("NewCircuitBreakersPageData", func(t *testing.T) {
		breakers := []CircuitBreakerInfo{{Service: "test-service"}}
		data := NewCircuitBreakersPageData(breakers)
		if len(data.Breakers) != 1 {
			t.Errorf("expected 1 breaker, got %d", len(data.Breakers))
		}
	})

	t.Run("NewEventsPageData", func(t *testing.T) {
		events := []StoredEvent{{Type: "tx_created"}}
		eventTypes := []string{"tx_created", "tx_completed"}
		filter := EventFilter{Type: "tx_created"}
		data := NewEventsPageData(events, eventTypes, filter)
		if len(data.Events) != 1 {
			t.Errorf("expected 1 event, got %d", len(data.Events))
		}
		if len(data.EventTypes) != 2 {
			t.Errorf("expected 2 event types, got %d", len(data.EventTypes))
		}
	})
}

func TestPageData_CanStatusFunctions(t *testing.T) {
	t.Run("canForceCompleteStatus", func(t *testing.T) {
		tests := []struct {
			status   string
			expected bool
		}{
			{"LOCKED", true},
			{"EXECUTING", true},
			{"CONFIRMING", true},
			{"COMPLETED", false},
			{"FAILED", false},
		}

		for _, tt := range tests {
			result := canForceCompleteStatus(tt.status)
			if result != tt.expected {
				t.Errorf("canForceCompleteStatus(%s) = %v, expected %v", tt.status, result, tt.expected)
			}
		}
	})

	t.Run("canForceCancelStatus", func(t *testing.T) {
		tests := []struct {
			status   string
			expected bool
		}{
			{"CREATED", true},
			{"LOCKED", true},
			{"EXECUTING", true},
			{"FAILED", true},
			{"COMPLETED", false},
			{"COMPENSATED", false},
		}

		for _, tt := range tests {
			result := canForceCancelStatus(tt.status)
			if result != tt.expected {
				t.Errorf("canForceCancelStatus(%s) = %v, expected %v", tt.status, result, tt.expected)
			}
		}
	})

	t.Run("canRetryStatus", func(t *testing.T) {
		tests := []struct {
			status   string
			expected bool
		}{
			{"FAILED", true},
			{"COMPLETED", false},
			{"EXECUTING", false},
		}

		for _, tt := range tests {
			result := canRetryStatus(tt.status)
			if result != tt.expected {
				t.Errorf("canRetryStatus(%s) = %v, expected %v", tt.status, result, tt.expected)
			}
		}
	})
}

// ============================================================================
// Event Store Tests
// ============================================================================

func TestEventStore_Clear(t *testing.T) {
	store := NewEventStore(100)

	// Add some events
	store.Store(event.NewEvent(event.EventTxCreated).WithTxID("tx-1"))
	store.Store(event.NewEvent(event.EventTxCompleted).WithTxID("tx-1"))
	store.Store(event.NewEvent(event.EventTxFailed).WithTxID("tx-2"))

	if store.Len() != 3 {
		t.Fatalf("expected 3 events, got %d", store.Len())
	}

	// Clear the store
	store.Clear()

	if store.Len() != 0 {
		t.Errorf("expected 0 events after clear, got %d", store.Len())
	}

	// Verify list returns empty
	events := store.List(EventFilter{})
	if len(events) != 0 {
		t.Errorf("expected empty list after clear, got %d events", len(events))
	}
}

func TestEventStore_List_EdgeCases(t *testing.T) {
	t.Run("list from empty store", func(t *testing.T) {
		store := NewEventStore(100)
		events := store.List(EventFilter{})
		if len(events) != 0 {
			t.Errorf("expected empty list, got %d events", len(events))
		}
	})

	t.Run("list with offset beyond events", func(t *testing.T) {
		store := NewEventStore(100)
		store.Store(event.NewEvent(event.EventTxCreated).WithTxID("tx-1"))
		store.Store(event.NewEvent(event.EventTxCompleted).WithTxID("tx-1"))

		events := store.List(EventFilter{Offset: 100})
		if len(events) != 0 {
			t.Errorf("expected empty list with large offset, got %d events", len(events))
		}
	})

	t.Run("list with limit larger than events", func(t *testing.T) {
		store := NewEventStore(100)
		store.Store(event.NewEvent(event.EventTxCreated).WithTxID("tx-1"))
		store.Store(event.NewEvent(event.EventTxCompleted).WithTxID("tx-1"))

		events := store.List(EventFilter{Limit: 1000})
		if len(events) != 2 {
			t.Errorf("expected 2 events, got %d", len(events))
		}
	})

	t.Run("list with zero limit uses default", func(t *testing.T) {
		store := NewEventStore(100)
		for i := 0; i < 150; i++ {
			store.Store(event.NewEvent(event.EventTxCreated).WithTxID(fmt.Sprintf("tx-%d", i)))
		}

		events := store.List(EventFilter{Limit: 0})
		// Default limit is 100
		if len(events) != 100 {
			t.Errorf("expected 100 events with default limit, got %d", len(events))
		}
	})

	t.Run("list with type filter no matches", func(t *testing.T) {
		store := NewEventStore(100)
		store.Store(event.NewEvent(event.EventTxCreated).WithTxID("tx-1"))
		store.Store(event.NewEvent(event.EventTxCompleted).WithTxID("tx-1"))

		events := store.List(EventFilter{Type: "non_existent_type"})
		if len(events) != 0 {
			t.Errorf("expected empty list with non-matching type, got %d events", len(events))
		}
	})

	t.Run("list with tx_id filter no matches", func(t *testing.T) {
		store := NewEventStore(100)
		store.Store(event.NewEvent(event.EventTxCreated).WithTxID("tx-1"))
		store.Store(event.NewEvent(event.EventTxCompleted).WithTxID("tx-1"))

		events := store.List(EventFilter{TxID: "non-existent-tx"})
		if len(events) != 0 {
			t.Errorf("expected empty list with non-matching tx_id, got %d events", len(events))
		}
	})

	t.Run("list returns events in reverse order (newest first)", func(t *testing.T) {
		store := NewEventStore(100)
		store.Store(event.NewEvent(event.EventTxCreated).WithTxID("tx-1"))
		time.Sleep(1 * time.Millisecond) // Ensure different timestamps
		store.Store(event.NewEvent(event.EventTxCompleted).WithTxID("tx-2"))

		events := store.List(EventFilter{})
		if len(events) != 2 {
			t.Fatalf("expected 2 events, got %d", len(events))
		}
		// Newest event (tx-2) should be first
		if events[0].TxID != "tx-2" {
			t.Errorf("expected newest event first (tx-2), got %s", events[0].TxID)
		}
	})
}

func TestEventStore_Count(t *testing.T) {
	store := NewEventStore(100)

	// Add events with different types and tx_ids
	store.Store(event.NewEvent(event.EventTxCreated).WithTxID("tx-1").WithTxType("type-a"))
	store.Store(event.NewEvent(event.EventTxCompleted).WithTxID("tx-1").WithTxType("type-a"))
	store.Store(event.NewEvent(event.EventTxCreated).WithTxID("tx-2").WithTxType("type-b"))

	t.Run("count all events", func(t *testing.T) {
		count := store.Count(EventFilter{})
		if count != 3 {
			t.Errorf("expected count 3, got %d", count)
		}
	})

	t.Run("count with type filter", func(t *testing.T) {
		count := store.Count(EventFilter{Type: string(event.EventTxCreated)})
		if count != 2 {
			t.Errorf("expected count 2 for tx_created, got %d", count)
		}
	})

	t.Run("count with tx_id filter", func(t *testing.T) {
		count := store.Count(EventFilter{TxID: "tx-1"})
		if count != 2 {
			t.Errorf("expected count 2 for tx-1, got %d", count)
		}
	})

	t.Run("count with combined filters", func(t *testing.T) {
		count := store.Count(EventFilter{Type: string(event.EventTxCreated), TxID: "tx-1"})
		if count != 1 {
			t.Errorf("expected count 1 for tx_created and tx-1, got %d", count)
		}
	})
}

func TestEventStore_MaxEvents(t *testing.T) {
	t.Run("events are trimmed when exceeding max", func(t *testing.T) {
		store := NewEventStore(5)

		// Add more events than max
		for i := 0; i < 10; i++ {
			store.Store(event.NewEvent(event.EventTxCreated).WithTxID(fmt.Sprintf("tx-%d", i)))
		}

		if store.Len() != 5 {
			t.Errorf("expected 5 events (max), got %d", store.Len())
		}

		// Verify oldest events were removed (tx-0 through tx-4 should be gone)
		events := store.List(EventFilter{})
		for _, e := range events {
			if e.TxID == "tx-0" || e.TxID == "tx-1" || e.TxID == "tx-2" || e.TxID == "tx-3" || e.TxID == "tx-4" {
				t.Errorf("expected old event %s to be removed", e.TxID)
			}
		}
	})

	t.Run("default max events when zero provided", func(t *testing.T) {
		store := NewEventStore(0)
		// Default should be 1000
		for i := 0; i < 1100; i++ {
			store.Store(event.NewEvent(event.EventTxCreated).WithTxID(fmt.Sprintf("tx-%d", i)))
		}

		if store.Len() != 1000 {
			t.Errorf("expected 1000 events (default max), got %d", store.Len())
		}
	})
}

func TestEventStore_GetEventTypes(t *testing.T) {
	store := NewEventStore(100)

	t.Run("empty store returns empty types", func(t *testing.T) {
		types := store.GetEventTypes()
		if len(types) != 0 {
			t.Errorf("expected empty types, got %d", len(types))
		}
	})

	t.Run("returns unique event types", func(t *testing.T) {
		store.Store(event.NewEvent(event.EventTxCreated).WithTxID("tx-1"))
		store.Store(event.NewEvent(event.EventTxCreated).WithTxID("tx-2"))
		store.Store(event.NewEvent(event.EventTxCompleted).WithTxID("tx-1"))
		store.Store(event.NewEvent(event.EventTxFailed).WithTxID("tx-3"))

		types := store.GetEventTypes()
		if len(types) != 3 {
			t.Errorf("expected 3 unique types, got %d", len(types))
		}

		// Verify all expected types are present
		typeSet := make(map[string]bool)
		for _, t := range types {
			typeSet[t] = true
		}
		if !typeSet[string(event.EventTxCreated)] {
			t.Error("expected tx_created type")
		}
		if !typeSet[string(event.EventTxCompleted)] {
			t.Error("expected tx_completed type")
		}
		if !typeSet[string(event.EventTxFailed)] {
			t.Error("expected tx_failed type")
		}
	})
}

func TestEventStore_EventHandler(t *testing.T) {
	store := NewEventStore(100)

	handler := store.EventHandler()
	if handler == nil {
		t.Fatal("expected event handler to be returned")
	}

	// Use the handler to store an event
	e := event.NewEvent(event.EventTxCreated).WithTxID("tx-handler-test")
	err := handler(context.Background(), e)
	if err != nil {
		t.Fatalf("unexpected error from handler: %v", err)
	}

	// Verify event was stored
	events := store.List(EventFilter{TxID: "tx-handler-test"})
	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}
}

func TestEventStore_StoreWithError(t *testing.T) {
	store := NewEventStore(100)

	// Store event with error
	testErr := errors.New("test error")
	e := event.NewEvent(event.EventTxFailed).WithTxID("tx-error").WithError(testErr)
	store.Store(e)

	events := store.List(EventFilter{TxID: "tx-error"})
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	if events[0].Error != "test error" {
		t.Errorf("expected error message 'test error', got '%s'", events[0].Error)
	}
}

// ============================================================================
// Additional Coverage Tests
// ============================================================================

func TestAdminServer_WithServerCoordinator(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	coord := rte.NewCoordinator(
		rte.WithStore(store),
		rte.WithLocker(locker),
		rte.WithBreaker(breaker),
		rte.WithEventBus(eventBus),
	)

	server := NewAdminServer(
		WithServerCoordinator(coord),
	)

	if server.coordinator == nil {
		t.Error("expected coordinator to be set")
	}
}

func TestAdminServer_WithTemplates(t *testing.T) {
	// Create a custom template
	customTemplate := template.New("custom")
	customTemplate.Parse("{{define \"test\"}}test content{{end}}")

	server := NewAdminServer(
		WithTemplates(customTemplate),
	)

	if server.templates == nil {
		t.Error("expected templates to be set")
	}
}

func TestFallbackPageHandler(t *testing.T) {
	// Create a fallback handler directly
	handler := &fallbackPageHandler{}

	t.Run("HandleIndex", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		w := httptest.NewRecorder()

		handler.HandleIndex(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "RTE 管理控制台") {
			t.Error("expected response to contain 'RTE 管理控制台'")
		}
	})

	t.Run("HandleTransactions", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/transactions", nil)
		w := httptest.NewRecorder()

		handler.HandleTransactions(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "事务列表") {
			t.Error("expected response to contain '事务列表'")
		}
	})

	t.Run("HandleTransactionDetail", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/transactions/tx-1", nil)
		w := httptest.NewRecorder()

		handler.HandleTransactionDetail(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "事务详情") {
			t.Error("expected response to contain '事务详情'")
		}
	})

	t.Run("HandleRecovery", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/recovery", nil)
		w := httptest.NewRecorder()

		handler.HandleRecovery(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "恢复监控") {
			t.Error("expected response to contain '恢复监控'")
		}
	})

	t.Run("HandleCircuitBreakers", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/circuit-breakers", nil)
		w := httptest.NewRecorder()

		handler.HandleCircuitBreakers(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "熔断器面板") {
			t.Error("expected response to contain '熔断器面板'")
		}
	})

	t.Run("HandleEvents", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/events", nil)
		w := httptest.NewRecorder()

		handler.HandleEvents(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "事件日志") {
			t.Error("expected response to contain '事件日志'")
		}
	})
}

func TestPageData_StepStatusHelpers_Unknown(t *testing.T) {
	t.Run("StepStatusClass unknown status", func(t *testing.T) {
		result := StepStatusClass("UNKNOWN_STATUS")
		if result != "status-unknown" {
			t.Errorf("StepStatusClass(UNKNOWN_STATUS) = %s, expected status-unknown", result)
		}
	})

	t.Run("StepStatusLabel unknown status", func(t *testing.T) {
		result := StepStatusLabel("UNKNOWN_STATUS")
		if result != "UNKNOWN_STATUS" {
			t.Errorf("StepStatusLabel(UNKNOWN_STATUS) = %s, expected UNKNOWN_STATUS", result)
		}
	})
}

func TestAPIHandler_HandleGetCircuitBreakers(t *testing.T) {
	breaker := newMockBreaker()

	// Pre-populate some circuit breakers
	breaker.Get("service-1")
	breaker.Get("service-2")

	server := NewAdminServer(WithServerBreaker(breaker))
	handler := server.Handler()

	t.Run("get circuit breakers", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/circuit-breakers", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if !resp.Success {
			t.Error("expected success to be true")
		}
	})

	t.Run("get circuit breakers without breaker configured", func(t *testing.T) {
		serverNoBreaker := NewAdminServer()
		handlerNoBreaker := serverNoBreaker.Handler()

		req := httptest.NewRequest("GET", "/api/circuit-breakers", nil)
		w := httptest.NewRecorder()

		handlerNoBreaker.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})
}

func TestAPIHandler_HandleResetCircuitBreaker_Extended(t *testing.T) {
	t.Run("reset circuit breaker without breaker configured", func(t *testing.T) {
		serverNoBreaker := NewAdminServer()
		handlerNoBreaker := serverNoBreaker.Handler()

		req := httptest.NewRequest("POST", "/api/circuit-breakers/test-service/reset", nil)
		w := httptest.NewRecorder()

		handlerNoBreaker.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})
}

func TestAPIHandler_HandleListEvents(t *testing.T) {
	eventStore := NewEventStore(100)

	// Add some test events
	eventStore.Store(event.NewEvent(event.EventTxCreated).WithTxID("tx-1").WithTxType("test"))
	eventStore.Store(event.NewEvent(event.EventTxCompleted).WithTxID("tx-1").WithTxType("test"))

	server := NewAdminServer(WithEventStore(eventStore))
	handler := server.Handler()

	t.Run("list events", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/events", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var resp APIResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if !resp.Success {
			t.Error("expected success to be true")
		}
	})

	t.Run("list events with filters", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/events?type=tx_created&tx_id=tx-1&limit=10&offset=0", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("list events without event store configured", func(t *testing.T) {
		serverNoEvents := NewAdminServer()
		handlerNoEvents := serverNoEvents.Handler()

		req := httptest.NewRequest("GET", "/api/events", nil)
		w := httptest.NewRecorder()

		handlerNoEvents.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})
}

func TestAPIHandler_ParseTransactionFilter_Extended(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	server := NewAdminServer(WithAdminImpl(admin))
	handler := server.Handler()

	t.Run("filter with multiple statuses", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions?status=COMPLETED&status=FAILED&status=EXECUTING", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("filter with pagination", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions?limit=50&offset=10", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("filter with time range", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions?start_time=2024-01-01T00:00:00Z&end_time=2025-12-31T23:59:59Z", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("filter with invalid time format", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/transactions?start_time=invalid-time", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		// Should still return 200 but ignore invalid time
		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})
}

func TestAdmin_ListTransactions_Extended(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	// Create test transactions
	createTestTransaction(store, "tx-list-1", "type-a", rte.TxStatusCompleted, []string{"step1"})
	createTestTransaction(store, "tx-list-2", "type-a", rte.TxStatusFailed, []string{"step1"})

	t.Run("list with nil filter uses defaults", func(t *testing.T) {
		result, err := admin.ListTransactions(context.Background(), nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.Total != 2 {
			t.Errorf("expected 2 transactions, got %d", result.Total)
		}
	})
}

func TestAdmin_GetTransaction_Extended(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	// Create test transaction with steps
	tx := rte.NewStoreTx("tx-detail-ext", "test-type", []string{"step1", "step2"})
	tx.Status = rte.TxStatusExecuting
	tx.Context = &rte.StoreTxContext{
		TxID:   "tx-detail-ext",
		TxType: "test-type",
		Input:  map[string]any{"key": "value"},
		Output: map[string]any{"result": "success"},
	}
	store.CreateTransaction(context.Background(), tx)

	// Create step records
	for i := 0; i < 2; i++ {
		step := rte.NewStoreStepRecord("tx-detail-ext", i, fmt.Sprintf("step%d", i+1))
		store.CreateStep(context.Background(), step)
	}

	t.Run("get transaction with context", func(t *testing.T) {
		detail, err := admin.GetTransaction(context.Background(), "tx-detail-ext")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if detail.Transaction == nil {
			t.Fatal("expected transaction to be returned")
		}
		if len(detail.Steps) != 2 {
			t.Errorf("expected 2 steps, got %d", len(detail.Steps))
		}
	})
}

func TestAdmin_GetStats_Extended(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))

	// Create transactions with all status types
	createTestTransaction(store, "tx-stat-created", "type-a", rte.TxStatusCreated, []string{"step1"})
	createTestTransaction(store, "tx-stat-locked", "type-a", rte.TxStatusLocked, []string{"step1"})
	createTestTransaction(store, "tx-stat-timeout", "type-a", rte.TxStatusTimeout, []string{"step1"})
	createTestTransaction(store, "tx-stat-cancelled", "type-a", rte.TxStatusCancelled, []string{"step1"})
	createTestTransaction(store, "tx-stat-comp-failed", "type-a", rte.TxStatusCompensationFailed, []string{"step1"})

	t.Run("get stats with all status types", func(t *testing.T) {
		stats, err := admin.GetStats(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if stats.TotalTransactions != 5 {
			t.Errorf("expected 5 total transactions, got %d", stats.TotalTransactions)
		}
		// Pending includes CREATED, LOCKED, EXECUTING statuses
		if stats.PendingTransactions < 2 {
			t.Errorf("expected at least 2 pending transactions, got %d", stats.PendingTransactions)
		}
	})
}

func TestPageHandler_RecoveryWithWorker(t *testing.T) {
	store := newMockStore()
	admin := NewAdmin(WithAdminStore(store))
	recoveryWorker := recovery.NewWorker()

	server := NewAdminServer(
		WithAdminImpl(admin),
		WithServerRecovery(recoveryWorker),
	)
	handler := server.Handler()

	t.Run("render recovery page with worker", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/recovery", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})
}

func TestPageData_TemplateFuncs(t *testing.T) {
	funcs := TemplateFuncs()

	if funcs == nil {
		t.Fatal("expected template funcs to be returned")
	}

	// Verify some expected functions exist
	expectedFuncs := []string{
		"statusClass", "statusLabel", "stepStatusClass", "stepStatusLabel",
		"cbStateClass", "cbStateLabel", "eventTypeClass", "eventTypeLabel",
		"formatTime", "formatTimePtr", "toJSON", "paginationURL", "paginationRange",
		"hasStatus", "add", "sub", "mul", "div", "float64",
	}

	for _, name := range expectedFuncs {
		if _, ok := funcs[name]; !ok {
			t.Errorf("expected template func '%s' to exist", name)
		}
	}
}

func TestAPIHandler_HandleForceComplete_Extended(t *testing.T) {
	store := newMockStore()
	eventBus := event.NewMemoryEventBus()
	admin := NewAdmin(
		WithAdminStore(store),
		WithAdminEventBus(eventBus),
	)

	server := NewAdminServer(WithAdminImpl(admin))
	handler := server.Handler()

	t.Run("force complete with invalid request body", func(t *testing.T) {
		createTestTransaction(store, "tx-fc-invalid", "test-type", rte.TxStatusExecuting, []string{"step1"})

		body := strings.NewReader(`invalid json`)
		req := httptest.NewRequest("POST", "/api/transactions/tx-fc-invalid/force-complete", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}
	})

	t.Run("force complete without admin configured", func(t *testing.T) {
		serverNoAdmin := NewAdminServer()
		handlerNoAdmin := serverNoAdmin.Handler()

		body := strings.NewReader(`{"reason": "test"}`)
		req := httptest.NewRequest("POST", "/api/transactions/tx-1/force-complete", body)
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handlerNoAdmin.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})
}

func TestAPIHandler_HandleListTransactions_Extended(t *testing.T) {
	t.Run("list transactions without admin configured", func(t *testing.T) {
		serverNoAdmin := NewAdminServer()
		handlerNoAdmin := serverNoAdmin.Handler()

		req := httptest.NewRequest("GET", "/api/transactions", nil)
		w := httptest.NewRecorder()

		handlerNoAdmin.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})
}

func TestAPIHandler_HandleGetTransaction_Extended(t *testing.T) {
	t.Run("get transaction without admin configured", func(t *testing.T) {
		serverNoAdmin := NewAdminServer()
		handlerNoAdmin := serverNoAdmin.Handler()

		req := httptest.NewRequest("GET", "/api/transactions/tx-1", nil)
		w := httptest.NewRecorder()

		handlerNoAdmin.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})
}
