package recovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"rte/event"
	"rte/lock"

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
}

func newMockStore() *mockStore {
	return &mockStore{
		transactions: make(map[string]*StoreTx),
	}
}

func (s *mockStore) GetTransaction(ctx context.Context, txID string) (*StoreTx, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tx, exists := s.transactions[txID]
	if !exists {
		return nil, errors.New("transaction not found")
	}
	txCopy := *tx
	return &txCopy, nil
}

func (s *mockStore) UpdateTransaction(ctx context.Context, tx *StoreTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	txCopy := *tx
	s.transactions[tx.TxID] = &txCopy
	return nil
}

func (s *mockStore) GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*StoreTx, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*StoreTx
	threshold := time.Now().Add(-olderThan)
	for _, tx := range s.transactions {
		if (tx.Status == "LOCKED" || tx.Status == "EXECUTING") && tx.UpdatedAt.Before(threshold) {
			txCopy := *tx
			result = append(result, &txCopy)
		}
	}
	return result, nil
}

func (s *mockStore) GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*StoreTx, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*StoreTx
	for _, tx := range s.transactions {
		if tx.Status == "FAILED" && tx.RetryCount < maxRetries {
			txCopy := *tx
			result = append(result, &txCopy)
		}
	}
	return result, nil
}

func (s *mockStore) AddTransaction(tx *StoreTx) {
	s.mu.Lock()
	defer s.mu.Unlock()
	txCopy := *tx
	s.transactions[tx.TxID] = &txCopy
}

// mockLocker implements lock.Locker for testing with proper concurrent access handling
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

	// Check all keys first
	for _, key := range keys {
		if l.locks[key] {
			return nil, errors.New("lock already held")
		}
	}

	// Acquire all keys atomically
	for _, key := range keys {
		l.locks[key] = true
	}

	return &mockLockHandle{locker: l, keys: keys}, nil
}

func (l *mockLocker) IsLocked(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.locks[key]
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

// mockCoordinator implements Coordinator for testing
type mockCoordinator struct {
	mu           sync.Mutex
	resumeCalls  []string
	resumeErr    error
	resumeDelay  time.Duration
	resumeResult map[string]error
}

func newMockCoordinator() *mockCoordinator {
	return &mockCoordinator{
		resumeCalls:  make([]string, 0),
		resumeResult: make(map[string]error),
	}
}

func (c *mockCoordinator) Resume(ctx context.Context, txID string) error {
	if c.resumeDelay > 0 {
		time.Sleep(c.resumeDelay)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeCalls = append(c.resumeCalls, txID)
	if err, ok := c.resumeResult[txID]; ok {
		return err
	}
	return c.resumeErr
}

func (c *mockCoordinator) GetResumeCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]string, len(c.resumeCalls))
	copy(result, c.resumeCalls)
	return result
}

func (c *mockCoordinator) SetResumeError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeErr = err
}

func (c *mockCoordinator) SetResumeErrorForTx(txID string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeResult[txID] = err
}

// silentLogger suppresses log output during tests
type silentLogger struct{}

func (l *silentLogger) Printf(format string, v ...any) {}

// ============================================================================
// Unit Tests
// ============================================================================

func TestWorker_NewWorker(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	coordinator := newMockCoordinator()
	eventBus := event.NewMemoryEventBus()

	worker := NewWorker(
		WithStore(store),
		WithLocker(locker),
		WithCoordinator(coordinator),
		WithEventBus(eventBus),
		WithLogger(&silentLogger{}),
	)

	if worker == nil {
		t.Fatal("expected worker to be created")
	}
	if worker.store == nil {
		t.Error("expected store to be set")
	}
	if worker.locker == nil {
		t.Error("expected locker to be set")
	}
	if worker.coordinator == nil {
		t.Error("expected coordinator to be set")
	}
}

func TestWorker_StartStop(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	coordinator := newMockCoordinator()

	worker := NewWorker(
		WithStore(store),
		WithLocker(locker),
		WithCoordinator(coordinator),
		WithConfig(Config{
			RecoveryInterval: 100 * time.Millisecond,
			StuckThreshold:   1 * time.Second,
			MaxRetries:       3,
			LockTTL:          30 * time.Second,
		}),
		WithLogger(&silentLogger{}),
	)

	ctx := context.Background()

	// Start worker
	err := worker.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}

	if !worker.IsRunning() {
		t.Error("expected worker to be running")
	}

	// Try to start again (should fail)
	err = worker.Start(ctx)
	if err == nil {
		t.Error("expected error when starting already running worker")
	}

	// Stop worker
	worker.Stop()

	if worker.IsRunning() {
		t.Error("expected worker to be stopped")
	}
}

func TestWorker_RecoverStuckTransaction(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	coordinator := newMockCoordinator()
	eventBus := event.NewMemoryEventBus()

	// Add a stuck transaction
	stuckTime := time.Now().Add(-10 * time.Minute)
	store.AddTransaction(&StoreTx{
		TxID:       "tx-stuck-1",
		TxType:     "test",
		Status:     "EXECUTING",
		RetryCount: 0,
		MaxRetries: 3,
		UpdatedAt:  stuckTime,
	})

	worker := NewWorker(
		WithStore(store),
		WithLocker(locker),
		WithCoordinator(coordinator),
		WithEventBus(eventBus),
		WithConfig(Config{
			RecoveryInterval: 100 * time.Millisecond,
			StuckThreshold:   5 * time.Minute,
			MaxRetries:       3,
			LockTTL:          30 * time.Second,
		}),
		WithLogger(&silentLogger{}),
	)

	// Run a single scan
	worker.ScanOnce(context.Background())

	// Verify coordinator was called
	calls := coordinator.GetResumeCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 resume call, got %d", len(calls))
	}
	if calls[0] != "tx-stuck-1" {
		t.Errorf("expected resume call for tx-stuck-1, got %s", calls[0])
	}

	// Verify stats
	stats := worker.Stats()
	if stats.ScannedCount != 1 {
		t.Errorf("expected scanned count 1, got %d", stats.ScannedCount)
	}
	if stats.ProcessedCount != 1 {
		t.Errorf("expected processed count 1, got %d", stats.ProcessedCount)
	}
}

func TestWorker_RetryFailedTransaction(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	coordinator := newMockCoordinator()
	eventBus := event.NewMemoryEventBus()

	// Add a failed transaction that can be retried
	store.AddTransaction(&StoreTx{
		TxID:       "tx-failed-1",
		TxType:     "test",
		Status:     "FAILED",
		RetryCount: 1,
		MaxRetries: 3,
		UpdatedAt:  time.Now(),
	})

	worker := NewWorker(
		WithStore(store),
		WithLocker(locker),
		WithCoordinator(coordinator),
		WithEventBus(eventBus),
		WithConfig(Config{
			RecoveryInterval: 100 * time.Millisecond,
			StuckThreshold:   5 * time.Minute,
			MaxRetries:       3,
			LockTTL:          30 * time.Second,
		}),
		WithLogger(&silentLogger{}),
	)

	// Run a single scan
	worker.ScanOnce(context.Background())

	// Verify coordinator was called
	calls := coordinator.GetResumeCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 resume call, got %d", len(calls))
	}
	if calls[0] != "tx-failed-1" {
		t.Errorf("expected resume call for tx-failed-1, got %s", calls[0])
	}
}

func TestWorker_MaxRetriesExceeded(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	coordinator := newMockCoordinator()
	eventBus := event.NewMemoryEventBus()

	// Track critical alerts
	var criticalAlerts []event.Event
	var alertMu sync.Mutex
	eventBus.Subscribe(event.EventAlertCritical, func(ctx context.Context, e event.Event) error {
		alertMu.Lock()
		criticalAlerts = append(criticalAlerts, e)
		alertMu.Unlock()
		return nil
	})

	// Add a failed transaction that has exceeded max retries
	// Note: RetryCount=2 so it will be returned by GetRetryableTransactions (2 < 3)
	// but when we check in retryTransaction, we'll see it's at max
	store.AddTransaction(&StoreTx{
		TxID:       "tx-max-retries",
		TxType:     "test",
		Status:     "FAILED",
		RetryCount: 3, // Already at max
		MaxRetries: 3,
		UpdatedAt:  time.Now(),
	})

	// Use a custom store that returns the transaction even though it's at max retries
	// This simulates the race condition where a transaction reaches max retries
	// between being queried and being processed
	customStore := &maxRetriesTestStore{
		mockStore: store,
	}

	worker := NewWorker(
		WithStore(customStore),
		WithLocker(locker),
		WithCoordinator(coordinator),
		WithEventBus(eventBus),
		WithConfig(Config{
			RecoveryInterval: 100 * time.Millisecond,
			StuckThreshold:   5 * time.Minute,
			MaxRetries:       3,
			LockTTL:          30 * time.Second,
		}),
		WithLogger(&silentLogger{}),
	)

	// Run a single scan
	worker.ScanOnce(context.Background())

	// Verify coordinator was NOT called (max retries exceeded)
	calls := coordinator.GetResumeCalls()
	if len(calls) != 0 {
		t.Fatalf("expected 0 resume calls, got %d", len(calls))
	}

	// Verify critical alert was published
	alertMu.Lock()
	defer alertMu.Unlock()
	if len(criticalAlerts) != 1 {
		t.Fatalf("expected 1 critical alert, got %d", len(criticalAlerts))
	}
	if criticalAlerts[0].TxID != "tx-max-retries" {
		t.Errorf("expected alert for tx-max-retries, got %s", criticalAlerts[0].TxID)
	}
}

// maxRetriesTestStore wraps mockStore to return transactions at max retries
type maxRetriesTestStore struct {
	*mockStore
}

func (s *maxRetriesTestStore) GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*StoreTx, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*StoreTx
	for _, tx := range s.transactions {
		// Return all FAILED transactions, even those at max retries
		// This simulates the scenario where we need to check and alert
		if tx.Status == "FAILED" {
			txCopy := *tx
			result = append(result, &txCopy)
		}
	}
	return result, nil
}

func TestWorker_SkipsAlreadyRecoveredTransaction(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	coordinator := newMockCoordinator()

	// Add a transaction that was stuck but is now completed
	stuckTime := time.Now().Add(-10 * time.Minute)
	store.AddTransaction(&StoreTx{
		TxID:       "tx-recovered",
		TxType:     "test",
		Status:     "COMPLETED", // Already recovered
		RetryCount: 0,
		MaxRetries: 3,
		UpdatedAt:  stuckTime,
	})

	worker := NewWorker(
		WithStore(store),
		WithLocker(locker),
		WithCoordinator(coordinator),
		WithConfig(Config{
			RecoveryInterval: 100 * time.Millisecond,
			StuckThreshold:   5 * time.Minute,
			MaxRetries:       3,
			LockTTL:          30 * time.Second,
		}),
		WithLogger(&silentLogger{}),
	)

	// Run a single scan
	worker.ScanOnce(context.Background())

	// Verify coordinator was NOT called (transaction already recovered)
	calls := coordinator.GetResumeCalls()
	if len(calls) != 0 {
		t.Fatalf("expected 0 resume calls, got %d", len(calls))
	}
}

// ============================================================================
// Property-Based Tests
// ============================================================================

// Property 7: Recovery Worker Coordination
// For any stuck transaction, only one recovery worker instance SHALL process it at a time.
func TestProperty_RecoveryWorkerCoordination(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockStore()
		locker := newMockLocker()
		eventBus := event.NewMemoryEventBus()

		// Generate random number of stuck transactions
		numTx := rapid.IntRange(1, 5).Draw(t, "numTx")

		// Add stuck transactions
		stuckTime := time.Now().Add(-10 * time.Minute)
		for i := 0; i < numTx; i++ {
			store.AddTransaction(&StoreTx{
				TxID:       fmt.Sprintf("tx-stuck-%d", i),
				TxType:     "test",
				Status:     "EXECUTING",
				RetryCount: 0,
				MaxRetries: 3,
				UpdatedAt:  stuckTime,
			})
		}

		// Track which transactions were processed and by which worker
		var processedMu sync.Mutex
		processedBy := make(map[string][]int) // txID -> list of worker IDs that processed it

		// Generate random number of concurrent workers
		numWorkers := rapid.IntRange(2, 5).Draw(t, "numWorkers")

		// Create workers with coordinators that track processing
		workers := make([]*Worker, numWorkers)
		for i := 0; i < numWorkers; i++ {
			workerID := i
			coordinator := &trackingCoordinator{
				workerID:    workerID,
				processedBy: processedBy,
				mu:          &processedMu,
				store:       store,
			}

			workers[i] = NewWorker(
				WithStore(store),
				WithLocker(locker),
				WithCoordinator(coordinator),
				WithEventBus(eventBus),
				WithConfig(Config{
					RecoveryInterval: 100 * time.Millisecond,
					StuckThreshold:   5 * time.Minute,
					MaxRetries:       3,
					LockTTL:          30 * time.Second,
				}),
				WithLogger(&silentLogger{}),
			)
		}

		// Run all workers concurrently
		var wg sync.WaitGroup
		for _, w := range workers {
			wg.Add(1)
			go func(worker *Worker) {
				defer wg.Done()
				worker.ScanOnce(context.Background())
			}(w)
		}
		wg.Wait()

		// Property: Each transaction should be processed by exactly one worker
		processedMu.Lock()
		defer processedMu.Unlock()

		for txID, workerIDs := range processedBy {
			if len(workerIDs) != 1 {
				t.Fatalf("transaction %s was processed by %d workers: %v (expected exactly 1)",
					txID, len(workerIDs), workerIDs)
			}
		}

		// Property: All stuck transactions should be processed
		if len(processedBy) != numTx {
			t.Fatalf("expected %d transactions to be processed, got %d", numTx, len(processedBy))
		}
	})
}

// trackingCoordinator tracks which worker processed each transaction
type trackingCoordinator struct {
	workerID    int
	processedBy map[string][]int
	mu          *sync.Mutex
	store       *mockStore
}

func (c *trackingCoordinator) Resume(ctx context.Context, txID string) error {
	// Simulate some processing time
	time.Sleep(10 * time.Millisecond)

	c.mu.Lock()
	c.processedBy[txID] = append(c.processedBy[txID], c.workerID)
	c.mu.Unlock()

	// Update transaction status to COMPLETED so other workers won't process it
	if c.store != nil {
		c.store.mu.Lock()
		if tx, exists := c.store.transactions[txID]; exists {
			tx.Status = "COMPLETED"
		}
		c.store.mu.Unlock()
	}

	return nil
}

// TestProperty_RecoveryWorkerCoordination_WithContention tests coordination under high contention
func TestProperty_RecoveryWorkerCoordination_WithContention(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockStore()
		locker := newMockLocker()
		eventBus := event.NewMemoryEventBus()

		// Single stuck transaction with multiple workers competing
		stuckTime := time.Now().Add(-10 * time.Minute)
		store.AddTransaction(&StoreTx{
			TxID:       "tx-contended",
			TxType:     "test",
			Status:     "EXECUTING",
			RetryCount: 0,
			MaxRetries: 3,
			UpdatedAt:  stuckTime,
		})

		// Track processing
		var processCount int32

		// Generate random number of concurrent workers (more workers = more contention)
		numWorkers := rapid.IntRange(3, 10).Draw(t, "numWorkers")

		// Create workers
		workers := make([]*Worker, numWorkers)
		for i := 0; i < numWorkers; i++ {
			coordinator := &countingCoordinator{
				processCount: &processCount,
				store:        store,
			}

			workers[i] = NewWorker(
				WithStore(store),
				WithLocker(locker),
				WithCoordinator(coordinator),
				WithEventBus(eventBus),
				WithConfig(Config{
					RecoveryInterval: 100 * time.Millisecond,
					StuckThreshold:   5 * time.Minute,
					MaxRetries:       3,
					LockTTL:          30 * time.Second,
				}),
				WithLogger(&silentLogger{}),
			)
		}

		// Run all workers concurrently
		var wg sync.WaitGroup
		for _, w := range workers {
			wg.Add(1)
			go func(worker *Worker) {
				defer wg.Done()
				worker.ScanOnce(context.Background())
			}(w)
		}
		wg.Wait()

		// Property: Transaction should be processed exactly once
		finalCount := atomic.LoadInt32(&processCount)
		if finalCount != 1 {
			t.Fatalf("expected transaction to be processed exactly once, got %d times", finalCount)
		}
	})
}

// countingCoordinator counts how many times Resume is called
type countingCoordinator struct {
	processCount *int32
	store        *mockStore
}

func (c *countingCoordinator) Resume(ctx context.Context, txID string) error {
	// Simulate some processing time
	time.Sleep(50 * time.Millisecond)
	atomic.AddInt32(c.processCount, 1)

	// Update transaction status to COMPLETED so other workers won't process it
	if c.store != nil {
		c.store.mu.Lock()
		if tx, exists := c.store.transactions[txID]; exists {
			tx.Status = "COMPLETED"
		}
		c.store.mu.Unlock()
	}

	return nil
}
