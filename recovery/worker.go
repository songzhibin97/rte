// Package recovery provides the recovery worker for handling stuck and failed transactions.
package recovery

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"rte/event"
	"rte/lock"
)

// StoreTx represents a transaction record for storage.
// This is a simplified version to avoid circular imports.
type StoreTx struct {
	ID          int64
	TxID        string
	TxType      string
	Status      string
	CurrentStep int
	TotalSteps  int
	StepNames   []string
	LockKeys    []string
	ErrorMsg    string
	RetryCount  int
	MaxRetries  int
	Version     int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	LockedAt    *time.Time
	CompletedAt *time.Time
	TimeoutAt   *time.Time
}

// TxStore defines the storage interface needed by the recovery worker.
type TxStore interface {
	GetTransaction(ctx context.Context, txID string) (*StoreTx, error)
	UpdateTransaction(ctx context.Context, tx *StoreTx) error
	GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*StoreTx, error)
	GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*StoreTx, error)
}

// Coordinator defines the interface for resuming transactions.
type Coordinator interface {
	Resume(ctx context.Context, txID string) error
}

// Config holds the configuration for the recovery worker.
type Config struct {
	// RecoveryInterval is the interval between recovery scans.
	RecoveryInterval time.Duration
	// StuckThreshold is the duration after which a transaction is considered stuck.
	StuckThreshold time.Duration
	// MaxRetries is the maximum number of retries for failed transactions.
	MaxRetries int
	// LockTTL is the TTL for recovery locks.
	LockTTL time.Duration
}

// DefaultConfig returns the default configuration for the recovery worker.
func DefaultConfig() Config {
	return Config{
		RecoveryInterval: 30 * time.Second,
		StuckThreshold:   5 * time.Minute,
		MaxRetries:       3,
		LockTTL:          30 * time.Second,
	}
}

// Logger defines the logging interface.
type Logger interface {
	Printf(format string, v ...any)
}

// defaultLogger is the default logger implementation.
type defaultLogger struct{}

func (l *defaultLogger) Printf(format string, v ...any) {
	log.Printf("[RecoveryWorker] "+format, v...)
}

// Worker is the recovery worker that handles stuck and failed transactions.
// It periodically scans for transactions that need recovery and processes them.
type Worker struct {
	store       TxStore
	locker      lock.Locker
	coordinator Coordinator
	events      event.EventBus
	config      Config
	logger      Logger

	// State
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
	mu      sync.Mutex

	// Metrics
	scannedCount   int64
	processedCount int64
	failedCount    int64
	metricsMu      sync.RWMutex
}

// WorkerOption is a function that configures the Worker.
type WorkerOption func(*Worker)

// WithStore sets the store for the worker.
func WithStore(s TxStore) WorkerOption {
	return func(w *Worker) {
		w.store = s
	}
}

// WithLocker sets the locker for the worker.
func WithLocker(l lock.Locker) WorkerOption {
	return func(w *Worker) {
		w.locker = l
	}
}

// WithCoordinator sets the coordinator for the worker.
func WithCoordinator(c Coordinator) WorkerOption {
	return func(w *Worker) {
		w.coordinator = c
	}
}

// WithEventBus sets the event bus for the worker.
func WithEventBus(e event.EventBus) WorkerOption {
	return func(w *Worker) {
		w.events = e
	}
}

// WithConfig sets the configuration for the worker.
func WithConfig(cfg Config) WorkerOption {
	return func(w *Worker) {
		w.config = cfg
	}
}

// WithLogger sets the logger for the worker.
func WithLogger(l Logger) WorkerOption {
	return func(w *Worker) {
		w.logger = l
	}
}

// NewWorker creates a new recovery worker with the given options.
func NewWorker(opts ...WorkerOption) *Worker {
	w := &Worker{
		config: DefaultConfig(),
		logger: &defaultLogger{},
		stopCh: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

// Start starts the recovery worker.
// It runs in the background and periodically scans for transactions that need recovery.
func (w *Worker) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("recovery worker already running")
	}
	w.running = true
	w.stopCh = make(chan struct{})
	w.mu.Unlock()

	w.wg.Add(1)
	go w.run(ctx)

	w.logger.Printf("started with interval=%v, stuckThreshold=%v", w.config.RecoveryInterval, w.config.StuckThreshold)
	return nil
}

// Stop stops the recovery worker gracefully.
func (w *Worker) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	w.running = false
	close(w.stopCh)
	w.mu.Unlock()

	w.wg.Wait()
	w.logger.Printf("stopped")
}

// IsRunning returns true if the worker is running.
func (w *Worker) IsRunning() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.running
}

// run is the main loop of the recovery worker.
func (w *Worker) run(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(w.config.RecoveryInterval)
	defer ticker.Stop()

	// Run initial scan immediately
	w.scan(ctx)

	for {
		select {
		case <-ticker.C:
			w.scan(ctx)
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// scan performs a single recovery scan.
func (w *Worker) scan(ctx context.Context) {
	// Publish recovery start event
	w.publishEvent(ctx, event.NewEvent(event.EventRecoveryStart))

	// 1. Process stuck transactions (LOCKED/EXECUTING beyond threshold)
	stuck, err := w.store.GetStuckTransactions(ctx, w.config.StuckThreshold)
	if err != nil {
		w.logger.Printf("failed to get stuck transactions: %v", err)
	} else {
		w.incrementScanned(int64(len(stuck)))
		for _, tx := range stuck {
			w.recoverTransaction(ctx, tx)
		}
	}

	// 2. Process retryable failed transactions
	retryable, err := w.store.GetRetryableTransactions(ctx, w.config.MaxRetries)
	if err != nil {
		w.logger.Printf("failed to get retryable transactions: %v", err)
	} else {
		w.incrementScanned(int64(len(retryable)))
		for _, tx := range retryable {
			w.retryTransaction(ctx, tx)
		}
	}
}

// recoverTransaction attempts to recover a stuck transaction.
func (w *Worker) recoverTransaction(ctx context.Context, tx *StoreTx) {
	// Acquire distributed lock to prevent concurrent recovery
	lockKey := fmt.Sprintf("recovery:%s", tx.TxID)
	handle, err := w.locker.Acquire(ctx, []string{lockKey}, w.config.LockTTL)
	if err != nil {
		// Another instance is processing this transaction
		w.logger.Printf("skipping tx %s: lock acquisition failed (likely being processed by another instance)", tx.TxID)
		return
	}
	defer handle.Release(ctx)

	// Reload transaction state to ensure it still needs recovery
	currentTx, err := w.store.GetTransaction(ctx, tx.TxID)
	if err != nil {
		w.logger.Printf("failed to reload tx %s: %v", tx.TxID, err)
		return
	}

	// Check if transaction still needs recovery
	// Only recover LOCKED or EXECUTING transactions
	if currentTx.Status != "LOCKED" && currentTx.Status != "EXECUTING" {
		w.logger.Printf("tx %s no longer needs recovery (status=%s)", tx.TxID, currentTx.Status)
		return
	}

	w.logger.Printf("recovering stuck tx %s (status=%s, stuck since %v)", tx.TxID, currentTx.Status, currentTx.UpdatedAt)

	// Resume the transaction
	err = w.coordinator.Resume(ctx, tx.TxID)
	if err != nil {
		w.logger.Printf("failed to recover tx %s: %v", tx.TxID, err)
		w.incrementFailed()
		w.publishEvent(ctx, event.NewEvent(event.EventAlertWarning).
			WithTxID(tx.TxID).
			WithTxType(tx.TxType).
			WithData("message", fmt.Sprintf("recovery failed: %v", err)).
			WithError(err))
		return
	}

	w.incrementProcessed()
	w.logger.Printf("successfully recovered tx %s", tx.TxID)
}

// retryTransaction attempts to retry a failed transaction.
func (w *Worker) retryTransaction(ctx context.Context, tx *StoreTx) {
	// Acquire distributed lock to prevent concurrent retry
	lockKey := fmt.Sprintf("recovery:%s", tx.TxID)
	handle, err := w.locker.Acquire(ctx, []string{lockKey}, w.config.LockTTL)
	if err != nil {
		// Another instance is processing this transaction
		return
	}
	defer handle.Release(ctx)

	// Reload transaction state
	currentTx, err := w.store.GetTransaction(ctx, tx.TxID)
	if err != nil {
		w.logger.Printf("failed to reload tx %s: %v", tx.TxID, err)
		return
	}

	// Check if transaction still needs retry
	if currentTx.Status != "FAILED" {
		return
	}

	// Check retry count
	if currentTx.RetryCount >= currentTx.MaxRetries {
		// Max retries exceeded - publish critical alert
		w.logger.Printf("tx %s exceeded max retries (%d/%d)", tx.TxID, currentTx.RetryCount, currentTx.MaxRetries)
		w.publishEvent(ctx, event.NewEvent(event.EventAlertCritical).
			WithTxID(tx.TxID).
			WithTxType(tx.TxType).
			WithData("message", fmt.Sprintf("transaction %s exceeded max retries", tx.TxID)).
			WithData("retry_count", currentTx.RetryCount).
			WithData("max_retries", currentTx.MaxRetries))
		return
	}

	w.logger.Printf("retrying failed tx %s (attempt %d/%d)", tx.TxID, currentTx.RetryCount+1, currentTx.MaxRetries)

	// Resume the transaction (coordinator will handle retry logic)
	err = w.coordinator.Resume(ctx, tx.TxID)
	if err != nil {
		w.logger.Printf("failed to retry tx %s: %v", tx.TxID, err)
		w.incrementFailed()
		return
	}

	w.incrementProcessed()
	w.logger.Printf("successfully retried tx %s", tx.TxID)
}

// publishEvent publishes an event to the event bus.
func (w *Worker) publishEvent(ctx context.Context, e event.Event) {
	if w.events != nil {
		w.events.Publish(ctx, e)
	}
}

// Metrics methods

func (w *Worker) incrementScanned(count int64) {
	w.metricsMu.Lock()
	defer w.metricsMu.Unlock()
	w.scannedCount += count
}

func (w *Worker) incrementProcessed() {
	w.metricsMu.Lock()
	defer w.metricsMu.Unlock()
	w.processedCount++
}

func (w *Worker) incrementFailed() {
	w.metricsMu.Lock()
	defer w.metricsMu.Unlock()
	w.failedCount++
}

// Stats returns the current statistics of the recovery worker.
type Stats struct {
	ScannedCount   int64
	ProcessedCount int64
	FailedCount    int64
	IsRunning      bool
}

// Stats returns the current statistics of the recovery worker.
func (w *Worker) Stats() Stats {
	w.metricsMu.RLock()
	defer w.metricsMu.RUnlock()
	return Stats{
		ScannedCount:   w.scannedCount,
		ProcessedCount: w.processedCount,
		FailedCount:    w.failedCount,
		IsRunning:      w.IsRunning(),
	}
}

// ResetStats resets the statistics counters.
func (w *Worker) ResetStats() {
	w.metricsMu.Lock()
	defer w.metricsMu.Unlock()
	w.scannedCount = 0
	w.processedCount = 0
	w.failedCount = 0
}

// ScanOnce performs a single recovery scan synchronously.
// This is useful for testing.
func (w *Worker) ScanOnce(ctx context.Context) {
	w.scan(ctx)
}
