// Package admin provides administrative interfaces for the RTE engine.
// It allows operators to query, monitor, and manually intervene in transactions.
package admin

import (
	"context"
	"fmt"
	"time"

	"rte"
	"rte/circuit"
	"rte/event"
	"rte/lock"
)

// TxListResult represents the result of listing transactions.
type TxListResult struct {
	// Transactions is the list of transactions.
	Transactions []*rte.StoreTx
	// Total is the total number of transactions matching the filter.
	Total int64
	// Limit is the maximum number of results returned.
	Limit int
	// Offset is the number of results skipped.
	Offset int
}

// TxDetail represents detailed transaction information.
type TxDetail struct {
	// Transaction is the transaction record.
	Transaction *rte.StoreTx
	// Steps is the list of step records.
	Steps []*rte.StoreStepRecord
}

// EngineStats represents engine statistics.
type EngineStats struct {
	// TotalTransactions is the total number of transactions.
	TotalTransactions int64
	// PendingTransactions is the number of pending transactions.
	PendingTransactions int64
	// FailedTransactions is the number of failed transactions.
	FailedTransactions int64
	// CompletedTransactions is the number of completed transactions.
	CompletedTransactions int64
	// CompensatedTransactions is the number of compensated transactions.
	CompensatedTransactions int64
	// CircuitBreakerStats contains circuit breaker statistics by service.
	CircuitBreakerStats map[string]circuit.BreakerCounts
}

// Admin provides administrative operations for the RTE engine.
type Admin interface {
	// ListTransactions lists transactions with optional filters.
	ListTransactions(ctx context.Context, filter *rte.StoreTxFilter) (*TxListResult, error)

	// GetTransaction retrieves detailed transaction information.
	GetTransaction(ctx context.Context, txID string) (*TxDetail, error)

	// ForceComplete forces a stuck transaction to complete.
	ForceComplete(ctx context.Context, txID string, reason string) error

	// ForceCancel forces a transaction to cancel.
	ForceCancel(ctx context.Context, txID string, reason string) error

	// RetryTransaction manually retries a failed transaction.
	RetryTransaction(ctx context.Context, txID string) error

	// GetStats retrieves engine statistics.
	GetStats(ctx context.Context) (*EngineStats, error)
}

// AdminImpl implements the Admin interface.
type AdminImpl struct {
	store       rte.TxStore
	locker      lock.Locker
	breaker     circuit.Breaker
	events      event.EventBus
	coordinator *rte.Coordinator
	config      rte.Config
}

// AdminOption is a function that configures the Admin.
type AdminOption func(*AdminImpl)

// WithAdminStore sets the store for the admin.
func WithAdminStore(s rte.TxStore) AdminOption {
	return func(a *AdminImpl) {
		a.store = s
	}
}

// WithAdminLocker sets the locker for the admin.
func WithAdminLocker(l lock.Locker) AdminOption {
	return func(a *AdminImpl) {
		a.locker = l
	}
}

// WithAdminBreaker sets the circuit breaker for the admin.
func WithAdminBreaker(b circuit.Breaker) AdminOption {
	return func(a *AdminImpl) {
		a.breaker = b
	}
}

// WithAdminEventBus sets the event bus for the admin.
func WithAdminEventBus(e event.EventBus) AdminOption {
	return func(a *AdminImpl) {
		a.events = e
	}
}

// WithAdminCoordinator sets the coordinator for the admin.
func WithAdminCoordinator(c *rte.Coordinator) AdminOption {
	return func(a *AdminImpl) {
		a.coordinator = c
	}
}

// WithAdminConfig sets the configuration for the admin.
func WithAdminConfig(cfg rte.Config) AdminOption {
	return func(a *AdminImpl) {
		a.config = cfg
	}
}

// NewAdmin creates a new Admin with the given options.
func NewAdmin(opts ...AdminOption) *AdminImpl {
	a := &AdminImpl{
		config: rte.DefaultConfig(),
	}

	for _, opt := range opts {
		opt(a)
	}

	return a
}

// ListTransactions lists transactions with optional filters.
func (a *AdminImpl) ListTransactions(ctx context.Context, filter *rte.StoreTxFilter) (*TxListResult, error) {
	if a.store == nil {
		return nil, fmt.Errorf("store not configured")
	}

	// Apply default pagination if not set
	if filter == nil {
		filter = &rte.StoreTxFilter{
			Limit:  100,
			Offset: 0,
		}
	}
	if filter.Limit <= 0 {
		filter.Limit = 100
	}

	transactions, total, err := a.store.ListTransactions(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("failed to list transactions: %w", err)
	}

	return &TxListResult{
		Transactions: transactions,
		Total:        total,
		Limit:        filter.Limit,
		Offset:       filter.Offset,
	}, nil
}

// GetTransaction retrieves detailed transaction information including all steps.
func (a *AdminImpl) GetTransaction(ctx context.Context, txID string) (*TxDetail, error) {
	if a.store == nil {
		return nil, fmt.Errorf("store not configured")
	}

	// Get transaction
	tx, err := a.store.GetTransaction(ctx, txID)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}
	if tx == nil {
		return nil, rte.ErrTransactionNotFound
	}

	// Get steps
	steps, err := a.store.GetSteps(ctx, txID)
	if err != nil {
		return nil, fmt.Errorf("failed to get steps: %w", err)
	}

	return &TxDetail{
		Transaction: tx,
		Steps:       steps,
	}, nil
}

// ForceComplete forces a stuck transaction to complete.
func (a *AdminImpl) ForceComplete(ctx context.Context, txID string, reason string) error {
	if a.store == nil {
		return fmt.Errorf("store not configured")
	}

	// Get transaction
	tx, err := a.store.GetTransaction(ctx, txID)
	if err != nil {
		return fmt.Errorf("failed to get transaction: %w", err)
	}
	if tx == nil {
		return rte.ErrTransactionNotFound
	}

	// Check if transaction can be force completed
	// Only allow force complete for stuck states (LOCKED, EXECUTING, CONFIRMING)
	if !canForceComplete(tx.Status) {
		return fmt.Errorf("%w: cannot force complete transaction in status %s", rte.ErrInvalidTransactionState, tx.Status)
	}

	// Update transaction status to COMPLETED
	tx.Status = rte.TxStatusCompleted
	now := time.Now()
	tx.CompletedAt = &now
	tx.ErrorMsg = fmt.Sprintf("force completed: %s", reason)
	tx.IncrementVersion()

	if err := a.store.UpdateTransaction(ctx, tx); err != nil {
		return fmt.Errorf("failed to update transaction: %w", err)
	}

	// Publish event
	if a.events != nil {
		a.events.Publish(ctx, event.NewEvent(event.EventTxCompleted).
			WithTxID(txID).
			WithTxType(tx.TxType).
			WithData("force_completed", true).
			WithData("reason", reason))
	}

	return nil
}

// canForceComplete checks if a transaction can be force completed.
func canForceComplete(status rte.TxStatus) bool {
	switch status {
	case rte.TxStatusLocked, rte.TxStatusExecuting, rte.TxStatusConfirming:
		return true
	default:
		return false
	}
}

// ForceCancel forces a transaction to cancel.
func (a *AdminImpl) ForceCancel(ctx context.Context, txID string, reason string) error {
	if a.store == nil {
		return fmt.Errorf("store not configured")
	}

	// Get transaction
	tx, err := a.store.GetTransaction(ctx, txID)
	if err != nil {
		return fmt.Errorf("failed to get transaction: %w", err)
	}
	if tx == nil {
		return rte.ErrTransactionNotFound
	}

	// Check if transaction can be cancelled
	if !canForceCancel(tx.Status) {
		return fmt.Errorf("%w: cannot cancel transaction in status %s", rte.ErrInvalidTransactionState, tx.Status)
	}

	// Check if compensation is needed (if any steps were completed)
	needsCompensation := false
	if tx.CurrentStep > 0 && a.coordinator != nil {
		steps, _ := a.store.GetSteps(ctx, txID)
		for _, step := range steps {
			if step.Status == rte.StepStatusCompleted {
				stepImpl := a.coordinator.GetStep(step.StepName)
				if stepImpl != nil && stepImpl.SupportsCompensation() {
					needsCompensation = true
					break
				}
			}
		}
	}

	if needsCompensation && a.coordinator != nil {
		// Trigger compensation
		tx.Status = rte.TxStatusCompensating
		tx.ErrorMsg = fmt.Sprintf("force cancelled: %s", reason)
		tx.IncrementVersion()

		if err := a.store.UpdateTransaction(ctx, tx); err != nil {
			return fmt.Errorf("failed to update transaction: %w", err)
		}

		// Resume will handle compensation
		txCtx := tx.Context.ToTxContext()
		_, compErr := a.compensateTransaction(ctx, tx, txCtx)
		if compErr != nil {
			return fmt.Errorf("compensation failed: %w", compErr)
		}
	} else {
		// No compensation needed, just cancel
		tx.Status = rte.TxStatusCancelled
		tx.ErrorMsg = fmt.Sprintf("force cancelled: %s", reason)
		tx.IncrementVersion()

		if err := a.store.UpdateTransaction(ctx, tx); err != nil {
			return fmt.Errorf("failed to update transaction: %w", err)
		}
	}

	// Publish event
	if a.events != nil {
		a.events.Publish(ctx, event.NewEvent(event.EventTxCancelled).
			WithTxID(txID).
			WithTxType(tx.TxType).
			WithData("force_cancelled", true).
			WithData("reason", reason))
	}

	return nil
}

// canForceCancel checks if a transaction can be force cancelled.
func canForceCancel(status rte.TxStatus) bool {
	switch status {
	case rte.TxStatusCreated, rte.TxStatusLocked, rte.TxStatusExecuting, rte.TxStatusFailed:
		return true
	default:
		return false
	}
}

// compensateTransaction performs compensation for a transaction.
func (a *AdminImpl) compensateTransaction(ctx context.Context, tx *rte.StoreTx, txCtx *rte.TxContext) (*rte.StoreTx, error) {
	// Get all steps
	steps, err := a.store.GetSteps(ctx, tx.TxID)
	if err != nil {
		return tx, fmt.Errorf("failed to get steps: %w", err)
	}

	// Compensate from current step backwards
	for i := len(steps) - 1; i >= 0; i-- {
		step := steps[i]

		// Skip steps that were not completed
		if step.Status != rte.StepStatusCompleted {
			continue
		}

		stepImpl := a.coordinator.GetStep(step.StepName)
		if stepImpl == nil {
			continue
		}

		// Check if step supports compensation
		if !stepImpl.SupportsCompensation() {
			continue
		}

		// Update step status to COMPENSATING
		step.Status = rte.StepStatusCompensating
		a.store.UpdateStep(ctx, step)

		// Execute compensation
		txCtx.StepIndex = step.StepIndex
		compErr := stepImpl.Compensate(ctx, txCtx)
		if compErr != nil {
			// Compensation failed
			step.Status = rte.StepStatusFailed
			step.ErrorMsg = compErr.Error()
			a.store.UpdateStep(ctx, step)

			tx.Status = rte.TxStatusCompensationFailed
			tx.ErrorMsg = fmt.Sprintf("compensation failed at step %d (%s): %v", i, step.StepName, compErr)
			tx.IncrementVersion()
			a.store.UpdateTransaction(ctx, tx)

			if a.events != nil {
				a.events.Publish(ctx, event.NewEvent(event.EventTxCompensationFailed).
					WithTxID(tx.TxID).
					WithTxType(tx.TxType).
					WithError(compErr))
			}

			return tx, compErr
		}

		// Update step status to COMPENSATED
		step.Status = rte.StepStatusCompensated
		a.store.UpdateStep(ctx, step)
	}

	// Update transaction status to COMPENSATED
	tx.Status = rte.TxStatusCompensated
	tx.IncrementVersion()
	if err := a.store.UpdateTransaction(ctx, tx); err != nil {
		return tx, err
	}

	return tx, nil
}

// RetryTransaction manually retries a failed transaction.
func (a *AdminImpl) RetryTransaction(ctx context.Context, txID string) error {
	if a.store == nil {
		return fmt.Errorf("store not configured")
	}
	if a.coordinator == nil {
		return fmt.Errorf("coordinator not configured")
	}

	// Get transaction
	tx, err := a.store.GetTransaction(ctx, txID)
	if err != nil {
		return fmt.Errorf("failed to get transaction: %w", err)
	}
	if tx == nil {
		return rte.ErrTransactionNotFound
	}

	// Check if transaction can be retried
	if tx.Status != rte.TxStatusFailed {
		return fmt.Errorf("%w: can only retry failed transactions, current status: %s", rte.ErrInvalidTransactionState, tx.Status)
	}

	// Increment retry count
	tx.RetryCount++
	tx.ErrorMsg = ""
	tx.IncrementVersion()

	if err := a.store.UpdateTransaction(ctx, tx); err != nil {
		return fmt.Errorf("failed to update transaction: %w", err)
	}

	// Resume transaction execution
	_, err = a.coordinator.Resume(ctx, tx)
	if err != nil {
		return fmt.Errorf("retry failed: %w", err)
	}

	return nil
}

// GetStats retrieves engine statistics.
func (a *AdminImpl) GetStats(ctx context.Context) (*EngineStats, error) {
	if a.store == nil {
		return nil, fmt.Errorf("store not configured")
	}

	stats := &EngineStats{
		CircuitBreakerStats: make(map[string]circuit.BreakerCounts),
	}

	// Get transaction counts by status
	// Total transactions
	allTxs, total, err := a.store.ListTransactions(ctx, &rte.StoreTxFilter{Limit: 1})
	if err != nil {
		return nil, fmt.Errorf("failed to get total transactions: %w", err)
	}
	_ = allTxs // We only need the total count
	stats.TotalTransactions = total

	// Pending transactions (CREATED, LOCKED, EXECUTING, CONFIRMING)
	pendingStatuses := []rte.TxStatus{
		rte.TxStatusCreated,
		rte.TxStatusLocked,
		rte.TxStatusExecuting,
		rte.TxStatusConfirming,
	}
	_, pendingCount, err := a.store.ListTransactions(ctx, &rte.StoreTxFilter{
		Status: pendingStatuses,
		Limit:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get pending transactions: %w", err)
	}
	stats.PendingTransactions = pendingCount

	// Failed transactions
	failedStatuses := []rte.TxStatus{
		rte.TxStatusFailed,
		rte.TxStatusCompensationFailed,
		rte.TxStatusTimeout,
	}
	_, failedCount, err := a.store.ListTransactions(ctx, &rte.StoreTxFilter{
		Status: failedStatuses,
		Limit:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get failed transactions: %w", err)
	}
	stats.FailedTransactions = failedCount

	// Completed transactions
	_, completedCount, err := a.store.ListTransactions(ctx, &rte.StoreTxFilter{
		Status: []rte.TxStatus{rte.TxStatusCompleted},
		Limit:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get completed transactions: %w", err)
	}
	stats.CompletedTransactions = completedCount

	// Compensated transactions
	_, compensatedCount, err := a.store.ListTransactions(ctx, &rte.StoreTxFilter{
		Status: []rte.TxStatus{rte.TxStatusCompensated},
		Limit:  1,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get compensated transactions: %w", err)
	}
	stats.CompensatedTransactions = compensatedCount

	return stats, nil
}

// NewTxFilter creates a new transaction filter with default values.
func NewTxFilter() *rte.StoreTxFilter {
	return &rte.StoreTxFilter{
		Limit:  100,
		Offset: 0,
	}
}

// FilterWithStatus adds status filters to the filter.
func FilterWithStatus(filter *rte.StoreTxFilter, status ...rte.TxStatus) *rte.StoreTxFilter {
	filter.Status = append(filter.Status, status...)
	return filter
}

// FilterWithTxType sets the transaction type filter.
func FilterWithTxType(filter *rte.StoreTxFilter, txType string) *rte.StoreTxFilter {
	filter.TxType = txType
	return filter
}

// FilterWithTimeRange sets the time range filter.
func FilterWithTimeRange(filter *rte.StoreTxFilter, start, end time.Time) *rte.StoreTxFilter {
	filter.StartTime = start
	filter.EndTime = end
	return filter
}

// FilterWithPagination sets pagination parameters.
func FilterWithPagination(filter *rte.StoreTxFilter, limit, offset int) *rte.StoreTxFilter {
	filter.Limit = limit
	filter.Offset = offset
	return filter
}
