// Package testinfra provides test infrastructure for RTE production validation.
package testinfra

import (
	"context"
	"time"

	"rte"
	"rte/store"
	"rte/store/mysql"
)

// StoreAdapter adapts store.Store to rte.TxStore interface.
// This allows using mysql.MySQLStore with rte.Engine.
type StoreAdapter struct {
	store *mysql.MySQLStore
}

// NewStoreAdapter creates a new StoreAdapter wrapping a MySQLStore.
func NewStoreAdapter(s *mysql.MySQLStore) *StoreAdapter {
	return &StoreAdapter{store: s}
}

// CreateTransaction creates a new transaction record.
func (a *StoreAdapter) CreateTransaction(ctx context.Context, tx *rte.StoreTx) error {
	storeTx := toStoreTransaction(tx)
	err := a.store.CreateTransaction(ctx, storeTx)
	if err != nil {
		return err
	}
	tx.ID = storeTx.ID
	return nil
}

// UpdateTransaction updates an existing transaction.
func (a *StoreAdapter) UpdateTransaction(ctx context.Context, tx *rte.StoreTx) error {
	storeTx := toStoreTransaction(tx)
	err := a.store.UpdateTransaction(ctx, storeTx)
	if err != nil {
		return err
	}
	tx.Version = storeTx.Version
	tx.UpdatedAt = storeTx.UpdatedAt
	return nil
}

// GetTransaction retrieves a transaction by its ID.
func (a *StoreAdapter) GetTransaction(ctx context.Context, txID string) (*rte.StoreTx, error) {
	storeTx, err := a.store.GetTransaction(ctx, txID)
	if err != nil {
		return nil, err
	}
	return toStoreTx(storeTx), nil
}

// CreateStep creates a new step record.
func (a *StoreAdapter) CreateStep(ctx context.Context, step *rte.StoreStepRecord) error {
	storeStep := toStoreStepRecord(step)
	err := a.store.CreateStep(ctx, storeStep)
	if err != nil {
		return err
	}
	step.ID = storeStep.ID
	return nil
}

// UpdateStep updates an existing step record.
func (a *StoreAdapter) UpdateStep(ctx context.Context, step *rte.StoreStepRecord) error {
	storeStep := toStoreStepRecord(step)
	err := a.store.UpdateStep(ctx, storeStep)
	if err != nil {
		return err
	}
	step.UpdatedAt = storeStep.UpdatedAt
	return nil
}

// GetStep retrieves a specific step by transaction ID and step index.
func (a *StoreAdapter) GetStep(ctx context.Context, txID string, stepIndex int) (*rte.StoreStepRecord, error) {
	storeStep, err := a.store.GetStep(ctx, txID, stepIndex)
	if err != nil {
		return nil, err
	}
	return toStoreStepRecordFromStore(storeStep), nil
}

// GetSteps retrieves all steps for a transaction.
func (a *StoreAdapter) GetSteps(ctx context.Context, txID string) ([]*rte.StoreStepRecord, error) {
	storeSteps, err := a.store.GetSteps(ctx, txID)
	if err != nil {
		return nil, err
	}
	result := make([]*rte.StoreStepRecord, len(storeSteps))
	for i, s := range storeSteps {
		result[i] = toStoreStepRecordFromStore(s)
	}
	return result, nil
}

// GetPendingTransactions retrieves pending transactions older than the specified duration.
func (a *StoreAdapter) GetPendingTransactions(ctx context.Context, olderThan time.Duration) ([]*rte.StoreTx, error) {
	storeTxs, err := a.store.GetPendingTransactions(ctx, olderThan)
	if err != nil {
		return nil, err
	}
	return toStoreTxSlice(storeTxs), nil
}

// GetStuckTransactions retrieves stuck transactions older than the specified duration.
func (a *StoreAdapter) GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*rte.StoreTx, error) {
	storeTxs, err := a.store.GetStuckTransactions(ctx, olderThan)
	if err != nil {
		return nil, err
	}
	return toStoreTxSlice(storeTxs), nil
}

// GetRetryableTransactions retrieves failed transactions that can be retried.
func (a *StoreAdapter) GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*rte.StoreTx, error) {
	storeTxs, err := a.store.GetRetryableTransactions(ctx, maxRetries)
	if err != nil {
		return nil, err
	}
	return toStoreTxSlice(storeTxs), nil
}

// ListTransactions lists transactions with optional filters.
func (a *StoreAdapter) ListTransactions(ctx context.Context, filter *rte.StoreTxFilter) ([]*rte.StoreTx, int64, error) {
	storeFilter := &store.TxFilter{
		Status:    filter.Status,
		TxType:    filter.TxType,
		StartTime: filter.StartTime,
		EndTime:   filter.EndTime,
		Limit:     filter.Limit,
		Offset:    filter.Offset,
	}
	storeTxs, total, err := a.store.ListTransactions(ctx, storeFilter)
	if err != nil {
		return nil, 0, err
	}
	return toStoreTxSlice(storeTxs), total, nil
}

// CheckIdempotency checks if an operation was already executed.
func (a *StoreAdapter) CheckIdempotency(ctx context.Context, key string) (bool, []byte, error) {
	return a.store.CheckIdempotency(ctx, key)
}

// MarkIdempotency marks an operation as executed with its result.
func (a *StoreAdapter) MarkIdempotency(ctx context.Context, key string, result []byte, ttl time.Duration) error {
	return a.store.MarkIdempotency(ctx, key, result, ttl)
}

// DeleteExpiredIdempotency removes expired idempotency records.
func (a *StoreAdapter) DeleteExpiredIdempotency(ctx context.Context) (int64, error) {
	return a.store.DeleteExpiredIdempotency(ctx)
}

// ============================================================================
// Conversion helpers
// ============================================================================

// toStoreTransaction converts rte.StoreTx to store.Transaction.
func toStoreTransaction(tx *rte.StoreTx) *store.Transaction {
	var ctx *store.ContextJSON
	if tx.Context != nil {
		ctx = &store.ContextJSON{
			TxID:      tx.Context.TxID,
			TxType:    tx.Context.TxType,
			StepIndex: tx.Context.StepIndex,
			Input:     tx.Context.Input,
			Output:    tx.Context.Output,
			Metadata:  tx.Context.Metadata,
		}
	}
	return &store.Transaction{
		ID:          tx.ID,
		TxID:        tx.TxID,
		TxType:      tx.TxType,
		Status:      tx.Status,
		CurrentStep: tx.CurrentStep,
		TotalSteps:  tx.TotalSteps,
		StepNames:   tx.StepNames,
		LockKeys:    tx.LockKeys,
		Context:     ctx,
		ErrorMsg:    tx.ErrorMsg,
		RetryCount:  tx.RetryCount,
		MaxRetries:  tx.MaxRetries,
		Version:     tx.Version,
		CreatedAt:   tx.CreatedAt,
		UpdatedAt:   tx.UpdatedAt,
		LockedAt:    tx.LockedAt,
		CompletedAt: tx.CompletedAt,
		TimeoutAt:   tx.TimeoutAt,
	}
}

// toStoreTx converts store.Transaction to rte.StoreTx.
func toStoreTx(tx *store.Transaction) *rte.StoreTx {
	var ctx *rte.StoreTxContext
	if tx.Context != nil {
		ctx = &rte.StoreTxContext{
			TxID:      tx.Context.TxID,
			TxType:    tx.Context.TxType,
			StepIndex: tx.Context.StepIndex,
			Input:     tx.Context.Input,
			Output:    tx.Context.Output,
			Metadata:  tx.Context.Metadata,
		}
	}
	return &rte.StoreTx{
		ID:          tx.ID,
		TxID:        tx.TxID,
		TxType:      tx.TxType,
		Status:      tx.Status,
		CurrentStep: tx.CurrentStep,
		TotalSteps:  tx.TotalSteps,
		StepNames:   tx.StepNames,
		LockKeys:    tx.LockKeys,
		Context:     ctx,
		ErrorMsg:    tx.ErrorMsg,
		RetryCount:  tx.RetryCount,
		MaxRetries:  tx.MaxRetries,
		Version:     tx.Version,
		CreatedAt:   tx.CreatedAt,
		UpdatedAt:   tx.UpdatedAt,
		LockedAt:    tx.LockedAt,
		CompletedAt: tx.CompletedAt,
		TimeoutAt:   tx.TimeoutAt,
	}
}

// toStoreTxSlice converts a slice of store.Transaction to rte.StoreTx.
func toStoreTxSlice(txs []*store.Transaction) []*rte.StoreTx {
	result := make([]*rte.StoreTx, len(txs))
	for i, tx := range txs {
		result[i] = toStoreTx(tx)
	}
	return result
}

// toStoreStepRecord converts rte.StoreStepRecord to store.StepRecord.
func toStoreStepRecord(step *rte.StoreStepRecord) *store.StepRecord {
	return &store.StepRecord{
		ID:             step.ID,
		TxID:           step.TxID,
		StepIndex:      step.StepIndex,
		StepName:       step.StepName,
		Status:         step.Status,
		IdempotencyKey: step.IdempotencyKey,
		Input:          step.Input,
		Output:         step.Output,
		ErrorMsg:       step.ErrorMsg,
		RetryCount:     step.RetryCount,
		StartedAt:      step.StartedAt,
		CompletedAt:    step.CompletedAt,
		CreatedAt:      step.CreatedAt,
		UpdatedAt:      step.UpdatedAt,
	}
}

// toStoreStepRecordFromStore converts store.StepRecord to rte.StoreStepRecord.
func toStoreStepRecordFromStore(step *store.StepRecord) *rte.StoreStepRecord {
	return &rte.StoreStepRecord{
		ID:             step.ID,
		TxID:           step.TxID,
		StepIndex:      step.StepIndex,
		StepName:       step.StepName,
		Status:         step.Status,
		IdempotencyKey: step.IdempotencyKey,
		Input:          step.Input,
		Output:         step.Output,
		ErrorMsg:       step.ErrorMsg,
		RetryCount:     step.RetryCount,
		StartedAt:      step.StartedAt,
		CompletedAt:    step.CompletedAt,
		CreatedAt:      step.CreatedAt,
		UpdatedAt:      step.UpdatedAt,
	}
}

// Ensure StoreAdapter implements rte.TxStore interface.
var _ rte.TxStore = (*StoreAdapter)(nil)
