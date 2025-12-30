// Package store provides the storage interface and models for the RTE engine.
package store

import (
	"context"
	"time"

	"rte"
)

// Store defines the storage interface for transactions and steps.
// It supports CRUD operations, recovery queries, and admin queries.
type Store interface {
	// Transaction operations

	// CreateTransaction creates a new transaction record.
	CreateTransaction(ctx context.Context, tx *Transaction) error

	// UpdateTransaction updates an existing transaction.
	// It uses optimistic locking via the version field.
	UpdateTransaction(ctx context.Context, tx *Transaction) error

	// GetTransaction retrieves a transaction by its ID.
	GetTransaction(ctx context.Context, txID string) (*Transaction, error)

	// Step operations

	// CreateStep creates a new step record.
	CreateStep(ctx context.Context, step *StepRecord) error

	// UpdateStep updates an existing step record.
	UpdateStep(ctx context.Context, step *StepRecord) error

	// GetStep retrieves a specific step by transaction ID and step index.
	GetStep(ctx context.Context, txID string, stepIndex int) (*StepRecord, error)

	// GetSteps retrieves all steps for a transaction.
	GetSteps(ctx context.Context, txID string) ([]*StepRecord, error)

	// Recovery queries

	// GetPendingTransactions retrieves transactions that are pending
	// (CREATED status) and older than the specified duration.
	GetPendingTransactions(ctx context.Context, olderThan time.Duration) ([]*Transaction, error)

	// GetStuckTransactions retrieves transactions that are stuck
	// (LOCKED or EXECUTING status) and older than the specified duration.
	GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*Transaction, error)

	// GetRetryableTransactions retrieves failed transactions that can be retried
	// (retry_count < maxRetries).
	GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*Transaction, error)

	// Admin queries

	// ListTransactions lists transactions with optional filters.
	ListTransactions(ctx context.Context, filter *TxFilter) ([]*Transaction, int64, error)

	// Idempotency operations (optional, can use separate Checker)

	// CheckIdempotency checks if an operation was already executed.
	// Returns exists=true if the operation was executed, along with its result.
	CheckIdempotency(ctx context.Context, key string) (exists bool, result []byte, err error)

	// MarkIdempotency marks an operation as executed with its result.
	MarkIdempotency(ctx context.Context, key string, result []byte, ttl time.Duration) error

	// DeleteExpiredIdempotency removes expired idempotency records.
	DeleteExpiredIdempotency(ctx context.Context) (int64, error)
}

// TxFilter defines filters for listing transactions.
type TxFilter struct {
	// Status filters by transaction status (multiple allowed).
	Status []rte.TxStatus

	// TxType filters by transaction type.
	TxType string

	// StartTime filters transactions created after this time.
	StartTime time.Time

	// EndTime filters transactions created before this time.
	EndTime time.Time

	// Limit specifies the maximum number of results to return.
	Limit int

	// Offset specifies the number of results to skip.
	Offset int
}

// NewTxFilter creates a new TxFilter with default values.
func NewTxFilter() *TxFilter {
	return &TxFilter{
		Limit:  100,
		Offset: 0,
	}
}

// WithStatus adds status filters.
func (f *TxFilter) WithStatus(status ...rte.TxStatus) *TxFilter {
	f.Status = append(f.Status, status...)
	return f
}

// WithTxType sets the transaction type filter.
func (f *TxFilter) WithTxType(txType string) *TxFilter {
	f.TxType = txType
	return f
}

// WithTimeRange sets the time range filter.
func (f *TxFilter) WithTimeRange(start, end time.Time) *TxFilter {
	f.StartTime = start
	f.EndTime = end
	return f
}

// WithPagination sets pagination parameters.
func (f *TxFilter) WithPagination(limit, offset int) *TxFilter {
	f.Limit = limit
	f.Offset = offset
	return f
}
