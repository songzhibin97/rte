package rte

import (
	"context"
	"time"
)

// TxStore defines the storage interface for transactions and steps.
// This interface is implemented by store/mysql and other storage backends.
type TxStore interface {
	// Transaction operations
	CreateTransaction(ctx context.Context, tx *StoreTx) error
	UpdateTransaction(ctx context.Context, tx *StoreTx) error
	GetTransaction(ctx context.Context, txID string) (*StoreTx, error)

	// Step operations
	CreateStep(ctx context.Context, step *StoreStepRecord) error
	UpdateStep(ctx context.Context, step *StoreStepRecord) error
	GetStep(ctx context.Context, txID string, stepIndex int) (*StoreStepRecord, error)
	GetSteps(ctx context.Context, txID string) ([]*StoreStepRecord, error)

	// Recovery queries
	GetPendingTransactions(ctx context.Context, olderThan time.Duration) ([]*StoreTx, error)
	GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*StoreTx, error)
	GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*StoreTx, error)

	// Admin queries
	ListTransactions(ctx context.Context, filter *StoreTxFilter) ([]*StoreTx, int64, error)

	// Idempotency operations
	CheckIdempotency(ctx context.Context, key string) (exists bool, result []byte, err error)
	MarkIdempotency(ctx context.Context, key string, result []byte, ttl time.Duration) error
	DeleteExpiredIdempotency(ctx context.Context) (int64, error)
}

// StoreTx represents a transaction record for storage.
type StoreTx struct {
	ID          int64
	TxID        string
	TxType      string
	Status      TxStatus
	CurrentStep int
	TotalSteps  int
	StepNames   []string
	LockKeys    []string
	Context     *StoreTxContext
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

// StoreTxContext represents the transaction context for storage.
type StoreTxContext struct {
	TxID      string
	TxType    string
	StepIndex int
	Input     map[string]any
	Output    map[string]any
	Metadata  map[string]string
}

// ToTxContext converts StoreTxContext to TxContext.
func (c *StoreTxContext) ToTxContext() *TxContext {
	if c == nil {
		return nil
	}
	ctx := NewTxContext(c.TxID, c.TxType)
	ctx.StepIndex = c.StepIndex
	ctx.WithInput(c.Input)
	for k, v := range c.Output {
		ctx.SetOutput(k, v)
	}
	for k, v := range c.Metadata {
		ctx.SetMetadata(k, v)
	}
	return ctx
}

// NewStoreTxContext creates a StoreTxContext from TxContext.
func NewStoreTxContext(ctx *TxContext) *StoreTxContext {
	if ctx == nil {
		return nil
	}
	return &StoreTxContext{
		TxID:      ctx.TxID,
		TxType:    ctx.TxType,
		StepIndex: ctx.StepIndex,
		Input:     ctx.Input,
		Output:    ctx.Output,
		Metadata:  ctx.Metadata,
	}
}

// StoreStepRecord represents a step record for storage.
type StoreStepRecord struct {
	ID             int64
	TxID           string
	StepIndex      int
	StepName       string
	Status         StepStatus
	IdempotencyKey string
	Input          []byte
	Output         []byte
	ErrorMsg       string
	RetryCount     int
	StartedAt      *time.Time
	CompletedAt    *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// StoreTxFilter defines filters for listing transactions.
type StoreTxFilter struct {
	Status    []TxStatus
	TxType    string
	StartTime time.Time
	EndTime   time.Time
	Limit     int
	Offset    int
}

// NewStoreTx creates a new StoreTx with default values.
func NewStoreTx(txID, txType string, stepNames []string) *StoreTx {
	now := time.Now()
	return &StoreTx{
		TxID:        txID,
		TxType:      txType,
		Status:      TxStatusCreated,
		CurrentStep: 0,
		TotalSteps:  len(stepNames),
		StepNames:   stepNames,
		Context:     &StoreTxContext{TxID: txID, TxType: txType},
		RetryCount:  0,
		MaxRetries:  3,
		Version:     0,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// NewStoreStepRecord creates a new StoreStepRecord with default values.
func NewStoreStepRecord(txID string, stepIndex int, stepName string) *StoreStepRecord {
	now := time.Now()
	return &StoreStepRecord{
		TxID:       txID,
		StepIndex:  stepIndex,
		StepName:   stepName,
		Status:     StepStatusPending,
		RetryCount: 0,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

// IsTerminal returns true if the transaction is in a terminal state.
func (t *StoreTx) IsTerminal() bool {
	return IsTxTerminal(t.Status)
}

// IsFailed returns true if the transaction is in a failed state.
func (t *StoreTx) IsFailed() bool {
	return IsTxFailed(t.Status)
}

// CanRetry returns true if the transaction can be retried.
func (t *StoreTx) CanRetry() bool {
	return t.Status == TxStatusFailed && t.RetryCount < t.MaxRetries
}

// IncrementVersion increments the version for optimistic locking.
func (t *StoreTx) IncrementVersion() {
	t.Version++
	t.UpdatedAt = time.Now()
}
