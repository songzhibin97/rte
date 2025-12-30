package store

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"rte"
)

// Transaction represents a transaction record in the database.
type Transaction struct {
	// ID is the auto-increment primary key.
	ID int64 `db:"id" json:"id"`

	// TxID is the unique transaction identifier.
	TxID string `db:"tx_id" json:"tx_id"`

	// TxType is the type of transaction (e.g., "saving_to_forex").
	TxType string `db:"tx_type" json:"tx_type"`

	// Status is the current transaction status.
	Status rte.TxStatus `db:"status" json:"status"`

	// CurrentStep is the index of the current step being executed.
	CurrentStep int `db:"current_step" json:"current_step"`

	// TotalSteps is the total number of steps in the transaction.
	TotalSteps int `db:"total_steps" json:"total_steps"`

	// StepNames contains the ordered list of step names.
	StepNames StringSlice `db:"step_names" json:"step_names"`

	// LockKeys contains the keys to lock for this transaction.
	LockKeys StringSlice `db:"lock_keys" json:"lock_keys"`

	// Context contains the transaction context (input, output, metadata).
	Context *ContextJSON `db:"context" json:"context"`

	// ErrorMsg contains the error message if the transaction failed.
	ErrorMsg string `db:"error_msg" json:"error_msg"`

	// RetryCount is the number of times this transaction has been retried.
	RetryCount int `db:"retry_count" json:"retry_count"`

	// MaxRetries is the maximum number of retries allowed.
	MaxRetries int `db:"max_retries" json:"max_retries"`

	// Version is used for optimistic locking.
	Version int `db:"version" json:"version"`

	// CreatedAt is when the transaction was created.
	CreatedAt time.Time `db:"created_at" json:"created_at"`

	// UpdatedAt is when the transaction was last updated.
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`

	// LockedAt is when the transaction acquired locks.
	LockedAt *time.Time `db:"locked_at" json:"locked_at,omitempty"`

	// CompletedAt is when the transaction completed.
	CompletedAt *time.Time `db:"completed_at" json:"completed_at,omitempty"`

	// TimeoutAt is when the transaction should timeout.
	TimeoutAt *time.Time `db:"timeout_at" json:"timeout_at,omitempty"`
}

// StepRecord represents a step record in the database.
type StepRecord struct {
	// ID is the auto-increment primary key.
	ID int64 `db:"id" json:"id"`

	// TxID is the transaction ID this step belongs to.
	TxID string `db:"tx_id" json:"tx_id"`

	// StepIndex is the index of this step in the transaction.
	StepIndex int `db:"step_index" json:"step_index"`

	// StepName is the name of the step.
	StepName string `db:"step_name" json:"step_name"`

	// Status is the current step status.
	Status rte.StepStatus `db:"status" json:"status"`

	// IdempotencyKey is the key used for idempotency checking.
	IdempotencyKey string `db:"idempotency_key" json:"idempotency_key,omitempty"`

	// Input contains the step input as JSON.
	Input []byte `db:"input" json:"input,omitempty"`

	// Output contains the step output as JSON.
	Output []byte `db:"output" json:"output,omitempty"`

	// ErrorMsg contains the error message if the step failed.
	ErrorMsg string `db:"error_msg" json:"error_msg,omitempty"`

	// RetryCount is the number of times this step has been retried.
	RetryCount int `db:"retry_count" json:"retry_count"`

	// StartedAt is when the step started executing.
	StartedAt *time.Time `db:"started_at" json:"started_at,omitempty"`

	// CompletedAt is when the step completed.
	CompletedAt *time.Time `db:"completed_at" json:"completed_at,omitempty"`

	// CreatedAt is when the step record was created.
	CreatedAt time.Time `db:"created_at" json:"created_at"`

	// UpdatedAt is when the step record was last updated.
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

// IdempotencyRecord represents an idempotency record in the database.
type IdempotencyRecord struct {
	// ID is the auto-increment primary key.
	ID int64 `db:"id" json:"id"`

	// IdempotencyKey is the unique key for this operation.
	IdempotencyKey string `db:"idempotency_key" json:"idempotency_key"`

	// Result contains the operation result.
	Result []byte `db:"result" json:"result,omitempty"`

	// CreatedAt is when the record was created.
	CreatedAt time.Time `db:"created_at" json:"created_at"`

	// ExpiresAt is when the record expires.
	ExpiresAt time.Time `db:"expires_at" json:"expires_at"`
}

// StringSlice is a custom type for storing string slices as JSON in the database.
type StringSlice []string

// Value implements the driver.Valuer interface for database serialization.
func (s StringSlice) Value() (driver.Value, error) {
	if s == nil {
		return "[]", nil
	}
	return json.Marshal(s)
}

// Scan implements the sql.Scanner interface for database deserialization.
func (s *StringSlice) Scan(value interface{}) error {
	if value == nil {
		*s = nil
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("cannot scan type %T into StringSlice", value)
	}

	return json.Unmarshal(bytes, s)
}

// ContextJSON is a wrapper for TxContext that implements database serialization.
type ContextJSON struct {
	TxID      string            `json:"tx_id"`
	TxType    string            `json:"tx_type"`
	StepIndex int               `json:"step_index"`
	Input     map[string]any    `json:"input"`
	Output    map[string]any    `json:"output"`
	Metadata  map[string]string `json:"metadata"`
}

// Value implements the driver.Valuer interface for database serialization.
func (c *ContextJSON) Value() (driver.Value, error) {
	if c == nil {
		return "{}", nil
	}
	return json.Marshal(c)
}

// Scan implements the sql.Scanner interface for database deserialization.
func (c *ContextJSON) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	var bytes []byte
	switch v := value.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		return fmt.Errorf("cannot scan type %T into ContextJSON", value)
	}

	return json.Unmarshal(bytes, c)
}

// ToTxContext converts ContextJSON to rte.TxContext.
func (c *ContextJSON) ToTxContext() *rte.TxContext {
	if c == nil {
		return nil
	}
	ctx := rte.NewTxContext(c.TxID, c.TxType)
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

// NewContextJSON creates a ContextJSON from rte.TxContext.
func NewContextJSON(ctx *rte.TxContext) *ContextJSON {
	if ctx == nil {
		return nil
	}
	return &ContextJSON{
		TxID:      ctx.TxID,
		TxType:    ctx.TxType,
		StepIndex: ctx.StepIndex,
		Input:     ctx.Input,
		Output:    ctx.Output,
		Metadata:  ctx.Metadata,
	}
}

// NewTransaction creates a new Transaction with default values.
func NewTransaction(txID, txType string, stepNames []string) *Transaction {
	now := time.Now()
	return &Transaction{
		TxID:        txID,
		TxType:      txType,
		Status:      rte.TxStatusCreated,
		CurrentStep: 0,
		TotalSteps:  len(stepNames),
		StepNames:   stepNames,
		LockKeys:    nil,
		Context:     &ContextJSON{TxID: txID, TxType: txType},
		RetryCount:  0,
		MaxRetries:  3,
		Version:     0,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// NewStepRecord creates a new StepRecord with default values.
func NewStepRecord(txID string, stepIndex int, stepName string) *StepRecord {
	now := time.Now()
	return &StepRecord{
		TxID:       txID,
		StepIndex:  stepIndex,
		StepName:   stepName,
		Status:     rte.StepStatusPending,
		RetryCount: 0,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

// IsTerminal returns true if the transaction is in a terminal state.
func (t *Transaction) IsTerminal() bool {
	return rte.IsTxTerminal(t.Status)
}

// IsFailed returns true if the transaction is in a failed state.
func (t *Transaction) IsFailed() bool {
	return rte.IsTxFailed(t.Status)
}

// CanRetry returns true if the transaction can be retried.
func (t *Transaction) CanRetry() bool {
	return t.Status == rte.TxStatusFailed && t.RetryCount < t.MaxRetries
}

// IncrementVersion increments the version for optimistic locking.
func (t *Transaction) IncrementVersion() {
	t.Version++
	t.UpdatedAt = time.Now()
}

// IsTerminal returns true if the step is in a terminal state.
func (s *StepRecord) IsTerminal() bool {
	return rte.IsStepTerminal(s.Status)
}

// CanCompensate returns true if the step can be compensated.
func (s *StepRecord) CanCompensate() bool {
	return rte.IsStepCompensatable(s.Status)
}
