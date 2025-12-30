package rte

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Transaction represents a transaction definition with its configuration.
// It uses a builder pattern for fluent configuration.
type Transaction struct {
	// txID is the unique transaction identifier
	txID string

	// txType is the type of transaction (e.g., "saving_to_forex")
	txType string

	// stepNames contains the ordered list of step names to execute
	stepNames []string

	// lockKeys contains the keys to lock for this transaction
	lockKeys []string

	// input contains the input parameters for the transaction
	input map[string]any

	// metadata contains additional metadata for the transaction
	metadata map[string]string

	// timeout is the total transaction timeout
	timeout time.Duration

	// maxRetries is the maximum number of retries for the transaction
	maxRetries int

	// stepRegistry is a reference to the step registry for validation
	stepRegistry StepRegistry
}

// StepRegistry defines the interface for step registration lookup.
// This is used to validate that steps are registered before execution.
type StepRegistry interface {
	// GetStep returns a step by name, or nil if not found
	GetStep(name string) Step
	// HasStep returns true if a step with the given name is registered
	HasStep(name string) bool
}

// TransactionBuilder provides a fluent API for building transactions.
type TransactionBuilder struct {
	tx     *Transaction
	errors []error
}

// NewTransaction creates a new transaction builder with the given type.
// The transaction ID is automatically generated using UUID.
func NewTransaction(txType string) *TransactionBuilder {
	return &TransactionBuilder{
		tx: &Transaction{
			txID:       uuid.New().String(),
			txType:     txType,
			stepNames:  make([]string, 0),
			lockKeys:   make([]string, 0),
			input:      make(map[string]any),
			metadata:   make(map[string]string),
			maxRetries: 3, // default max retries
		},
		errors: make([]error, 0),
	}
}

// NewTransactionWithID creates a new transaction builder with a specific ID.
// This is useful for idempotent transaction creation.
func NewTransactionWithID(txID, txType string) *TransactionBuilder {
	return &TransactionBuilder{
		tx: &Transaction{
			txID:       txID,
			txType:     txType,
			stepNames:  make([]string, 0),
			lockKeys:   make([]string, 0),
			input:      make(map[string]any),
			metadata:   make(map[string]string),
			maxRetries: 3, // default max retries
		},
		errors: make([]error, 0),
	}
}

// WithLockKeys sets the lock keys for the transaction.
// These keys will be acquired before execution to prevent concurrent modifications.
func (b *TransactionBuilder) WithLockKeys(keys ...string) *TransactionBuilder {
	b.tx.lockKeys = append(b.tx.lockKeys, keys...)
	return b
}

// WithInput sets the input parameters for the transaction.
// These parameters will be available to all steps via TxContext.
func (b *TransactionBuilder) WithInput(input map[string]any) *TransactionBuilder {
	for k, v := range input {
		b.tx.input[k] = v
	}
	return b
}

// WithInputValue sets a single input parameter.
func (b *TransactionBuilder) WithInputValue(key string, value any) *TransactionBuilder {
	b.tx.input[key] = value
	return b
}

// WithMetadata sets the metadata for the transaction.
func (b *TransactionBuilder) WithMetadata(metadata map[string]string) *TransactionBuilder {
	for k, v := range metadata {
		b.tx.metadata[k] = v
	}
	return b
}

// WithMetadataValue sets a single metadata value.
func (b *TransactionBuilder) WithMetadataValue(key, value string) *TransactionBuilder {
	b.tx.metadata[key] = value
	return b
}

// WithTimeout sets the total transaction timeout.
func (b *TransactionBuilder) WithTimeout(timeout time.Duration) *TransactionBuilder {
	b.tx.timeout = timeout
	return b
}

// WithMaxRetries sets the maximum number of retries for the transaction.
func (b *TransactionBuilder) WithMaxRetries(maxRetries int) *TransactionBuilder {
	b.tx.maxRetries = maxRetries
	return b
}

// WithStepRegistry sets the step registry for validation.
// When set, AddStep will validate that steps are registered.
func (b *TransactionBuilder) WithStepRegistry(registry StepRegistry) *TransactionBuilder {
	b.tx.stepRegistry = registry
	return b
}

// AddStep adds a step to the transaction by name.
// If a step registry is set, it validates that the step is registered.
func (b *TransactionBuilder) AddStep(stepName string) *TransactionBuilder {
	if stepName == "" {
		b.errors = append(b.errors, fmt.Errorf("%w: step name cannot be empty", ErrStepNotRegistered))
		return b
	}

	// Validate step is registered if registry is available
	if b.tx.stepRegistry != nil && !b.tx.stepRegistry.HasStep(stepName) {
		b.errors = append(b.errors, fmt.Errorf("%w: %s", ErrStepNotRegistered, stepName))
		return b
	}

	b.tx.stepNames = append(b.tx.stepNames, stepName)
	return b
}

// AddSteps adds multiple steps to the transaction.
func (b *TransactionBuilder) AddSteps(stepNames ...string) *TransactionBuilder {
	for _, name := range stepNames {
		b.AddStep(name)
	}
	return b
}

// Build validates and returns the transaction.
// Returns an error if validation fails.
func (b *TransactionBuilder) Build() (*Transaction, error) {
	// Check for accumulated errors
	if len(b.errors) > 0 {
		return nil, b.errors[0]
	}

	// Validate transaction type
	if b.tx.txType == "" {
		return nil, fmt.Errorf("transaction type cannot be empty")
	}

	// Validate at least one step
	if len(b.tx.stepNames) == 0 {
		return nil, fmt.Errorf("transaction must have at least one step")
	}

	return b.tx, nil
}

// MustBuild validates and returns the transaction, panicking on error.
// Use this only when you're certain the transaction is valid.
func (b *TransactionBuilder) MustBuild() *Transaction {
	tx, err := b.Build()
	if err != nil {
		panic(err)
	}
	return tx
}

// Transaction getter methods

// TxID returns the transaction ID.
func (t *Transaction) TxID() string {
	return t.txID
}

// TxType returns the transaction type.
func (t *Transaction) TxType() string {
	return t.txType
}

// StepNames returns the ordered list of step names.
func (t *Transaction) StepNames() []string {
	result := make([]string, len(t.stepNames))
	copy(result, t.stepNames)
	return result
}

// LockKeys returns the lock keys.
func (t *Transaction) LockKeys() []string {
	result := make([]string, len(t.lockKeys))
	copy(result, t.lockKeys)
	return result
}

// Input returns the input parameters.
func (t *Transaction) Input() map[string]any {
	result := make(map[string]any, len(t.input))
	for k, v := range t.input {
		result[k] = v
	}
	return result
}

// Metadata returns the metadata.
func (t *Transaction) Metadata() map[string]string {
	result := make(map[string]string, len(t.metadata))
	for k, v := range t.metadata {
		result[k] = v
	}
	return result
}

// Timeout returns the transaction timeout.
func (t *Transaction) Timeout() time.Duration {
	return t.timeout
}

// MaxRetries returns the maximum number of retries.
func (t *Transaction) MaxRetries() int {
	return t.maxRetries
}

// TotalSteps returns the total number of steps.
func (t *Transaction) TotalSteps() int {
	return len(t.stepNames)
}

// ToContext creates a TxContext from the transaction.
func (t *Transaction) ToContext() *TxContext {
	ctx := NewTxContext(t.txID, t.txType)
	ctx.WithInput(t.input)
	for k, v := range t.metadata {
		ctx.SetMetadata(k, v)
	}
	return ctx
}

// Validate validates the transaction configuration.
// This is called automatically by Build(), but can be called separately.
func (t *Transaction) Validate(registry StepRegistry) error {
	if t.txID == "" {
		return fmt.Errorf("transaction ID cannot be empty")
	}

	if t.txType == "" {
		return fmt.Errorf("transaction type cannot be empty")
	}

	if len(t.stepNames) == 0 {
		return fmt.Errorf("transaction must have at least one step")
	}

	// Validate all steps are registered
	if registry != nil {
		for _, stepName := range t.stepNames {
			if !registry.HasStep(stepName) {
				return fmt.Errorf("%w: %s", ErrStepNotRegistered, stepName)
			}
		}
	}

	return nil
}
