package rte

import "errors"

// Transaction errors
var (
	// ErrTransactionNotFound indicates the transaction does not exist
	ErrTransactionNotFound = errors.New("transaction not found")

	// ErrTransactionAlreadyExists indicates the transaction already exists
	ErrTransactionAlreadyExists = errors.New("transaction already exists")

	// ErrInvalidTransactionState indicates an invalid state transition
	ErrInvalidTransactionState = errors.New("invalid transaction state")

	// ErrTransactionTimeout indicates the transaction has timed out
	ErrTransactionTimeout = errors.New("transaction timeout")

	// ErrTransactionCancelled indicates the transaction was cancelled
	ErrTransactionCancelled = errors.New("transaction cancelled")

	// ErrMaxRetriesExceeded indicates the maximum retry count has been exceeded
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
)

// Step errors
var (
	// ErrStepNotFound indicates the step does not exist
	ErrStepNotFound = errors.New("step not found")

	// ErrStepNotRegistered indicates the step is not registered
	ErrStepNotRegistered = errors.New("step not registered")

	// ErrCompensationNotSupported indicates the step does not support compensation
	ErrCompensationNotSupported = errors.New("compensation not supported")

	// ErrStepExecutionFailed indicates step execution failed
	ErrStepExecutionFailed = errors.New("step execution failed")

	// ErrStepTimeout indicates the step has timed out
	ErrStepTimeout = errors.New("step timeout")
)

// Lock errors
var (
	// ErrLockAcquisitionFailed indicates lock acquisition failed
	ErrLockAcquisitionFailed = errors.New("lock acquisition failed")

	// ErrLockNotHeld indicates the lock is not held
	ErrLockNotHeld = errors.New("lock not held")

	// ErrLockExtensionFailed indicates lock extension failed
	ErrLockExtensionFailed = errors.New("lock extension failed")

	// ErrLockReleaseFailed indicates lock release failed
	ErrLockReleaseFailed = errors.New("lock release failed")
)

// Compensation errors
var (
	// ErrCompensationFailed indicates compensation failed
	ErrCompensationFailed = errors.New("compensation failed")

	// ErrCompensationMaxRetriesExceeded indicates max retries exceeded during compensation
	ErrCompensationMaxRetriesExceeded = errors.New("compensation max retries exceeded")
)

// Store errors
var (
	// ErrVersionConflict indicates optimistic lock version conflict
	ErrVersionConflict = errors.New("version conflict")

	// ErrStoreOperationFailed indicates a store operation failed
	ErrStoreOperationFailed = errors.New("store operation failed")
)

// Circuit breaker errors
var (
	// ErrCircuitOpen indicates the circuit breaker is open
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

// Idempotency errors
var (
	// ErrIdempotencyCheckFailed indicates idempotency check failed
	ErrIdempotencyCheckFailed = errors.New("idempotency check failed")
)

// Context errors
var (
	// ErrInputKeyNotFound indicates the input key was not found
	ErrInputKeyNotFound = errors.New("input key not found")

	// ErrInputTypeMismatch indicates the input type does not match
	ErrInputTypeMismatch = errors.New("input type mismatch")

	// ErrOutputKeyNotFound indicates the output key was not found
	ErrOutputKeyNotFound = errors.New("output key not found")

	// ErrOutputTypeMismatch indicates the output type does not match
	ErrOutputTypeMismatch = errors.New("output type mismatch")
)

// Config errors
var (
	// ErrInvalidConfig indicates the configuration is invalid
	ErrInvalidConfig = errors.New("invalid configuration")
)
