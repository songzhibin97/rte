package rte

// TxStatus represents the status of a transaction
type TxStatus string

const (
	// TxStatusCreated indicates the transaction has been created
	TxStatusCreated TxStatus = "CREATED"
	// TxStatusLocked indicates the transaction has acquired locks
	TxStatusLocked TxStatus = "LOCKED"
	// TxStatusExecuting indicates the transaction is executing steps
	TxStatusExecuting TxStatus = "EXECUTING"
	// TxStatusConfirming indicates the transaction is confirming completion
	TxStatusConfirming TxStatus = "CONFIRMING"
	// TxStatusCompleted indicates the transaction completed successfully
	TxStatusCompleted TxStatus = "COMPLETED"
	// TxStatusFailed indicates the transaction failed
	TxStatusFailed TxStatus = "FAILED"
	// TxStatusCompensating indicates the transaction is compensating
	TxStatusCompensating TxStatus = "COMPENSATING"
	// TxStatusCompensated indicates the transaction has been compensated
	TxStatusCompensated TxStatus = "COMPENSATED"
	// TxStatusCompensationFailed indicates compensation failed
	TxStatusCompensationFailed TxStatus = "COMPENSATION_FAILED"
	// TxStatusCancelled indicates the transaction was cancelled
	TxStatusCancelled TxStatus = "CANCELLED"
	// TxStatusTimeout indicates the transaction timed out
	TxStatusTimeout TxStatus = "TIMEOUT"
)

// StepStatus represents the status of a step
type StepStatus string

const (
	// StepStatusPending indicates the step is pending execution
	StepStatusPending StepStatus = "PENDING"
	// StepStatusExecuting indicates the step is executing
	StepStatusExecuting StepStatus = "EXECUTING"
	// StepStatusCompleted indicates the step completed successfully
	StepStatusCompleted StepStatus = "COMPLETED"
	// StepStatusFailed indicates the step failed
	StepStatusFailed StepStatus = "FAILED"
	// StepStatusCompensating indicates the step is compensating
	StepStatusCompensating StepStatus = "COMPENSATING"
	// StepStatusCompensated indicates the step has been compensated
	StepStatusCompensated StepStatus = "COMPENSATED"
	// StepStatusSkipped indicates the step was skipped
	StepStatusSkipped StepStatus = "SKIPPED"
)

// validTxTransitions defines valid state transitions for transactions
var validTxTransitions = map[TxStatus][]TxStatus{
	TxStatusCreated: {
		TxStatusLocked,
		TxStatusCancelled,
	},
	TxStatusLocked: {
		TxStatusExecuting,
		TxStatusTimeout,
		TxStatusCancelled,
	},
	TxStatusExecuting: {
		TxStatusConfirming,
		TxStatusFailed,
		TxStatusTimeout,
		TxStatusCompensating,
	},
	TxStatusConfirming: {
		TxStatusCompleted,
		TxStatusFailed,
	},
	TxStatusCompleted: {},
	TxStatusFailed: {
		TxStatusCompensating,
	},
	TxStatusCompensating: {
		TxStatusCompensated,
		TxStatusCompensationFailed,
	},
	TxStatusCompensated:        {},
	TxStatusCompensationFailed: {},
	TxStatusCancelled:          {},
	TxStatusTimeout:            {},
}

// ValidateTxTransition checks if a transaction state transition is valid
func ValidateTxTransition(from, to TxStatus) bool {
	validTargets, ok := validTxTransitions[from]
	if !ok {
		return false
	}
	for _, target := range validTargets {
		if target == to {
			return true
		}
	}
	return false
}

// IsTxTerminal returns true if the transaction status is terminal (no further transitions)
func IsTxTerminal(status TxStatus) bool {
	switch status {
	case TxStatusCompleted, TxStatusCompensated, TxStatusCompensationFailed, TxStatusCancelled, TxStatusTimeout:
		return true
	default:
		return false
	}
}

// IsTxFailed returns true if the transaction is in a failed state
func IsTxFailed(status TxStatus) bool {
	switch status {
	case TxStatusFailed, TxStatusCompensationFailed, TxStatusTimeout:
		return true
	default:
		return false
	}
}

// validStepTransitions defines valid state transitions for steps
var validStepTransitions = map[StepStatus][]StepStatus{
	StepStatusPending: {
		StepStatusExecuting,
		StepStatusSkipped,
	},
	StepStatusExecuting: {
		StepStatusCompleted,
		StepStatusFailed,
	},
	StepStatusCompleted: {
		StepStatusCompensating,
	},
	StepStatusFailed: {
		StepStatusCompensating,
		StepStatusSkipped,
	},
	StepStatusCompensating: {
		StepStatusCompensated,
		StepStatusFailed,
	},
	StepStatusCompensated: {},
	StepStatusSkipped:     {},
}

// ValidateStepTransition checks if a step state transition is valid
func ValidateStepTransition(from, to StepStatus) bool {
	validTargets, ok := validStepTransitions[from]
	if !ok {
		return false
	}
	for _, target := range validTargets {
		if target == to {
			return true
		}
	}
	return false
}

// IsStepTerminal returns true if the step status is terminal
func IsStepTerminal(status StepStatus) bool {
	switch status {
	case StepStatusCompleted, StepStatusCompensated, StepStatusSkipped:
		return true
	default:
		return false
	}
}

// IsStepCompensatable returns true if the step can be compensated
func IsStepCompensatable(status StepStatus) bool {
	switch status {
	case StepStatusCompleted, StepStatusFailed:
		return true
	default:
		return false
	}
}
