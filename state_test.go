package rte

import (
	"testing"

	"pgregory.net/rapid"
)

// ============================================================================
// Unit Tests for state.go
// Tests ValidateTxTransition, IsTxFailed, ValidateStepTransition,
// IsStepTerminal, and IsStepCompensatable
// ============================================================================

// All valid transaction statuses
var allTxStatuses = []TxStatus{
	TxStatusCreated,
	TxStatusLocked,
	TxStatusExecuting,
	TxStatusConfirming,
	TxStatusCompleted,
	TxStatusFailed,
	TxStatusCompensating,
	TxStatusCompensated,
	TxStatusCompensationFailed,
	TxStatusCancelled,
	TxStatusTimeout,
}

// All valid step statuses
var allStepStatuses = []StepStatus{
	StepStatusPending,
	StepStatusExecuting,
	StepStatusCompleted,
	StepStatusFailed,
	StepStatusCompensating,
	StepStatusCompensated,
	StepStatusSkipped,
}

func TestValidateTxTransition_ValidTransitions(t *testing.T) {
	// Test all valid transitions from the state machine
	validTransitions := []struct {
		from TxStatus
		to   TxStatus
	}{
		// From CREATED
		{TxStatusCreated, TxStatusLocked},
		{TxStatusCreated, TxStatusCancelled},
		// From LOCKED
		{TxStatusLocked, TxStatusExecuting},
		{TxStatusLocked, TxStatusTimeout},
		{TxStatusLocked, TxStatusCancelled},
		// From EXECUTING
		{TxStatusExecuting, TxStatusConfirming},
		{TxStatusExecuting, TxStatusFailed},
		{TxStatusExecuting, TxStatusTimeout},
		{TxStatusExecuting, TxStatusCompensating},
		// From CONFIRMING
		{TxStatusConfirming, TxStatusCompleted},
		{TxStatusConfirming, TxStatusFailed},
		// From FAILED
		{TxStatusFailed, TxStatusCompensating},
		// From COMPENSATING
		{TxStatusCompensating, TxStatusCompensated},
		{TxStatusCompensating, TxStatusCompensationFailed},
	}

	for _, tt := range validTransitions {
		if !ValidateTxTransition(tt.from, tt.to) {
			t.Errorf("transition from %s to %s should be valid", tt.from, tt.to)
		}
	}
}

func TestValidateTxTransition_InvalidTransitions(t *testing.T) {
	// Test some invalid transitions
	invalidTransitions := []struct {
		from TxStatus
		to   TxStatus
	}{
		// Cannot go back to CREATED
		{TxStatusLocked, TxStatusCreated},
		{TxStatusExecuting, TxStatusCreated},
		// Cannot skip states
		{TxStatusCreated, TxStatusExecuting},
		{TxStatusCreated, TxStatusCompleted},
		// Terminal states cannot transition
		{TxStatusCompleted, TxStatusFailed},
		{TxStatusCompensated, TxStatusFailed},
		{TxStatusCancelled, TxStatusFailed},
		{TxStatusTimeout, TxStatusFailed},
		{TxStatusCompensationFailed, TxStatusCompensating},
		// Self-transitions are invalid
		{TxStatusCreated, TxStatusCreated},
		{TxStatusExecuting, TxStatusExecuting},
	}

	for _, tt := range invalidTransitions {
		if ValidateTxTransition(tt.from, tt.to) {
			t.Errorf("transition from %s to %s should be invalid", tt.from, tt.to)
		}
	}
}

func TestValidateTxTransition_UnknownStatus(t *testing.T) {
	unknownStatus := TxStatus("UNKNOWN")

	// Unknown source status
	if ValidateTxTransition(unknownStatus, TxStatusLocked) {
		t.Error("transition from unknown status should be invalid")
	}

	// Unknown target status
	if ValidateTxTransition(TxStatusCreated, unknownStatus) {
		t.Error("transition to unknown status should be invalid")
	}
}

func TestValidateTxTransition_TerminalStatesHaveNoTransitions(t *testing.T) {
	terminalStates := []TxStatus{
		TxStatusCompleted,
		TxStatusCompensated,
		TxStatusCompensationFailed,
		TxStatusCancelled,
		TxStatusTimeout,
	}

	for _, terminal := range terminalStates {
		for _, target := range allTxStatuses {
			if ValidateTxTransition(terminal, target) {
				t.Errorf("terminal state %s should not allow transition to %s", terminal, target)
			}
		}
	}
}

func TestIsTxFailed(t *testing.T) {
	failedStatuses := []TxStatus{
		TxStatusFailed,
		TxStatusCompensationFailed,
		TxStatusTimeout,
	}

	nonFailedStatuses := []TxStatus{
		TxStatusCreated,
		TxStatusLocked,
		TxStatusExecuting,
		TxStatusConfirming,
		TxStatusCompleted,
		TxStatusCompensating,
		TxStatusCompensated,
		TxStatusCancelled,
	}

	for _, status := range failedStatuses {
		if !IsTxFailed(status) {
			t.Errorf("%s should be considered failed", status)
		}
	}

	for _, status := range nonFailedStatuses {
		if IsTxFailed(status) {
			t.Errorf("%s should not be considered failed", status)
		}
	}
}

func TestIsTxTerminal(t *testing.T) {
	terminalStatuses := []TxStatus{
		TxStatusCompleted,
		TxStatusCompensated,
		TxStatusCompensationFailed,
		TxStatusCancelled,
		TxStatusTimeout,
	}

	nonTerminalStatuses := []TxStatus{
		TxStatusCreated,
		TxStatusLocked,
		TxStatusExecuting,
		TxStatusConfirming,
		TxStatusFailed,
		TxStatusCompensating,
	}

	for _, status := range terminalStatuses {
		if !IsTxTerminal(status) {
			t.Errorf("%s should be terminal", status)
		}
	}

	for _, status := range nonTerminalStatuses {
		if IsTxTerminal(status) {
			t.Errorf("%s should not be terminal", status)
		}
	}
}

func TestValidateStepTransition_ValidTransitions(t *testing.T) {
	validTransitions := []struct {
		from StepStatus
		to   StepStatus
	}{
		// From PENDING
		{StepStatusPending, StepStatusExecuting},
		{StepStatusPending, StepStatusSkipped},
		// From EXECUTING
		{StepStatusExecuting, StepStatusCompleted},
		{StepStatusExecuting, StepStatusFailed},
		// From COMPLETED
		{StepStatusCompleted, StepStatusCompensating},
		// From FAILED
		{StepStatusFailed, StepStatusCompensating},
		{StepStatusFailed, StepStatusSkipped},
		// From COMPENSATING
		{StepStatusCompensating, StepStatusCompensated},
		{StepStatusCompensating, StepStatusFailed},
	}

	for _, tt := range validTransitions {
		if !ValidateStepTransition(tt.from, tt.to) {
			t.Errorf("step transition from %s to %s should be valid", tt.from, tt.to)
		}
	}
}

func TestValidateStepTransition_InvalidTransitions(t *testing.T) {
	invalidTransitions := []struct {
		from StepStatus
		to   StepStatus
	}{
		// Cannot go back to PENDING
		{StepStatusExecuting, StepStatusPending},
		{StepStatusCompleted, StepStatusPending},
		// Cannot skip states
		{StepStatusPending, StepStatusCompleted},
		{StepStatusPending, StepStatusFailed},
		// Terminal states cannot transition
		{StepStatusCompensated, StepStatusFailed},
		{StepStatusSkipped, StepStatusExecuting},
		// Self-transitions are invalid
		{StepStatusPending, StepStatusPending},
		{StepStatusExecuting, StepStatusExecuting},
	}

	for _, tt := range invalidTransitions {
		if ValidateStepTransition(tt.from, tt.to) {
			t.Errorf("step transition from %s to %s should be invalid", tt.from, tt.to)
		}
	}
}

func TestValidateStepTransition_UnknownStatus(t *testing.T) {
	unknownStatus := StepStatus("UNKNOWN")

	if ValidateStepTransition(unknownStatus, StepStatusExecuting) {
		t.Error("transition from unknown status should be invalid")
	}

	if ValidateStepTransition(StepStatusPending, unknownStatus) {
		t.Error("transition to unknown status should be invalid")
	}
}

func TestIsStepTerminal(t *testing.T) {
	terminalStatuses := []StepStatus{
		StepStatusCompleted,
		StepStatusCompensated,
		StepStatusSkipped,
	}

	nonTerminalStatuses := []StepStatus{
		StepStatusPending,
		StepStatusExecuting,
		StepStatusFailed,
		StepStatusCompensating,
	}

	for _, status := range terminalStatuses {
		if !IsStepTerminal(status) {
			t.Errorf("%s should be terminal", status)
		}
	}

	for _, status := range nonTerminalStatuses {
		if IsStepTerminal(status) {
			t.Errorf("%s should not be terminal", status)
		}
	}
}

func TestIsStepCompensatable(t *testing.T) {
	compensatableStatuses := []StepStatus{
		StepStatusCompleted,
		StepStatusFailed,
	}

	nonCompensatableStatuses := []StepStatus{
		StepStatusPending,
		StepStatusExecuting,
		StepStatusCompensating,
		StepStatusCompensated,
		StepStatusSkipped,
	}

	for _, status := range compensatableStatuses {
		if !IsStepCompensatable(status) {
			t.Errorf("%s should be compensatable", status)
		}
	}

	for _, status := range nonCompensatableStatuses {
		if IsStepCompensatable(status) {
			t.Errorf("%s should not be compensatable", status)
		}
	}
}

func TestProperty_StateTransitionValidity(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Test transaction state transitions
		fromTxIdx := rapid.IntRange(0, len(allTxStatuses)-1).Draw(rt, "fromTxIdx")
		toTxIdx := rapid.IntRange(0, len(allTxStatuses)-1).Draw(rt, "toTxIdx")

		fromTx := allTxStatuses[fromTxIdx]
		toTx := allTxStatuses[toTxIdx]

		// Check if transition is in the valid transitions map
		validTargets, exists := validTxTransitions[fromTx]
		expectedValid := false
		if exists {
			for _, target := range validTargets {
				if target == toTx {
					expectedValid = true
					break
				}
			}
		}

		actualValid := ValidateTxTransition(fromTx, toTx)

		
		if actualValid != expectedValid {
			rt.Fatalf("ValidateTxTransition(%s, %s) = %v, expected %v",
				fromTx, toTx, actualValid, expectedValid)
		}

		
		if IsTxTerminal(fromTx) && actualValid {
			rt.Fatalf("Terminal state %s should not allow transition to %s", fromTx, toTx)
		}

		// Test step state transitions
		fromStepIdx := rapid.IntRange(0, len(allStepStatuses)-1).Draw(rt, "fromStepIdx")
		toStepIdx := rapid.IntRange(0, len(allStepStatuses)-1).Draw(rt, "toStepIdx")

		fromStep := allStepStatuses[fromStepIdx]
		toStep := allStepStatuses[toStepIdx]

		// Check if step transition is in the valid transitions map
		validStepTargets, stepExists := validStepTransitions[fromStep]
		expectedStepValid := false
		if stepExists {
			for _, target := range validStepTargets {
				if target == toStep {
					expectedStepValid = true
					break
				}
			}
		}

		actualStepValid := ValidateStepTransition(fromStep, toStep)

		
		if actualStepValid != expectedStepValid {
			rt.Fatalf("ValidateStepTransition(%s, %s) = %v, expected %v",
				fromStep, toStep, actualStepValid, expectedStepValid)
		}

		
		// (except COMPLETED which can go to COMPENSATING)
		if IsStepTerminal(fromStep) && fromStep != StepStatusCompleted && actualStepValid {
			rt.Fatalf("Terminal step state %s should not allow transition to %s", fromStep, toStep)
		}
	})
}

func TestProperty_TerminalStateConsistency(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Test transaction states
		txIdx := rapid.IntRange(0, len(allTxStatuses)-1).Draw(rt, "txIdx")
		txStatus := allTxStatuses[txIdx]

		isTerminal := IsTxTerminal(txStatus)
		validTargets := validTxTransitions[txStatus]

		
		if isTerminal && len(validTargets) > 0 {
			rt.Fatalf("Terminal state %s should have no valid transitions, but has %v",
				txStatus, validTargets)
		}

		
		// (except for edge cases in the state machine)
		if !isTerminal && txStatus != TxStatusFailed && len(validTargets) == 0 {
			rt.Fatalf("Non-terminal state %s should have valid transitions", txStatus)
		}

		// Test step states
		stepIdx := rapid.IntRange(0, len(allStepStatuses)-1).Draw(rt, "stepIdx")
		stepStatus := allStepStatuses[stepIdx]

		isStepTerminal := IsStepTerminal(stepStatus)
		validStepTargets := validStepTransitions[stepStatus]

		
		if isStepTerminal && stepStatus != StepStatusCompleted && len(validStepTargets) > 0 {
			rt.Fatalf("Terminal step state %s should have no valid transitions, but has %v",
				stepStatus, validStepTargets)
		}
	})
}
