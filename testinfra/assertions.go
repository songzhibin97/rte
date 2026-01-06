// Package testinfra provides test infrastructure for RTE production validation.
package testinfra

import (
	"context"
	"rte/event"
	"sync"
	"testing"

	"rte"
)

// AssertBalanceConservation asserts that total balance is conserved
func AssertBalanceConservation(t testing.TB, expected, actual int64) {
	t.Helper()
	if expected != actual {
		t.Errorf("Balance conservation violated: expected %d, got %d", expected, actual)
	}
}

// AssertTransactionStatus asserts that a transaction has the expected status
func AssertTransactionStatus(t testing.TB, store rte.TxStore, txID string, expected rte.TxStatus) {
	t.Helper()
	tx, err := store.GetTransaction(context.Background(), txID)
	if err != nil {
		t.Fatalf("Failed to get transaction %s: %v", txID, err)
	}
	if tx.Status != expected {
		t.Errorf("Transaction %s status: expected %s, got %s", txID, expected, tx.Status)
	}
}

// AssertTransactionCompleted asserts that a transaction completed successfully
func AssertTransactionCompleted(t testing.TB, store rte.TxStore, txID string) {
	t.Helper()
	AssertTransactionStatus(t, store, txID, rte.TxStatusCompleted)
}

// AssertTransactionFailed asserts that a transaction failed
func AssertTransactionFailed(t testing.TB, store rte.TxStore, txID string) {
	t.Helper()
	AssertTransactionStatus(t, store, txID, rte.TxStatusFailed)
}

// AssertTransactionCompensated asserts that a transaction was compensated
func AssertTransactionCompensated(t testing.TB, store rte.TxStore, txID string) {
	t.Helper()
	AssertTransactionStatus(t, store, txID, rte.TxStatusCompensated)
}

// AssertStepStatus asserts that a step has the expected status
func AssertStepStatus(t testing.TB, store rte.TxStore, txID string, stepIndex int, expected rte.StepStatus) {
	t.Helper()
	step, err := store.GetStep(context.Background(), txID, stepIndex)
	if err != nil {
		t.Fatalf("Failed to get step %s[%d]: %v", txID, stepIndex, err)
	}
	if step.Status != expected {
		t.Errorf("Step %s[%d] status: expected %s, got %s", txID, stepIndex, expected, step.Status)
	}
}

// AssertCompensationOrder asserts that steps were compensated in reverse order
func AssertCompensationOrder(t testing.TB, compensationOrder []int, failedAtStep int) {
	t.Helper()
	expectedCount := failedAtStep // Steps 0 to failedAtStep-1 should be compensated
	if len(compensationOrder) != expectedCount {
		t.Errorf("Expected %d compensations, got %d: %v", expectedCount, len(compensationOrder), compensationOrder)
		return
	}

	// Verify reverse order
	for i := 0; i < len(compensationOrder); i++ {
		expectedStepIndex := failedAtStep - 1 - i
		if compensationOrder[i] != expectedStepIndex {
			t.Errorf("Compensation order incorrect: expected step %d at position %d, got step %d. Full order: %v",
				expectedStepIndex, i, compensationOrder[i], compensationOrder)
			return
		}
	}
}

// AssertAllStepsCompensated asserts that all completed steps were compensated
func AssertAllStepsCompensated(t testing.TB, store rte.TxStore, txID string, failedAtStep int) {
	t.Helper()
	steps, err := store.GetSteps(context.Background(), txID)
	if err != nil {
		t.Fatalf("Failed to get steps for %s: %v", txID, err)
	}

	for _, step := range steps {
		if step.StepIndex < failedAtStep {
			if step.Status != rte.StepStatusCompensated {
				t.Errorf("Step %s[%d] should be COMPENSATED, got %s", txID, step.StepIndex, step.Status)
			}
		} else if step.StepIndex == failedAtStep {
			if step.Status != rte.StepStatusFailed {
				t.Errorf("Step %s[%d] should be FAILED, got %s", txID, step.StepIndex, step.Status)
			}
		} else {
			if step.Status != rte.StepStatusPending {
				t.Errorf("Step %s[%d] should be PENDING, got %s", txID, step.StepIndex, step.Status)
			}
		}
	}
}

// EventCollector collects events for testing
type EventCollector struct {
	events []event.Event
	mu     sync.Mutex
}

// NewEventCollector creates a new event collector
func NewEventCollector() *EventCollector {
	return &EventCollector{
		events: make([]event.Event, 0),
	}
}

// Handle handles an event by collecting it (matches event.EventHandler signature)
func (c *EventCollector) Handle(ctx context.Context, e event.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
	return nil
}

// Events returns all collected events
func (c *EventCollector) Events() []event.Event {
	return c.events
}

// Clear clears all collected events
func (c *EventCollector) Clear() {
	c.events = c.events[:0]
}

// HasEventType checks if an event of the given type was collected
func (c *EventCollector) HasEventType(eventType event.EventType) bool {
	for _, e := range c.events {
		if e.Type == eventType {
			return true
		}
	}
	return false
}

// CountEventType counts events of the given type
func (c *EventCollector) CountEventType(eventType event.EventType) int {
	count := 0
	for _, e := range c.events {
		if e.Type == eventType {
			count++
		}
	}
	return count
}

// AssertEventPublished asserts that an event of the given type was published
func AssertEventPublished(t testing.TB, collector *EventCollector, eventType event.EventType) {
	t.Helper()
	if !collector.HasEventType(eventType) {
		t.Errorf("Expected event %s to be published, but it was not", eventType)
	}
}

// AssertEventNotPublished asserts that an event of the given type was not published
func AssertEventNotPublished(t testing.TB, collector *EventCollector, eventType event.EventType) {
	t.Helper()
	if collector.HasEventType(eventType) {
		t.Errorf("Expected event %s to not be published, but it was", eventType)
	}
}

// AssertEventCount asserts the count of events of the given type
func AssertEventCount(t testing.TB, collector *EventCollector, eventType event.EventType, expected int) {
	t.Helper()
	actual := collector.CountEventType(eventType)
	if actual != expected {
		t.Errorf("Expected %d events of type %s, got %d", expected, eventType, actual)
	}
}

// AssertNoError asserts that there is no error
func AssertNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("Expected no error, got: %v", err)
	}
}

// AssertError asserts that there is an error
func AssertError(t testing.TB, err error) {
	t.Helper()
	if err == nil {
		t.Error("Expected an error, got nil")
	}
}

// AssertErrorIs asserts that the error is of the expected type
func AssertErrorIs(t testing.TB, err, target error) {
	t.Helper()
	if err == nil {
		t.Errorf("Expected error %v, got nil", target)
		return
	}
	// Use errors.Is for proper error comparison
	// For now, simple string comparison
	if err.Error() != target.Error() {
		t.Errorf("Expected error %v, got %v", target, err)
	}
}

// AssertTrue asserts that a condition is true
func AssertTrue(t testing.TB, condition bool, msg string) {
	t.Helper()
	if !condition {
		t.Errorf("Assertion failed: %s", msg)
	}
}

// AssertFalse asserts that a condition is false
func AssertFalse(t testing.TB, condition bool, msg string) {
	t.Helper()
	if condition {
		t.Errorf("Assertion failed: %s", msg)
	}
}

// AssertEqual asserts that two values are equal
func AssertEqual[T comparable](t testing.TB, expected, actual T) {
	t.Helper()
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}

// AssertNotEqual asserts that two values are not equal
func AssertNotEqual[T comparable](t testing.TB, expected, actual T) {
	t.Helper()
	if expected == actual {
		t.Errorf("Expected values to be different, both are %v", expected)
	}
}
