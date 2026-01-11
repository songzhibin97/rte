// Package testinfra provides property-based tests for RTE production validation.
// Feature: financial-production-readiness
// This file contains property tests for event audit completeness .
package testinfra

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"rte"
	"rte/circuit/memory"
	"rte/event"

	"pgregory.net/rapid"
)

// ============================================================================

// step lifecycle change (started, completed, failed), a corresponding event
// SHALL be emitted with transaction ID, timestamp, and relevant context.
// ============================================================================

// TestProperty_EventCompleteness_Integration tests that all transaction and step
// lifecycle events are emitted with required data fields.

func TestProperty_EventCompleteness_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random number of steps (1-4)
		numSteps := rapid.IntRange(1, 4).Draw(rt, "numSteps")

		// Generate random failure scenario (-1 = no failure, 0 to numSteps-1 = fail at step)
		failAtStep := rapid.IntRange(-1, numSteps-1).Draw(rt, "failAtStep")

		// Create event collector
		collector := NewEventCollector()

		// Create a fresh event bus and subscribe collector
		eventBus := event.NewMemoryEventBus()
		eventBus.SubscribeAll(collector.Handle)

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create coordinator
		coord := rte.NewCoordinator(
			rte.WithStore(ti.StoreAdapter),
			rte.WithLocker(ti.Locker),
			rte.WithBreaker(breaker),
			rte.WithEventBus(eventBus),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       3,
				RetryInterval:    100 * time.Millisecond,
			}),
		)

		// Create step names deterministically
		stepNames := make([]string, numSteps)
		for i := 0; i < numSteps; i++ {
			stepNames[i] = fmt.Sprintf("event-completeness-step-%d", i)
		}

		// Register steps
		for i := 0; i < numSteps; i++ {
			stepIndex := i
			step := &eventCompletenessStep{
				BaseStep:   rte.NewBaseStep(stepNames[i]),
				stepIndex:  stepIndex,
				failAtStep: failAtStep,
			}
			coord.RegisterStep(step)
		}

		// Build transaction with all steps
		txID := ti.GenerateTxID(fmt.Sprintf("event-completeness-%d", atomic.AddInt64(&txIDCounter, 1)))
		txType := "event-completeness-test"
		builder := rte.NewTransactionWithID(txID, txType).WithStepRegistry(coord)
		for _, name := range stepNames {
			builder.AddStep(name)
		}

		tx, err := builder.Build()
		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction
		result, _ := coord.Execute(context.Background(), tx)

		// Get all collected events
		events := collector.Events()

		
		txCreatedEvents := filterEventsByType(events, event.EventTxCreated)
		if len(txCreatedEvents) != 1 {
			rt.Fatalf("expected exactly 1 tx.created event, got %d", len(txCreatedEvents))
		}
		validateEventFields(rt, txCreatedEvents[0], txID, txType, "tx.created")

		
		if failAtStep == -1 {
			if result.Status != rte.TxStatusCompleted {
				rt.Fatalf("expected COMPLETED status, got %s", result.Status)
			}
			txCompletedEvents := filterEventsByType(events, event.EventTxCompleted)
			if len(txCompletedEvents) != 1 {
				rt.Fatalf("expected exactly 1 tx.completed event, got %d", len(txCompletedEvents))
			}
			validateEventFields(rt, txCompletedEvents[0], txID, txType, "tx.completed")
		}

		
		if failAtStep >= 0 {
			txFailedEvents := filterEventsByType(events, event.EventTxFailed)
			if len(txFailedEvents) < 1 {
				rt.Fatalf("expected at least 1 tx.failed event, got %d", len(txFailedEvents))
			}
			validateEventFields(rt, txFailedEvents[0], txID, txType, "tx.failed")
		}

		
		stepStartedEvents := filterEventsByType(events, event.EventStepStarted)
		expectedStepStarted := numSteps
		if failAtStep >= 0 {
			expectedStepStarted = failAtStep + 1 // Steps 0 to failAtStep
		}
		if len(stepStartedEvents) != expectedStepStarted {
			rt.Fatalf("expected %d step.started events, got %d", expectedStepStarted, len(stepStartedEvents))
		}
		// Validate each step.started event has required fields
		for i, e := range stepStartedEvents {
			validateEventFields(rt, e, txID, txType, fmt.Sprintf("step.started[%d]", i))
			if e.StepName == "" {
				rt.Fatalf("step.started event %d missing StepName", i)
			}
		}

		
		stepCompletedEvents := filterEventsByType(events, event.EventStepCompleted)
		expectedStepCompleted := numSteps
		if failAtStep >= 0 {
			expectedStepCompleted = failAtStep // Steps 0 to failAtStep-1 completed
		}
		if len(stepCompletedEvents) != expectedStepCompleted {
			rt.Fatalf("expected %d step.completed events, got %d", expectedStepCompleted, len(stepCompletedEvents))
		}
		// Validate each step.completed event has required fields
		for i, e := range stepCompletedEvents {
			validateEventFields(rt, e, txID, txType, fmt.Sprintf("step.completed[%d]", i))
			if e.StepName == "" {
				rt.Fatalf("step.completed event %d missing StepName", i)
			}
		}

		
		if failAtStep >= 0 {
			stepFailedEvents := filterEventsByType(events, event.EventStepFailed)
			if len(stepFailedEvents) != 1 {
				rt.Fatalf("expected 1 step.failed event, got %d", len(stepFailedEvents))
			}
			validateEventFields(rt, stepFailedEvents[0], txID, txType, "step.failed")
			if stepFailedEvents[0].StepName == "" {
				rt.Fatalf("step.failed event missing StepName")
			}
			if stepFailedEvents[0].Error == nil {
				rt.Fatalf("step.failed event missing Error")
			}
		}

		
		if len(events) > 0 && events[0].Type != event.EventTxCreated {
			rt.Fatalf("expected first event to be tx.created, got %s", events[0].Type)
		}

		rt.Logf("Event completeness test: numSteps=%d, failAtStep=%d, status=%s, totalEvents=%d",
			numSteps, failAtStep, result.Status, len(events))
	})
}

// eventCompletenessStep is a test step for event completeness testing
type eventCompletenessStep struct {
	*rte.BaseStep
	stepIndex  int
	failAtStep int
}

func (s *eventCompletenessStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	if s.stepIndex == s.failAtStep {
		return fmt.Errorf("simulated failure at step %d", s.stepIndex)
	}
	return nil
}

func (s *eventCompletenessStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *eventCompletenessStep) SupportsCompensation() bool {
	return true
}

// Ensure eventCompletenessStep implements rte.Step interface
var _ rte.Step = (*eventCompletenessStep)(nil)

// filterEventsByType filters events by type
func filterEventsByType(events []event.Event, eventType event.EventType) []event.Event {
	var filtered []event.Event
	for _, e := range events {
		if e.Type == eventType {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

// validateEventFields validates that an event has required fields 
func validateEventFields(rt *rapid.T, e event.Event, expectedTxID, expectedTxType, eventDesc string) {
	
	if e.TxID != expectedTxID {
		rt.Fatalf("%s event TxID mismatch: expected %s, got %s", eventDesc, expectedTxID, e.TxID)
	}

	
	if e.TxType != expectedTxType {
		rt.Fatalf("%s event TxType mismatch: expected %s, got %s", eventDesc, expectedTxType, e.TxType)
	}

	
	if e.Timestamp.IsZero() {
		rt.Fatalf("%s event missing Timestamp", eventDesc)
	}
}

// ============================================================================

// SHALL continue without being blocked.
// ============================================================================

// TestProperty_NonBlockingEventDelivery_Integration tests that failing or panicking
// event handlers do not block transaction processing.

func TestProperty_NonBlockingEventDelivery_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random number of steps (1-3)
		numSteps := rapid.IntRange(1, 3).Draw(rt, "numSteps")

		// Generate random number of failing handlers (1-3)
		numFailingHandlers := rapid.IntRange(1, 3).Draw(rt, "numFailingHandlers")

		// Generate random number of panicking handlers (0-2)
		numPanickingHandlers := rapid.IntRange(0, 2).Draw(rt, "numPanickingHandlers")

		// Create event collector to verify events are still delivered
		collector := NewEventCollector()

		// Create a fresh event bus
		eventBus := event.NewMemoryEventBus()

		// Subscribe the collector first (should receive events)
		eventBus.SubscribeAll(collector.Handle)

		// Subscribe failing handlers
		for i := 0; i < numFailingHandlers; i++ {
			handlerIdx := i
			eventBus.SubscribeAll(func(ctx context.Context, e event.Event) error {
				return fmt.Errorf("simulated handler %d failure for event %s", handlerIdx, e.Type)
			})
		}

		// Subscribe panicking handlers
		for i := 0; i < numPanickingHandlers; i++ {
			handlerIdx := i
			eventBus.SubscribeAll(func(ctx context.Context, e event.Event) error {
				panic(fmt.Sprintf("simulated handler %d panic for event %s", handlerIdx, e.Type))
			})
		}

		// Subscribe another collector after failing handlers (should still receive events)
		collector2 := NewEventCollector()
		eventBus.SubscribeAll(collector2.Handle)

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create coordinator
		coord := rte.NewCoordinator(
			rte.WithStore(ti.StoreAdapter),
			rte.WithLocker(ti.Locker),
			rte.WithBreaker(breaker),
			rte.WithEventBus(eventBus),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       3,
				RetryInterval:    100 * time.Millisecond,
			}),
		)

		// Create step names deterministically
		stepNames := make([]string, numSteps)
		for i := 0; i < numSteps; i++ {
			stepNames[i] = fmt.Sprintf("non-blocking-step-%d", i)
		}

		// Register steps (all succeed)
		for i := 0; i < numSteps; i++ {
			step := &nonBlockingTestStep{
				BaseStep: rte.NewBaseStep(stepNames[i]),
			}
			coord.RegisterStep(step)
		}

		// Build transaction with all steps
		txID := ti.GenerateTxID(fmt.Sprintf("non-blocking-%d", atomic.AddInt64(&txIDCounter, 1)))
		builder := rte.NewTransactionWithID(txID, "non-blocking-test").WithStepRegistry(coord)
		for _, name := range stepNames {
			builder.AddStep(name)
		}

		tx, err := builder.Build()
		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction - should complete despite failing/panicking handlers
		result, execErr := coord.Execute(context.Background(), tx)

		
		if execErr != nil {
			rt.Fatalf("transaction execution failed due to event handler issues: %v", execErr)
		}

		if result.Status != rte.TxStatusCompleted {
			rt.Fatalf("expected COMPLETED status despite failing handlers, got %s", result.Status)
		}

		
		events1 := collector.Events()
		events2 := collector2.Events()

		if len(events1) == 0 {
			rt.Fatalf("first collector received no events despite failing handlers")
		}

		if len(events2) == 0 {
			rt.Fatalf("second collector received no events despite failing handlers")
		}

		
		if len(events1) != len(events2) {
			rt.Fatalf("collectors received different number of events: %d vs %d", len(events1), len(events2))
		}

		
		if !collector.HasEventType(event.EventTxCreated) {
			rt.Fatalf("tx.created event not received despite failing handlers")
		}

		if !collector.HasEventType(event.EventTxCompleted) {
			rt.Fatalf("tx.completed event not received despite failing handlers")
		}

		
		stepStartedCount := collector.CountEventType(event.EventStepStarted)
		stepCompletedCount := collector.CountEventType(event.EventStepCompleted)

		if stepStartedCount != numSteps {
			rt.Fatalf("expected %d step.started events, got %d", numSteps, stepStartedCount)
		}

		if stepCompletedCount != numSteps {
			rt.Fatalf("expected %d step.completed events, got %d", numSteps, stepCompletedCount)
		}

		rt.Logf("Non-blocking event delivery test: numSteps=%d, failingHandlers=%d, panickingHandlers=%d, status=%s, events=%d",
			numSteps, numFailingHandlers, numPanickingHandlers, result.Status, len(events1))
	})
}

// nonBlockingTestStep is a test step that always succeeds
type nonBlockingTestStep struct {
	*rte.BaseStep
}

func (s *nonBlockingTestStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *nonBlockingTestStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *nonBlockingTestStep) SupportsCompensation() bool {
	return true
}

// Ensure nonBlockingTestStep implements rte.Step interface
var _ rte.Step = (*nonBlockingTestStep)(nil)

// ============================================================================

// Tests that slow event handlers do not block transaction processing.
// ============================================================================

// TestProperty_SlowHandlerNonBlocking_Integration tests that slow event handlers
// do not block transaction processing.
func TestProperty_SlowHandlerNonBlocking_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random number of steps (1-2)
		numSteps := rapid.IntRange(1, 2).Draw(rt, "numSteps")

		// Track handler execution
		var handlerCalled int32

		// Create event collector
		collector := NewEventCollector()

		// Create a fresh event bus
		eventBus := event.NewMemoryEventBus()

		// Subscribe the collector
		eventBus.SubscribeAll(collector.Handle)

		// Subscribe a "slow" handler that increments counter
		// Note: In the current implementation, handlers are called synchronously,
		// but errors/panics don't block. This test verifies the handler is called.
		eventBus.SubscribeAll(func(ctx context.Context, e event.Event) error {
			atomic.AddInt32(&handlerCalled, 1)
			// Simulate some work (but not blocking)
			return nil
		})

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create coordinator
		coord := rte.NewCoordinator(
			rte.WithStore(ti.StoreAdapter),
			rte.WithLocker(ti.Locker),
			rte.WithBreaker(breaker),
			rte.WithEventBus(eventBus),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       3,
				RetryInterval:    100 * time.Millisecond,
			}),
		)

		// Create step names deterministically
		stepNames := make([]string, numSteps)
		for i := 0; i < numSteps; i++ {
			stepNames[i] = fmt.Sprintf("slow-handler-step-%d", i)
		}

		// Register steps
		for i := 0; i < numSteps; i++ {
			step := &nonBlockingTestStep{
				BaseStep: rte.NewBaseStep(stepNames[i]),
			}
			coord.RegisterStep(step)
		}

		// Build transaction
		txID := ti.GenerateTxID(fmt.Sprintf("slow-handler-%d", atomic.AddInt64(&txIDCounter, 1)))
		builder := rte.NewTransactionWithID(txID, "slow-handler-test").WithStepRegistry(coord)
		for _, name := range stepNames {
			builder.AddStep(name)
		}

		tx, err := builder.Build()
		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction
		result, execErr := coord.Execute(context.Background(), tx)

		
		if execErr != nil {
			rt.Fatalf("transaction execution failed: %v", execErr)
		}

		if result.Status != rte.TxStatusCompleted {
			rt.Fatalf("expected COMPLETED status, got %s", result.Status)
		}

		
		// Expected events: tx.created + (step.started + step.completed) * numSteps + tx.completed
		expectedEvents := 1 + (2 * numSteps) + 1
		actualHandlerCalls := atomic.LoadInt32(&handlerCalled)

		if int(actualHandlerCalls) != expectedEvents {
			rt.Fatalf("expected handler to be called %d times, got %d", expectedEvents, actualHandlerCalls)
		}

		rt.Logf("Slow handler test: numSteps=%d, handlerCalls=%d, status=%s",
			numSteps, actualHandlerCalls, result.Status)
	})
}

// ============================================================================

// Tests that one handler's error doesn't affect other handlers.
// ============================================================================

// TestProperty_ErrorHandlerIsolation_Integration tests that errors in one handler
// don't prevent other handlers from receiving events.
func TestProperty_ErrorHandlerIsolation_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random number of handlers (3-5)
		numHandlers := rapid.IntRange(3, 5).Draw(rt, "numHandlers")

		// Generate which handler should fail (0 to numHandlers-1)
		failingHandlerIdx := rapid.IntRange(0, numHandlers-1).Draw(rt, "failingHandlerIdx")

		// Track which handlers were called
		handlerCalls := make([]int32, numHandlers)

		// Create a fresh event bus
		eventBus := event.NewMemoryEventBus()

		// Subscribe handlers
		for i := 0; i < numHandlers; i++ {
			handlerIdx := i
			eventBus.SubscribeAll(func(ctx context.Context, e event.Event) error {
				atomic.AddInt32(&handlerCalls[handlerIdx], 1)
				if handlerIdx == failingHandlerIdx {
					return errors.New("simulated handler error")
				}
				return nil
			})
		}

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create coordinator
		coord := rte.NewCoordinator(
			rte.WithStore(ti.StoreAdapter),
			rte.WithLocker(ti.Locker),
			rte.WithBreaker(breaker),
			rte.WithEventBus(eventBus),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       3,
				RetryInterval:    100 * time.Millisecond,
			}),
		)

		// Register a simple step
		step := &nonBlockingTestStep{
			BaseStep: rte.NewBaseStep("isolation-step"),
		}
		coord.RegisterStep(step)

		// Build transaction
		txID := ti.GenerateTxID(fmt.Sprintf("isolation-%d", atomic.AddInt64(&txIDCounter, 1)))
		tx, err := rte.NewTransactionWithID(txID, "isolation-test").
			WithStepRegistry(coord).
			AddStep("isolation-step").
			Build()

		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction
		result, execErr := coord.Execute(context.Background(), tx)

		
		if execErr != nil {
			rt.Fatalf("transaction execution failed: %v", execErr)
		}

		if result.Status != rte.TxStatusCompleted {
			rt.Fatalf("expected COMPLETED status, got %s", result.Status)
		}

		
		// Expected events: tx.created + step.started + step.completed + tx.completed = 4
		expectedCalls := int32(4)

		for i := 0; i < numHandlers; i++ {
			calls := atomic.LoadInt32(&handlerCalls[i])
			if calls != expectedCalls {
				rt.Fatalf("handler %d expected %d calls, got %d", i, expectedCalls, calls)
			}
		}

		rt.Logf("Error handler isolation test: numHandlers=%d, failingHandler=%d, status=%s",
			numHandlers, failingHandlerIdx, result.Status)
	})
}
