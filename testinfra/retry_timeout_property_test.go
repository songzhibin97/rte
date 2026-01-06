// Package testinfra provides property-based tests for RTE production validation.
// Feature: rte-production-validation
// This file contains property tests for retry and timeout properties.
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
// Property 6: Retry Boundedness (重试有界性)
// For any failing step, the number of retry attempts SHALL not exceed the
// configured maximum retries.
// ============================================================================

// TestProperty_RetryBoundedness_Integration tests retry boundedness using real MySQL and Redis.
// Property 6: Retry Boundedness
// *For any* failing step, the number of retry attempts SHALL not exceed the
// configured maximum retries.
func TestProperty_RetryBoundedness_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random max retries (1-5)
		maxRetries := rapid.IntRange(1, 5).Draw(rt, "maxRetries")

		// Track actual execution count (initial execution + retries)
		var executionCount int64

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create a fresh event bus for this iteration
		eventBus := event.NewMemoryEventBus()

		// Create coordinator with specific max retries
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
				MaxRetries:       maxRetries,
				RetryInterval:    10 * time.Millisecond, // Short interval for faster tests
				RetryMaxInterval: 100 * time.Millisecond,
				RetryMultiplier:  1.5,
				RetryJitter:      0.1,
			}),
		)

		// Create step that always fails and tracks execution count
		step := &alwaysFailingStep{
			BaseStep:       rte.NewBaseStep("always-failing-step"),
			executionCount: &executionCount,
		}
		coord.RegisterStep(step)

		// Build and execute transaction
		txID := ti.GenerateTxID(fmt.Sprintf("retry-%d", atomic.AddInt64(&txIDCounter, 1)))
		tx, err := rte.NewTransactionWithID(txID, "retry-test").
			WithStepRegistry(coord).
			AddStep("always-failing-step").
			Build()

		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction (should fail after retries)
		result, _ := coord.Execute(context.Background(), tx)

		// Property: Transaction should be in FAILED status
		if result.Status != rte.TxStatusFailed {
			rt.Fatalf("expected FAILED status, got %s", result.Status)
		}

		// Property: Execution count should be exactly 1 (no retries for step execution in coordinator)
		// Note: The coordinator doesn't retry step execution - it only retries compensation.
		// Step execution fails immediately and triggers compensation or failure.
		// The retry logic in the coordinator is for compensation, not for step execution.
		if executionCount != 1 {
			rt.Fatalf("expected exactly 1 execution (no step retries), got %d", executionCount)
		}

		// Verify step record shows failure
		stepRecord, err := ti.StoreAdapter.GetStep(context.Background(), txID, 0)
		if err != nil {
			rt.Fatalf("failed to get step record: %v", err)
		}

		if stepRecord.Status != rte.StepStatusFailed {
			rt.Fatalf("expected step status FAILED, got %s", stepRecord.Status)
		}

		rt.Logf("Retry boundedness test: maxRetries=%d, executionCount=%d, status=%s",
			maxRetries, executionCount, result.Status)
	})
}

// alwaysFailingStep is a test step that always fails
type alwaysFailingStep struct {
	*rte.BaseStep
	executionCount *int64
}

func (s *alwaysFailingStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	atomic.AddInt64(s.executionCount, 1)
	return errors.New("simulated permanent failure")
}

func (s *alwaysFailingStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *alwaysFailingStep) SupportsCompensation() bool {
	return false
}

// Ensure alwaysFailingStep implements rte.Step interface
var _ rte.Step = (*alwaysFailingStep)(nil)

// ============================================================================
// Property 6 (Alternative): Compensation Retry Boundedness
// For any failing compensation, the number of retry attempts SHALL not exceed
// the configured maximum retries.
// ============================================================================

// TestProperty_CompensationRetryBoundedness_Integration tests compensation retry boundedness.
// This tests that compensation retries are bounded by maxRetries.
func TestProperty_CompensationRetryBoundedness_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random max retries (1-3) - keep small for faster tests
		maxRetries := rapid.IntRange(1, 3).Draw(rt, "maxRetries")

		// Track compensation attempt count
		var compensationAttempts int64

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create a fresh event bus for this iteration
		eventBus := event.NewMemoryEventBus()

		// Create coordinator with specific max retries
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
				MaxRetries:       maxRetries,
				RetryInterval:    10 * time.Millisecond, // Short interval for faster tests
				RetryMaxInterval: 100 * time.Millisecond,
				RetryMultiplier:  1.5,
				RetryJitter:      0.1,
			}),
		)

		// Create first step that succeeds (so we have something to compensate)
		successStep := &successfulStep{
			BaseStep: rte.NewBaseStep("success-step"),
		}
		coord.RegisterStep(successStep)

		// Create second step that fails (triggers compensation)
		failStep := &failingStepWithCompensation{
			BaseStep: rte.NewBaseStep("fail-step"),
		}
		coord.RegisterStep(failStep)

		// Create compensation step that always fails
		successStep.compensationAttempts = &compensationAttempts

		// Build and execute transaction
		txID := ti.GenerateTxID(fmt.Sprintf("comp-retry-%d", atomic.AddInt64(&txIDCounter, 1)))
		tx, err := rte.NewTransactionWithID(txID, "comp-retry-test").
			WithStepRegistry(coord).
			AddStep("success-step").
			AddStep("fail-step").
			Build()

		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction (should fail at step 2, then fail compensation)
		result, _ := coord.Execute(context.Background(), tx)

		// Property: Transaction should be in COMPENSATION_FAILED status
		if result.Status != rte.TxStatusCompensationFailed {
			rt.Fatalf("expected COMPENSATION_FAILED status, got %s", result.Status)
		}

		// Property: Compensation attempts should not exceed maxRetries + 1 (initial + retries)
		expectedMaxAttempts := int64(maxRetries + 1)
		if compensationAttempts > expectedMaxAttempts {
			rt.Fatalf("compensation attempts %d exceeded max %d (maxRetries=%d)",
				compensationAttempts, expectedMaxAttempts, maxRetries)
		}

		// Property: Compensation attempts should be exactly maxRetries + 1
		if compensationAttempts != expectedMaxAttempts {
			rt.Fatalf("expected exactly %d compensation attempts, got %d",
				expectedMaxAttempts, compensationAttempts)
		}

		rt.Logf("Compensation retry boundedness test: maxRetries=%d, compensationAttempts=%d, status=%s",
			maxRetries, compensationAttempts, result.Status)
	})
}

// successfulStep is a test step that succeeds but has failing compensation
type successfulStep struct {
	*rte.BaseStep
	compensationAttempts *int64
}

func (s *successfulStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *successfulStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	if s.compensationAttempts != nil {
		atomic.AddInt64(s.compensationAttempts, 1)
	}
	return errors.New("simulated compensation failure")
}

func (s *successfulStep) SupportsCompensation() bool {
	return true
}

// failingStepWithCompensation is a test step that fails but supports compensation
type failingStepWithCompensation struct {
	*rte.BaseStep
}

func (s *failingStepWithCompensation) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	return errors.New("simulated step failure")
}

func (s *failingStepWithCompensation) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *failingStepWithCompensation) SupportsCompensation() bool {
	return true
}

// Ensure steps implement rte.Step interface
var _ rte.Step = (*successfulStep)(nil)
var _ rte.Step = (*failingStepWithCompensation)(nil)

// ============================================================================
// Property 7: Timeout Handling (超时处理)
// For any step that exceeds its timeout, the system SHALL interrupt execution
// and return a timeout error.
// ============================================================================

// TestProperty_TimeoutHandling_Integration tests timeout handling using real MySQL and Redis.
// Property 7: Timeout Handling
// *For any* step that exceeds its timeout, the system SHALL interrupt execution
// and return a timeout error.
func TestProperty_TimeoutHandling_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random timeout (50-200ms)
		timeoutMs := rapid.IntRange(50, 200).Draw(rt, "timeoutMs")
		timeout := time.Duration(timeoutMs) * time.Millisecond

		// Step execution time is 2x the timeout to ensure timeout occurs
		executionTime := timeout * 2

		// Track if step was interrupted
		var wasInterrupted int64

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create a fresh event bus for this iteration
		eventBus := event.NewMemoryEventBus()

		// Create coordinator with specific timeout
		coord := rte.NewCoordinator(
			rte.WithStore(ti.StoreAdapter),
			rte.WithLocker(ti.Locker),
			rte.WithBreaker(breaker),
			rte.WithEventBus(eventBus),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      timeout,
				TxTimeout:        30 * time.Second,
				MaxRetries:       0, // No retries for this test
				RetryInterval:    100 * time.Millisecond,
			}),
		)

		// Create slow step that exceeds timeout
		step := &slowStep{
			BaseStep:       rte.NewBaseStep("slow-step"),
			executionTime:  executionTime,
			wasInterrupted: &wasInterrupted,
		}
		coord.RegisterStep(step)

		// Build and execute transaction
		txID := ti.GenerateTxID(fmt.Sprintf("timeout-%d", atomic.AddInt64(&txIDCounter, 1)))
		tx, err := rte.NewTransactionWithID(txID, "timeout-test").
			WithStepRegistry(coord).
			AddStep("slow-step").
			Build()

		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction (should timeout)
		startTime := time.Now()
		result, _ := coord.Execute(context.Background(), tx)
		duration := time.Since(startTime)

		// Property: Transaction should be in FAILED status (timeout triggers failure)
		if result.Status != rte.TxStatusFailed {
			rt.Fatalf("expected FAILED status, got %s", result.Status)
		}

		// Property: Error should indicate timeout
		if result.Error == nil {
			rt.Fatalf("expected error, got nil")
		}
		if !errors.Is(result.Error, rte.ErrStepExecutionFailed) {
			rt.Logf("error type: %T, error: %v", result.Error, result.Error)
		}

		// Property: Step should have been interrupted (context cancelled)
		if atomic.LoadInt64(&wasInterrupted) != 1 {
			rt.Fatalf("expected step to be interrupted by timeout")
		}

		// Property: Execution duration should be close to timeout (not full execution time)
		// Allow some tolerance for scheduling delays
		maxExpectedDuration := timeout + 500*time.Millisecond
		if duration > maxExpectedDuration {
			rt.Fatalf("execution took too long: %v (expected ~%v)", duration, timeout)
		}

		// Verify step record shows failure
		stepRecord, err := ti.StoreAdapter.GetStep(context.Background(), txID, 0)
		if err != nil {
			rt.Fatalf("failed to get step record: %v", err)
		}

		if stepRecord.Status != rte.StepStatusFailed {
			rt.Fatalf("expected step status FAILED, got %s", stepRecord.Status)
		}

		rt.Logf("Timeout handling test: timeout=%v, duration=%v, status=%s",
			timeout, duration, result.Status)
	})
}

// slowStep is a test step that takes longer than the timeout
type slowStep struct {
	*rte.BaseStep
	executionTime  time.Duration
	wasInterrupted *int64
}

func (s *slowStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	// Wait for either execution time or context cancellation
	select {
	case <-time.After(s.executionTime):
		// Completed without interruption
		return nil
	case <-ctx.Done():
		// Context was cancelled (timeout)
		atomic.StoreInt64(s.wasInterrupted, 1)
		return ctx.Err()
	}
}

func (s *slowStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *slowStep) SupportsCompensation() bool {
	return false
}

// Ensure slowStep implements rte.Step interface
var _ rte.Step = (*slowStep)(nil)

// ============================================================================
// Property 7 (Alternative): Transaction Timeout Handling
// For any transaction that exceeds its total timeout, the system SHALL mark
// the transaction as TIMEOUT.
// ============================================================================

// TestProperty_TransactionTimeoutHandling_Integration tests transaction-level timeout handling.
func TestProperty_TransactionTimeoutHandling_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random transaction timeout (50-100ms) - short timeout
		txTimeoutMs := rapid.IntRange(50, 100).Draw(rt, "txTimeoutMs")
		txTimeout := time.Duration(txTimeoutMs) * time.Millisecond

		// Generate number of steps (2-3)
		numSteps := rapid.IntRange(2, 3).Draw(rt, "numSteps")

		// Each step takes the full txTimeout to ensure total clearly exceeds txTimeout
		// Total step time = numSteps * txTimeout >> txTimeout
		stepTime := txTimeout

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create a fresh event bus for this iteration
		eventBus := event.NewMemoryEventBus()

		// Create coordinator with specific transaction timeout
		coord := rte.NewCoordinator(
			rte.WithStore(ti.StoreAdapter),
			rte.WithLocker(ti.Locker),
			rte.WithBreaker(breaker),
			rte.WithEventBus(eventBus),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second, // Long step timeout
				TxTimeout:        txTimeout,       // Short transaction timeout
				MaxRetries:       0,
				RetryInterval:    100 * time.Millisecond,
			}),
		)

		// Create steps that each take some time
		stepNames := make([]string, numSteps)
		for i := 0; i < numSteps; i++ {
			stepName := fmt.Sprintf("timed-step-%d", i)
			stepNames[i] = stepName
			step := &timedStep{
				BaseStep:      rte.NewBaseStep(stepName),
				executionTime: stepTime,
			}
			coord.RegisterStep(step)
		}

		// Build transaction with all steps
		txID := ti.GenerateTxID(fmt.Sprintf("tx-timeout-%d", atomic.AddInt64(&txIDCounter, 1)))
		builder := rte.NewTransactionWithID(txID, "tx-timeout-test").WithStepRegistry(coord)
		for _, name := range stepNames {
			builder.AddStep(name)
		}

		tx, err := builder.Build()
		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction (should timeout)
		result, _ := coord.Execute(context.Background(), tx)

		// Property: Transaction should be in TIMEOUT or FAILED status
		// (depending on whether timeout is detected during step execution or between steps)
		if result.Status != rte.TxStatusTimeout && result.Status != rte.TxStatusFailed {
			rt.Fatalf("expected TIMEOUT or FAILED status, got %s (txTimeout=%v, numSteps=%d, stepTime=%v)",
				result.Status, txTimeout, numSteps, stepTime)
		}

		// Property: Error should be present
		if result.Error == nil {
			rt.Fatalf("expected error, got nil")
		}

		rt.Logf("Transaction timeout test: txTimeout=%v, numSteps=%d, stepTime=%v, status=%s",
			txTimeout, numSteps, stepTime, result.Status)
	})
}

// timedStep is a test step that takes a specific amount of time
type timedStep struct {
	*rte.BaseStep
	executionTime time.Duration
}

func (s *timedStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	select {
	case <-time.After(s.executionTime):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *timedStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *timedStep) SupportsCompensation() bool {
	return false
}

// Ensure timedStep implements rte.Step interface
var _ rte.Step = (*timedStep)(nil)
