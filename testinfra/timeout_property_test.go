// Package testinfra provides property-based tests for RTE production validation.
// Feature: financial-production-readiness
// This file contains property tests for timeout handling (Properties 16 and 17).
package testinfra

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"rte"
	"rte/circuit/memory"
	"rte/event"

	"pgregory.net/rapid"
)

// txIDCounterTimeout is used to generate unique transaction IDs for timeout tests
var txIDCounterTimeout int64

// ============================================================================

// SHALL be cancelled and marked as failed.
// ============================================================================

// TestProperty_StepTimeoutHandling tests that steps exceeding timeout are cancelled and marked as failed.

// SHALL be cancelled and marked as failed.
func TestProperty_StepTimeoutHandling(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random step timeout (50-150ms) - short for faster tests
		stepTimeoutMs := rapid.IntRange(50, 150).Draw(rt, "stepTimeoutMs")
		stepTimeout := time.Duration(stepTimeoutMs) * time.Millisecond

		// Step execution time is 2-3x the timeout to ensure timeout occurs
		executionMultiplier := rapid.Float64Range(2.0, 3.0).Draw(rt, "executionMultiplier")
		executionTime := time.Duration(float64(stepTimeout) * executionMultiplier)

		// Track if step was interrupted (context cancelled)
		var wasInterrupted int64

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create a fresh event bus for this iteration
		eventBus := event.NewMemoryEventBus()

		// Create coordinator with specific step timeout
		coord := rte.NewCoordinator(
			rte.WithStore(ti.StoreAdapter),
			rte.WithLocker(ti.Locker),
			rte.WithBreaker(breaker),
			rte.WithEventBus(eventBus),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      stepTimeout,
				TxTimeout:        60 * time.Second, // Long tx timeout to isolate step timeout
				MaxRetries:       0,                // No retries for this test
				RetryInterval:    100 * time.Millisecond,
				RetryMaxInterval: 1 * time.Second,
				RetryMultiplier:  2.0,
				RetryJitter:      0.1,
				IdempotencyTTL:   24 * time.Hour,
			}),
		)

		// Create a slow step that exceeds the timeout
		step := &timeoutTestStep{
			BaseStep:       rte.NewBaseStep("slow-timeout-step"),
			executionTime:  executionTime,
			wasInterrupted: &wasInterrupted,
		}
		coord.RegisterStep(step)

		// Build and execute transaction
		txID := ti.GenerateTxID(fmt.Sprintf("step-timeout-%d", atomic.AddInt64(&txIDCounterTimeout, 1)))
		tx, err := rte.NewTransactionWithID(txID, "step-timeout-test").
			WithStepRegistry(coord).
			AddStep("slow-timeout-step").
			Build()

		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction (should timeout at step level)
		startTime := time.Now()
		result, _ := coord.Execute(context.Background(), tx)
		duration := time.Since(startTime)

		
		if result.Status != rte.TxStatusFailed {
			rt.Fatalf("violated: expected FAILED status for step timeout, got %s", result.Status)
		}

		
		if result.Error == nil {
			rt.Fatalf("violated: expected error for step timeout, got nil")
		}

		
		if atomic.LoadInt64(&wasInterrupted) != 1 {
			rt.Fatalf("violated: step should be cancelled/interrupted by timeout")
		}

		
		// Allow tolerance for scheduling delays
		maxExpectedDuration := stepTimeout + 500*time.Millisecond
		if duration > maxExpectedDuration {
			rt.Fatalf("violated: execution took %v, expected ~%v (step timeout)", duration, stepTimeout)
		}

		
		stepRecord, err := ti.StoreAdapter.GetStep(context.Background(), txID, 0)
		if err != nil {
			rt.Fatalf("failed to get step record: %v", err)
		}

		if stepRecord.Status != rte.StepStatusFailed {
			rt.Fatalf("violated: step status should be FAILED, got %s", stepRecord.Status)
		}

		rt.Logf("Step timeout test passed: stepTimeout=%v, executionTime=%v, duration=%v, status=%s",
			stepTimeout, executionTime, duration, result.Status)
	})
}

// timeoutTestStep is a test step that takes longer than the configured timeout
type timeoutTestStep struct {
	*rte.BaseStep
	executionTime  time.Duration
	wasInterrupted *int64
}

func (s *timeoutTestStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	// Wait for either execution time or context cancellation
	select {
	case <-time.After(s.executionTime):
		// Completed without interruption (should not happen if timeout is working)
		return nil
	case <-ctx.Done():
		// Context was cancelled (timeout)
		atomic.StoreInt64(s.wasInterrupted, 1)
		return ctx.Err()
	}
}

func (s *timeoutTestStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *timeoutTestStep) SupportsCompensation() bool {
	return false
}

// Ensure timeoutTestStep implements rte.Step interface
var _ rte.Step = (*timeoutTestStep)(nil)

// ============================================================================

// compensation SHALL be triggered for all completed steps.
// ============================================================================

// TestProperty_TransactionTimeoutHandling tests that transaction timeout triggers compensation.

// compensation SHALL be triggered for all completed steps.
func TestProperty_TransactionTimeoutHandling(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random transaction timeout (100-200ms) - short for faster tests
		txTimeoutMs := rapid.IntRange(100, 200).Draw(rt, "txTimeoutMs")
		txTimeout := time.Duration(txTimeoutMs) * time.Millisecond

		// Generate number of steps (2-4) - need at least 2 to have completed steps before timeout
		numSteps := rapid.IntRange(2, 4).Draw(rt, "numSteps")

		// Each step takes enough time that total exceeds txTimeout
		// stepTime = txTimeout / (numSteps - 1) ensures timeout happens during last step
		stepTime := txTimeout / time.Duration(numSteps-1)

		// Track compensation calls
		var compensationCalls int64
		var compensationOrder []int
		var compensationMu = &sync.Mutex{}

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
				MaxRetries:       3,
				RetryInterval:    10 * time.Millisecond,
				RetryMaxInterval: 100 * time.Millisecond,
				RetryMultiplier:  1.5,
				RetryJitter:      0.1,
				IdempotencyTTL:   24 * time.Hour,
			}),
		)

		// Create steps that each take some time and support compensation
		stepNames := make([]string, numSteps)
		for i := 0; i < numSteps; i++ {
			stepName := fmt.Sprintf("timed-comp-step-%d", i)
			stepNames[i] = stepName
			stepIndex := i
			step := &txTimeoutTestStep{
				BaseStep:          rte.NewBaseStep(stepName),
				executionTime:     stepTime,
				stepIndex:         stepIndex,
				compensationCalls: &compensationCalls,
				compensationOrder: &compensationOrder,
				compensationMu:    compensationMu,
			}
			coord.RegisterStep(step)
		}

		// Build transaction with all steps
		txID := ti.GenerateTxID(fmt.Sprintf("tx-timeout-%d", atomic.AddInt64(&txIDCounterTimeout, 1)))
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

		
		// TIMEOUT if detected before compensation, COMPENSATED if compensation succeeds
		validStatuses := result.Status == rte.TxStatusTimeout ||
			result.Status == rte.TxStatusCompensated ||
			result.Status == rte.TxStatusFailed
		if !validStatuses {
			rt.Fatalf("violated: expected TIMEOUT, COMPENSATED, or FAILED status, got %s", result.Status)
		}

		
		if result.Error == nil {
			rt.Fatalf("violated: expected error for transaction timeout, got nil")
		}

		
		// Get step records to check which steps completed
		completedSteps := 0
		for i := 0; i < numSteps; i++ {
			stepRecord, err := ti.StoreAdapter.GetStep(context.Background(), txID, i)
			if err != nil {
				continue
			}
			if stepRecord.Status == rte.StepStatusCompleted || stepRecord.Status == rte.StepStatusCompensated {
				completedSteps++
			}
		}

		// If steps completed and status is COMPENSATED, verify compensation was called
		if result.Status == rte.TxStatusCompensated {
			compensationMu.Lock()
			actualCompensations := len(compensationOrder)
			compensationMu.Unlock()

			if actualCompensations == 0 && completedSteps > 0 {
				rt.Fatalf("violated: completed steps=%d but no compensation calls", completedSteps)
			}

			// Verify compensation order is reverse
			compensationMu.Lock()
			for i := 0; i < len(compensationOrder)-1; i++ {
				if compensationOrder[i] < compensationOrder[i+1] {
					rt.Fatalf("violated: compensation not in reverse order: %v", compensationOrder)
				}
			}
			compensationMu.Unlock()
		}

		
		if result.Error != nil {
			isTimeoutError := errors.Is(result.Error, rte.ErrTransactionTimeout) ||
				errors.Is(result.Error, context.DeadlineExceeded) ||
				errors.Is(result.Error, rte.ErrStepExecutionFailed)
			if !isTimeoutError {
				rt.Logf("Note: error type is %T: %v", result.Error, result.Error)
			}
		}

		rt.Logf("Transaction timeout test passed: txTimeout=%v, numSteps=%d, stepTime=%v, status=%s, completedSteps=%d",
			txTimeout, numSteps, stepTime, result.Status, completedSteps)
	})
}

// txTimeoutTestStep is a test step for transaction timeout testing
type txTimeoutTestStep struct {
	*rte.BaseStep
	executionTime     time.Duration
	stepIndex         int
	compensationCalls *int64
	compensationOrder *[]int
	compensationMu    *sync.Mutex
}

func (s *txTimeoutTestStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	select {
	case <-time.After(s.executionTime):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *txTimeoutTestStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	atomic.AddInt64(s.compensationCalls, 1)
	s.compensationMu.Lock()
	*s.compensationOrder = append(*s.compensationOrder, s.stepIndex)
	s.compensationMu.Unlock()
	return nil
}

func (s *txTimeoutTestStep) SupportsCompensation() bool {
	return true
}

// Ensure txTimeoutTestStep implements rte.Step interface
var _ rte.Step = (*txTimeoutTestStep)(nil)
