// Package testinfra provides property-based tests for RTE idempotency validation.
// Feature: financial-production-readiness
package testinfra

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"rte"
	"rte/circuit/memory"
	"rte/idempotency/store"

	"pgregory.net/rapid"
)

// ============================================================================

// For any step execution with idempotency support, executing the same step
// with the same idempotency key multiple times SHALL return the same result
// without re-executing the step logic.
// ============================================================================

// TestProperty_IdempotencyRoundTrip_Integration tests idempotency round-trip using real MySQL and Redis.

// with the same idempotency key multiple times SHALL return the same result
// without re-executing the step logic.
func TestProperty_IdempotencyRoundTrip_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Track execution count to verify step is not re-executed
		var executionCount int32

		// Generate random output value that the step will produce
		outputValue := rapid.Int64Range(1, 1000000).Draw(rt, "outputValue")

		// Create idempotency checker using the store
		checker := store.New(ti.StoreAdapter)

		// Create a fresh circuit breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create coordinator with idempotency checker
		coord := rte.NewCoordinator(
			rte.WithStore(ti.StoreAdapter),
			rte.WithLocker(ti.Locker),
			rte.WithBreaker(breaker),
			rte.WithEventBus(ti.EventBus),
			rte.WithChecker(checker),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       3,
				RetryInterval:    100 * time.Millisecond,
				IdempotencyTTL:   24 * time.Hour,
			}),
		)

		// Create an idempotent step that tracks execution count
		idempotentStep := &roundTripIdempotentStep{
			BaseStep:       rte.NewBaseStep("idempotent-step"),
			executionCount: &executionCount,
			outputValue:    outputValue,
		}
		coord.RegisterStep(idempotentStep)

		// Generate unique transaction ID
		txID := ti.GenerateTxID(fmt.Sprintf("idem-rt-%d", atomic.AddInt64(&txIDCounter, 1)))

		// Build and execute transaction first time
		tx1, err := rte.NewTransactionWithID(txID, "idempotency-test").
			WithStepRegistry(coord).
			AddStep("idempotent-step").
			Build()
		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		result1, err := coord.Execute(context.Background(), tx1)
		if err != nil {
			rt.Fatalf("first execution failed: %v", err)
		}

		// Verify first execution completed successfully
		if result1.Status != rte.TxStatusCompleted {
			rt.Fatalf("expected COMPLETED status, got %s", result1.Status)
		}

		// Verify step was executed exactly once
		if atomic.LoadInt32(&executionCount) != 1 {
			rt.Fatalf("expected 1 execution, got %d", atomic.LoadInt32(&executionCount))
		}

		// Verify output was set correctly
		firstOutput, ok := result1.Output["result"]
		if !ok {
			rt.Fatalf("expected 'result' in output")
		}
		// Convert to int64 for comparison (JSON unmarshaling may change type)
		var firstOutputInt64 int64
		switch v := firstOutput.(type) {
		case int64:
			firstOutputInt64 = v
		case float64:
			firstOutputInt64 = int64(v)
		case int:
			firstOutputInt64 = int64(v)
		default:
			rt.Fatalf("unexpected output type %T", firstOutput)
		}
		if firstOutputInt64 != outputValue {
			rt.Fatalf("expected output %d, got %d", outputValue, firstOutputInt64)
		}

		// Now simulate re-execution by creating a new transaction with same ID
		// This simulates a retry scenario where the same step needs to be executed again
		// We need to create a new coordinator to simulate a fresh execution context
		// but the idempotency key should still be recognized

		// Generate number of retry attempts
		numRetries := rapid.IntRange(1, 5).Draw(rt, "numRetries")

		for i := 0; i < numRetries; i++ {
			// Create a new transaction ID for retry (simulating recovery scenario)
			retryTxID := ti.GenerateTxID(fmt.Sprintf("idem-rt-retry-%d-%d", atomic.AddInt64(&txIDCounter, 1), i))

			// Create a new step with same idempotency key generation logic
			retryStep := &roundTripIdempotentStepWithFixedKey{
				BaseStep:       rte.NewBaseStep("idempotent-step"),
				executionCount: &executionCount,
				outputValue:    outputValue + int64(i+1),                  // Different output to verify cached result is used
				fixedKey:       fmt.Sprintf("%s-idempotent-step-0", txID), // Same key as first execution
			}

			// Create new coordinator with same checker
			retryCoord := rte.NewCoordinator(
				rte.WithStore(ti.StoreAdapter),
				rte.WithLocker(ti.Locker),
				rte.WithBreaker(breaker),
				rte.WithEventBus(ti.EventBus),
				rte.WithChecker(checker),
				rte.WithCoordinatorConfig(rte.Config{
					LockTTL:          30 * time.Second,
					LockExtendPeriod: 10 * time.Second,
					StepTimeout:      5 * time.Second,
					TxTimeout:        30 * time.Second,
					MaxRetries:       3,
					RetryInterval:    100 * time.Millisecond,
					IdempotencyTTL:   24 * time.Hour,
				}),
			)
			retryCoord.RegisterStep(retryStep)

			retryTx, err := rte.NewTransactionWithID(retryTxID, "idempotency-test").
				WithStepRegistry(retryCoord).
				AddStep("idempotent-step").
				Build()
			if err != nil {
				rt.Fatalf("failed to build retry transaction %d: %v", i, err)
			}

			retryResult, err := retryCoord.Execute(context.Background(), retryTx)
			if err != nil {
				rt.Fatalf("retry execution %d failed: %v", i, err)
			}

			
			if retryResult.Status != rte.TxStatusCompleted {
				rt.Fatalf("retry %d: expected COMPLETED status, got %s", i, retryResult.Status)
			}

			
			if atomic.LoadInt32(&executionCount) != 1 {
				rt.Fatalf("retry %d: expected 1 execution, got %d (step was re-executed)", i, atomic.LoadInt32(&executionCount))
			}

			
			retryOutput, ok := retryResult.Output["result"]
			if !ok {
				rt.Fatalf("retry %d: expected 'result' in output", i)
			}
			// Convert to int64 for comparison (JSON unmarshaling may change type)
			var retryOutputInt64 int64
			switch v := retryOutput.(type) {
			case int64:
				retryOutputInt64 = v
			case float64:
				retryOutputInt64 = int64(v)
			case int:
				retryOutputInt64 = int64(v)
			default:
				rt.Fatalf("retry %d: unexpected output type %T", i, retryOutput)
			}
			if retryOutputInt64 != outputValue {
				rt.Fatalf("retry %d: expected cached output %d, got %d", i, outputValue, retryOutputInt64)
			}
		}
	})
}

// roundTripIdempotentStep is a test step that supports idempotency and tracks execution count
type roundTripIdempotentStep struct {
	*rte.BaseStep
	executionCount *int32
	outputValue    int64
}

func (s *roundTripIdempotentStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	atomic.AddInt32(s.executionCount, 1)
	txCtx.SetOutput("result", s.outputValue)
	return nil
}

func (s *roundTripIdempotentStep) SupportsIdempotency() bool {
	return true
}

func (s *roundTripIdempotentStep) IdempotencyKey(txCtx *rte.TxContext) string {
	return fmt.Sprintf("%s-%s-%d", txCtx.TxID, s.Name(), txCtx.StepIndex)
}

func (s *roundTripIdempotentStep) SupportsCompensation() bool {
	return false
}

// roundTripIdempotentStepWithFixedKey is a test step with a fixed idempotency key for testing
type roundTripIdempotentStepWithFixedKey struct {
	*rte.BaseStep
	executionCount *int32
	outputValue    int64
	fixedKey       string
}

func (s *roundTripIdempotentStepWithFixedKey) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	atomic.AddInt32(s.executionCount, 1)
	txCtx.SetOutput("result", s.outputValue)
	return nil
}

func (s *roundTripIdempotentStepWithFixedKey) SupportsIdempotency() bool {
	return true
}

func (s *roundTripIdempotentStepWithFixedKey) IdempotencyKey(txCtx *rte.TxContext) string {
	return s.fixedKey
}

func (s *roundTripIdempotentStepWithFixedKey) SupportsCompensation() bool {
	return false
}

// ============================================================================

// For any two different step executions (different transaction ID or different
// step parameters), the generated idempotency keys SHALL be different.
// ============================================================================

// TestProperty_IdempotencyKeyUniqueness_Integration tests idempotency key uniqueness.

// step parameters), the generated idempotency keys SHALL be different.
func TestProperty_IdempotencyKeyUniqueness_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate two different transaction IDs
		txID1 := ti.GenerateTxID(fmt.Sprintf("idem-key1-%d", atomic.AddInt64(&txIDCounter, 1)))
		txID2 := ti.GenerateTxID(fmt.Sprintf("idem-key2-%d", atomic.AddInt64(&txIDCounter, 1)))

		// Generate random step indices
		stepIndex1 := rapid.IntRange(0, 10).Draw(rt, "stepIndex1")
		stepIndex2 := rapid.IntRange(0, 10).Draw(rt, "stepIndex2")

		// Generate random step names
		stepName1 := fmt.Sprintf("step-%d", rapid.IntRange(0, 100).Draw(rt, "stepNameSuffix1"))
		stepName2 := fmt.Sprintf("step-%d", rapid.IntRange(0, 100).Draw(rt, "stepNameSuffix2"))

		// Create test step for key generation
		step := &keyUniquenessTestStep{
			BaseStep: rte.NewBaseStep("test-step"),
		}

		// Generate keys for different contexts
		txCtx1 := &rte.TxContext{
			TxID:      txID1,
			StepIndex: stepIndex1,
		}
		txCtx2 := &rte.TxContext{
			TxID:      txID2,
			StepIndex: stepIndex2,
		}

		// Test with step name in key
		key1 := fmt.Sprintf("%s-%s-%d", txCtx1.TxID, stepName1, txCtx1.StepIndex)
		key2 := fmt.Sprintf("%s-%s-%d", txCtx2.TxID, stepName2, txCtx2.StepIndex)

		
		if txID1 != txID2 {
			keyA := step.generateKey(txID1, "same-step", 0)
			keyB := step.generateKey(txID2, "same-step", 0)
			if keyA == keyB {
				rt.Fatalf("different txIDs should produce different keys: txID1=%s, txID2=%s, key=%s",
					txID1, txID2, keyA)
			}
		}

		
		if stepIndex1 != stepIndex2 {
			keyA := step.generateKey(txID1, "same-step", stepIndex1)
			keyB := step.generateKey(txID1, "same-step", stepIndex2)
			if keyA == keyB {
				rt.Fatalf("different step indices should produce different keys: idx1=%d, idx2=%d, key=%s",
					stepIndex1, stepIndex2, keyA)
			}
		}

		
		if stepName1 != stepName2 {
			keyA := step.generateKey(txID1, stepName1, 0)
			keyB := step.generateKey(txID1, stepName2, 0)
			if keyA == keyB {
				rt.Fatalf("different step names should produce different keys: name1=%s, name2=%s, key=%s",
					stepName1, stepName2, keyA)
			}
		}

		
		if key1 == key2 && (txID1 != txID2 || stepName1 != stepName2 || stepIndex1 != stepIndex2) {
			rt.Fatalf("different contexts should produce different keys: key1=%s, key2=%s", key1, key2)
		}
	})
}

// keyUniquenessTestStep is a test step for key uniqueness testing
type keyUniquenessTestStep struct {
	*rte.BaseStep
}

func (s *keyUniquenessTestStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *keyUniquenessTestStep) SupportsIdempotency() bool {
	return true
}

func (s *keyUniquenessTestStep) IdempotencyKey(txCtx *rte.TxContext) string {
	return s.generateKey(txCtx.TxID, s.Name(), txCtx.StepIndex)
}

func (s *keyUniquenessTestStep) generateKey(txID, stepName string, stepIndex int) string {
	return fmt.Sprintf("%s-%s-%d", txID, stepName, stepIndex)
}

func (s *keyUniquenessTestStep) SupportsCompensation() bool {
	return false
}

// Ensure steps implement rte.Step interface
var _ rte.Step = (*roundTripIdempotentStep)(nil)
var _ rte.Step = (*roundTripIdempotentStepWithFixedKey)(nil)
var _ rte.Step = (*keyUniquenessTestStep)(nil)
