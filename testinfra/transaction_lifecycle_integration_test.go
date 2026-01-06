// Package testinfra provides integration tests for complete transaction lifecycle.
// These tests validate creation, execution, completion, failure, and compensation flows.
package testinfra

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"rte"
	"rte/event"
)

// ============================================================================
// Transaction Lifecycle Integration Tests
// ============================================================================

// testStep is a simple step implementation for testing
type testStep struct {
	*rte.BaseStep
	executeFn    func(ctx context.Context, txCtx *rte.TxContext) error
	compensateFn func(ctx context.Context, txCtx *rte.TxContext) error
	supportsComp bool
}

func newTestStep(name string) *testStep {
	return &testStep{
		BaseStep: rte.NewBaseStep(name),
		executeFn: func(ctx context.Context, txCtx *rte.TxContext) error {
			return nil
		},
	}
}

func (s *testStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	if s.executeFn != nil {
		return s.executeFn(ctx, txCtx)
	}
	return nil
}

func (s *testStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	if s.compensateFn != nil {
		return s.compensateFn(ctx, txCtx)
	}
	return nil
}

func (s *testStep) SupportsCompensation() bool {
	return s.supportsComp
}

func (s *testStep) WithExecute(fn func(ctx context.Context, txCtx *rte.TxContext) error) *testStep {
	s.executeFn = fn
	return s
}

func (s *testStep) WithCompensate(fn func(ctx context.Context, txCtx *rte.TxContext) error) *testStep {
	s.compensateFn = fn
	s.supportsComp = true
	return s
}

// TestIntegration_TransactionLifecycle_Success tests successful transaction execution
func TestIntegration_TransactionLifecycle_Success(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Single_Step_Success", func(t *testing.T) {
		// Register step
		step1 := newTestStep("single-step").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			txCtx.SetOutput("result", "success")
			return nil
		})
		ti.Engine.RegisterStep(step1)

		// Create and execute transaction
		txID := ti.GenerateTxID("single-success")
		tx, err := ti.Engine.NewTransactionWithID(txID, "test_single").
			AddStep("single-step").
			WithInputValue("test_key", "test_value").
			Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		result, err := ti.Engine.Execute(ctx, tx)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		if result.Status != rte.TxStatusCompleted {
			t.Errorf("Expected status COMPLETED, got %s", result.Status)
		}

		// Verify in database
		storedTx, err := ti.StoreAdapter.GetTransaction(ctx, txID)
		if err != nil {
			t.Fatalf("GetTransaction failed: %v", err)
		}

		if storedTx.Status != rte.TxStatusCompleted {
			t.Errorf("Stored status should be COMPLETED, got %s", storedTx.Status)
		}
		if storedTx.CompletedAt == nil {
			t.Error("CompletedAt should be set")
		}
	})

	t.Run("Multi_Step_Success", func(t *testing.T) {
		// Register steps
		var executionOrder []string
		step1 := newTestStep("multi-step-1").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			executionOrder = append(executionOrder, "step1")
			txCtx.SetOutput("step1_result", "done")
			return nil
		})
		step2 := newTestStep("multi-step-2").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			executionOrder = append(executionOrder, "step2")
			txCtx.SetOutput("step2_result", "done")
			return nil
		})
		step3 := newTestStep("multi-step-3").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			executionOrder = append(executionOrder, "step3")
			txCtx.SetOutput("step3_result", "done")
			return nil
		})

		ti.Engine.RegisterStep(step1)
		ti.Engine.RegisterStep(step2)
		ti.Engine.RegisterStep(step3)

		// Create and execute transaction
		txID := ti.GenerateTxID("multi-success")
		tx, err := ti.Engine.NewTransactionWithID(txID, "test_multi").
			AddSteps("multi-step-1", "multi-step-2", "multi-step-3").
			Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		result, err := ti.Engine.Execute(ctx, tx)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		if result.Status != rte.TxStatusCompleted {
			t.Errorf("Expected status COMPLETED, got %s", result.Status)
		}

		// Verify execution order
		expectedOrder := []string{"step1", "step2", "step3"}
		if len(executionOrder) != len(expectedOrder) {
			t.Errorf("Expected %d steps executed, got %d", len(expectedOrder), len(executionOrder))
		}
		for i, step := range expectedOrder {
			if i < len(executionOrder) && executionOrder[i] != step {
				t.Errorf("Step %d: expected %s, got %s", i, step, executionOrder[i])
			}
		}

		// Verify all steps in database
		steps, err := ti.StoreAdapter.GetSteps(ctx, txID)
		if err != nil {
			t.Fatalf("GetSteps failed: %v", err)
		}

		if len(steps) != 3 {
			t.Errorf("Expected 3 steps, got %d", len(steps))
		}

		for _, step := range steps {
			if step.Status != rte.StepStatusCompleted {
				t.Errorf("Step %s should be COMPLETED, got %s", step.StepName, step.Status)
			}
		}
	})

	t.Run("With_Lock_Keys", func(t *testing.T) {
		step := newTestStep("locked-step").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			return nil
		})
		ti.Engine.RegisterStep(step)

		txID := ti.GenerateTxID("locked-success")
		lockKey := ti.GenerateTxID("lock-key")

		tx, err := ti.Engine.NewTransactionWithID(txID, "test_locked").
			AddStep("locked-step").
			WithLockKeys(lockKey).
			Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		result, err := ti.Engine.Execute(ctx, tx)
		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}

		if result.Status != rte.TxStatusCompleted {
			t.Errorf("Expected status COMPLETED, got %s", result.Status)
		}

		// Verify lock was released (can acquire again)
		handle, err := ti.Locker.Acquire(ctx, []string{lockKey}, 5*time.Second)
		if err != nil {
			t.Errorf("Lock should be released after transaction: %v", err)
		} else {
			handle.Release(ctx)
		}
	})
}

// TestIntegration_TransactionLifecycle_Failure tests transaction failure handling
func TestIntegration_TransactionLifecycle_Failure(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Step_Failure_No_Compensation", func(t *testing.T) {
		// Register step that fails
		step := newTestStep("fail-step").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			return errors.New("intentional failure")
		})
		ti.Engine.RegisterStep(step)

		txID := ti.GenerateTxID("fail-no-comp")
		tx, err := ti.Engine.NewTransactionWithID(txID, "test_fail").
			AddStep("fail-step").
			Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		result, err := ti.Engine.Execute(ctx, tx)
		if err == nil {
			t.Error("Expected error from failed step")
		}

		if result.Status != rte.TxStatusFailed {
			t.Errorf("Expected status FAILED, got %s", result.Status)
		}

		// Verify in database
		storedTx, err := ti.StoreAdapter.GetTransaction(ctx, txID)
		if err != nil {
			t.Fatalf("GetTransaction failed: %v", err)
		}

		if storedTx.Status != rte.TxStatusFailed {
			t.Errorf("Stored status should be FAILED, got %s", storedTx.Status)
		}
		if storedTx.ErrorMsg == "" {
			t.Error("ErrorMsg should be set")
		}
	})

	t.Run("Middle_Step_Failure", func(t *testing.T) {
		var executedSteps []string

		step1 := newTestStep("mid-step-1").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			executedSteps = append(executedSteps, "step1")
			return nil
		})
		step2 := newTestStep("mid-step-2").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			executedSteps = append(executedSteps, "step2")
			return errors.New("step2 failed")
		})
		step3 := newTestStep("mid-step-3").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			executedSteps = append(executedSteps, "step3")
			return nil
		})

		ti.Engine.RegisterStep(step1)
		ti.Engine.RegisterStep(step2)
		ti.Engine.RegisterStep(step3)

		txID := ti.GenerateTxID("mid-fail")
		tx, err := ti.Engine.NewTransactionWithID(txID, "test_mid_fail").
			AddSteps("mid-step-1", "mid-step-2", "mid-step-3").
			Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		result, _ := ti.Engine.Execute(ctx, tx)

		if result.Status != rte.TxStatusFailed {
			t.Errorf("Expected status FAILED, got %s", result.Status)
		}

		// Step 3 should not have executed
		if len(executedSteps) != 2 {
			t.Errorf("Expected 2 steps executed, got %d: %v", len(executedSteps), executedSteps)
		}

		// Verify step statuses in database
		steps, err := ti.StoreAdapter.GetSteps(ctx, txID)
		if err != nil {
			t.Fatalf("GetSteps failed: %v", err)
		}

		for _, step := range steps {
			switch step.StepIndex {
			case 0:
				if step.Status != rte.StepStatusCompleted {
					t.Errorf("Step 0 should be COMPLETED, got %s", step.Status)
				}
			case 1:
				if step.Status != rte.StepStatusFailed {
					t.Errorf("Step 1 should be FAILED, got %s", step.Status)
				}
			case 2:
				if step.Status != rte.StepStatusPending {
					t.Errorf("Step 2 should be PENDING, got %s", step.Status)
				}
			}
		}
	})
}

// TestIntegration_TransactionLifecycle_Compensation tests compensation flow
func TestIntegration_TransactionLifecycle_Compensation(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Compensation_On_Failure", func(t *testing.T) {
		var compensationOrder []string

		step1 := newTestStep("comp-step-1").
			WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
				return nil
			}).
			WithCompensate(func(ctx context.Context, txCtx *rte.TxContext) error {
				compensationOrder = append(compensationOrder, "comp1")
				return nil
			})

		step2 := newTestStep("comp-step-2").
			WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
				return nil
			}).
			WithCompensate(func(ctx context.Context, txCtx *rte.TxContext) error {
				compensationOrder = append(compensationOrder, "comp2")
				return nil
			})

		step3 := newTestStep("comp-step-3").
			WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
				return errors.New("step3 failed")
			})

		ti.Engine.RegisterStep(step1)
		ti.Engine.RegisterStep(step2)
		ti.Engine.RegisterStep(step3)

		txID := ti.GenerateTxID("comp-flow")
		tx, err := ti.Engine.NewTransactionWithID(txID, "test_comp").
			AddSteps("comp-step-1", "comp-step-2", "comp-step-3").
			Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		result, _ := ti.Engine.Execute(ctx, tx)

		if result.Status != rte.TxStatusCompensated {
			t.Errorf("Expected status COMPENSATED, got %s", result.Status)
		}

		// Verify compensation order (reverse)
		expectedOrder := []string{"comp2", "comp1"}
		if len(compensationOrder) != len(expectedOrder) {
			t.Errorf("Expected %d compensations, got %d", len(expectedOrder), len(compensationOrder))
		}
		for i, comp := range expectedOrder {
			if i < len(compensationOrder) && compensationOrder[i] != comp {
				t.Errorf("Compensation %d: expected %s, got %s", i, comp, compensationOrder[i])
			}
		}

		// Verify step statuses
		steps, err := ti.StoreAdapter.GetSteps(ctx, txID)
		if err != nil {
			t.Fatalf("GetSteps failed: %v", err)
		}

		for _, step := range steps {
			switch step.StepIndex {
			case 0, 1:
				if step.Status != rte.StepStatusCompensated {
					t.Errorf("Step %d should be COMPENSATED, got %s", step.StepIndex, step.Status)
				}
			case 2:
				if step.Status != rte.StepStatusFailed {
					t.Errorf("Step 2 should be FAILED, got %s", step.Status)
				}
			}
		}
	})

	t.Run("Compensation_Failure", func(t *testing.T) {
		step1 := newTestStep("comp-fail-1").
			WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
				return nil
			}).
			WithCompensate(func(ctx context.Context, txCtx *rte.TxContext) error {
				return errors.New("compensation failed")
			})

		step2 := newTestStep("comp-fail-2").
			WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
				return errors.New("step2 failed")
			})

		ti.Engine.RegisterStep(step1)
		ti.Engine.RegisterStep(step2)

		txID := ti.GenerateTxID("comp-fail")
		tx, err := ti.Engine.NewTransactionWithID(txID, "test_comp_fail").
			AddSteps("comp-fail-1", "comp-fail-2").
			WithMaxRetries(0). // No retries for faster test
			Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}

		result, _ := ti.Engine.Execute(ctx, tx)

		if result.Status != rte.TxStatusCompensationFailed {
			t.Errorf("Expected status COMPENSATION_FAILED, got %s", result.Status)
		}

		// Verify in database
		storedTx, err := ti.StoreAdapter.GetTransaction(ctx, txID)
		if err != nil {
			t.Fatalf("GetTransaction failed: %v", err)
		}

		if storedTx.Status != rte.TxStatusCompensationFailed {
			t.Errorf("Stored status should be COMPENSATION_FAILED, got %s", storedTx.Status)
		}
	})
}

// TestIntegration_TransactionLifecycle_Events tests event publication
func TestIntegration_TransactionLifecycle_Events(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Success_Events", func(t *testing.T) {
		var events []event.Event
		ti.EventBus.SubscribeAll(func(ctx context.Context, e event.Event) error {
			events = append(events, e)
			return nil
		})

		step := newTestStep("event-step").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			return nil
		})
		ti.Engine.RegisterStep(step)

		txID := ti.GenerateTxID("event-success")
		tx, _ := ti.Engine.NewTransactionWithID(txID, "test_events").
			AddStep("event-step").
			Build()

		ti.Engine.Execute(ctx, tx)

		// Verify events
		eventTypes := make(map[event.EventType]bool)
		for _, e := range events {
			eventTypes[e.Type] = true
		}

		expectedEvents := []event.EventType{
			event.EventTxCreated,
			event.EventStepStarted,
			event.EventStepCompleted,
			event.EventTxCompleted,
		}

		for _, et := range expectedEvents {
			if !eventTypes[et] {
				t.Errorf("Expected event %s not found", et)
			}
		}
	})

	t.Run("Failure_Events", func(t *testing.T) {
		var events []event.Event
		ti.EventBus.SubscribeAll(func(ctx context.Context, e event.Event) error {
			events = append(events, e)
			return nil
		})

		step := newTestStep("event-fail-step").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			return errors.New("failed")
		})
		ti.Engine.RegisterStep(step)

		txID := ti.GenerateTxID("event-fail")
		tx, _ := ti.Engine.NewTransactionWithID(txID, "test_events_fail").
			AddStep("event-fail-step").
			Build()

		ti.Engine.Execute(ctx, tx)

		// Verify failure events
		eventTypes := make(map[event.EventType]bool)
		for _, e := range events {
			eventTypes[e.Type] = true
		}

		if !eventTypes[event.EventStepFailed] {
			t.Error("Expected EventStepFailed not found")
		}
		if !eventTypes[event.EventTxFailed] {
			t.Error("Expected EventTxFailed not found")
		}
	})
}

// TestIntegration_TransactionLifecycle_StateTransitions tests state transitions
func TestIntegration_TransactionLifecycle_StateTransitions(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Success_State_Transitions", func(t *testing.T) {
		var stateHistory []rte.TxStatus

		// Subscribe to events to track state changes
		ti.EventBus.SubscribeAll(func(ctx context.Context, e event.Event) error {
			txID := e.TxID
			if txID != "" {
				storedTx, err := ti.StoreAdapter.GetTransaction(ctx, txID)
				if err == nil {
					stateHistory = append(stateHistory, storedTx.Status)
				}
			}
			return nil
		})

		step := newTestStep("state-step").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			return nil
		})
		ti.Engine.RegisterStep(step)

		txID := ti.GenerateTxID("state-trans")
		tx, _ := ti.Engine.NewTransactionWithID(txID, "test_states").
			AddStep("state-step").
			Build()

		ti.Engine.Execute(ctx, tx)

		// Final state should be COMPLETED
		storedTx, _ := ti.StoreAdapter.GetTransaction(ctx, txID)
		if storedTx.Status != rte.TxStatusCompleted {
			t.Errorf("Final status should be COMPLETED, got %s", storedTx.Status)
		}
	})
}

// TestIntegration_TransactionLifecycle_Concurrent tests concurrent transaction execution
func TestIntegration_TransactionLifecycle_Concurrent(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Concurrent_Independent_Transactions", func(t *testing.T) {
		var executionCount int32

		step := newTestStep("concurrent-step").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			atomic.AddInt32(&executionCount, 1)
			time.Sleep(10 * time.Millisecond) // Simulate work
			return nil
		})
		ti.Engine.RegisterStep(step)

		const numTransactions = 10
		results := make(chan *rte.TxResult, numTransactions)
		errs := make(chan error, numTransactions)

		for i := 0; i < numTransactions; i++ {
			go func(idx int) {
				txID := ti.GenerateTxID(fmt.Sprintf("concurrent-%d", idx))
				tx, err := ti.Engine.NewTransactionWithID(txID, "test_concurrent").
					AddStep("concurrent-step").
					Build()
				if err != nil {
					errs <- err
					return
				}

				result, err := ti.Engine.Execute(ctx, tx)
				if err != nil {
					errs <- err
					return
				}
				results <- result
			}(i)
		}

		// Collect results
		successCount := 0
		for i := 0; i < numTransactions; i++ {
			select {
			case result := <-results:
				if result.Status == rte.TxStatusCompleted {
					successCount++
				}
			case err := <-errs:
				t.Errorf("Transaction error: %v", err)
			case <-time.After(30 * time.Second):
				t.Fatal("Timeout waiting for transactions")
			}
		}

		if successCount != numTransactions {
			t.Errorf("Expected %d successful transactions, got %d", numTransactions, successCount)
		}

		if int(executionCount) != numTransactions {
			t.Errorf("Expected %d executions, got %d", numTransactions, executionCount)
		}
	})

	t.Run("Concurrent_Same_Lock_Key", func(t *testing.T) {
		var executionCount int32

		step := newTestStep("locked-concurrent-step").WithExecute(func(ctx context.Context, txCtx *rte.TxContext) error {
			atomic.AddInt32(&executionCount, 1)
			time.Sleep(50 * time.Millisecond) // Hold lock for a bit
			return nil
		})
		ti.Engine.RegisterStep(step)

		sharedLockKey := ti.GenerateTxID("shared-lock")
		const numTransactions = 5
		results := make(chan *rte.TxResult, numTransactions)
		errs := make(chan error, numTransactions)

		for i := 0; i < numTransactions; i++ {
			go func(idx int) {
				txID := ti.GenerateTxID(fmt.Sprintf("locked-concurrent-%d", idx))
				tx, err := ti.Engine.NewTransactionWithID(txID, "test_locked_concurrent").
					AddStep("locked-concurrent-step").
					WithLockKeys(sharedLockKey).
					Build()
				if err != nil {
					errs <- err
					return
				}

				result, err := ti.Engine.Execute(ctx, tx)
				if err != nil {
					errs <- err
					return
				}
				results <- result
			}(i)
		}

		// Collect results - some may fail due to lock contention
		successCount := 0
		failCount := 0
		for i := 0; i < numTransactions; i++ {
			select {
			case result := <-results:
				if result.Status == rte.TxStatusCompleted {
					successCount++
				} else {
					failCount++
				}
			case <-errs:
				failCount++
			case <-time.After(30 * time.Second):
				t.Fatal("Timeout waiting for transactions")
			}
		}

		// At least one should succeed
		if successCount == 0 {
			t.Error("At least one transaction should succeed")
		}

		t.Logf("Success: %d, Failed: %d", successCount, failCount)
	})
}
