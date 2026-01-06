// Package testinfra provides property-based tests for RTE production validation.
// Feature: rte-production-validation
package testinfra

import (
	"context"
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

// Global counter for unique transaction IDs across test iterations
var txIDCounter int64

// ============================================================================
// Property 1: Balance Conservation (余额守恒)
// For any set of accounts and for any sequence of transfer transactions
// (successful, failed, or compensated), the total balance across all accounts
// SHALL remain constant.
// ============================================================================

// TestProperty_BalanceConservation_Integration tests balance conservation using real MySQL and Redis.
// Property 1: Balance Conservation
// *For any* set of accounts and *for any* sequence of transfer transactions,
// the total balance across all accounts SHALL remain constant.
func TestProperty_BalanceConservation_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Create a fresh account store for this test iteration
		accountStore := NewMockAccountStore()

		// Generate random number of accounts (2-5)
		numAccounts := rapid.IntRange(2, 5).Draw(rt, "numAccounts")

		// Generate random initial balances for each account
		accounts := make([]*Account, numAccounts)
		for i := 0; i < numAccounts; i++ {
			balance := rapid.Int64Range(1000, 100000).Draw(rt, fmt.Sprintf("balance_%d", i))
			accounts[i] = accountStore.CreateAccount(fmt.Sprintf("acc-%d", i), balance)
		}

		// Record initial total balance
		initialTotal := accountStore.TotalBalance()

		// Generate random transfer scenario
		// 0 = success, 1 = failure at step 1, 2 = failure at step 2
		scenario := rapid.IntRange(0, 2).Draw(rt, "scenario")

		// Pick random source and destination accounts
		fromIdx := rapid.IntRange(0, numAccounts-1).Draw(rt, "fromIdx")
		toIdx := rapid.IntRange(0, numAccounts-1).Draw(rt, "toIdx")
		if toIdx == fromIdx {
			toIdx = (toIdx + 1) % numAccounts
		}

		fromAccount := accounts[fromIdx]
		toAccount := accounts[toIdx]

		// Generate transfer amount (must be <= source balance)
		maxAmount := fromAccount.GetBalance()
		if maxAmount < 1 {
			maxAmount = 1
		}
		amount := rapid.Int64Range(1, maxAmount).Draw(rt, "amount")

		// Create coordinator with test steps
		coord := rte.NewCoordinator(
			rte.WithStore(ti.StoreAdapter),
			rte.WithLocker(ti.Locker),
			rte.WithBreaker(ti.Breaker),
			rte.WithEventBus(ti.EventBus),
			rte.WithCoordinatorConfig(rte.Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       3,
				RetryInterval:    100 * time.Millisecond,
			}),
		)

		// Register transfer steps
		debitStep := &balanceConservationDebitStep{
			BaseStep:     rte.NewBaseStep("debit"),
			accountStore: accountStore,
			failAtStep:   scenario == 1,
		}
		creditStep := &balanceConservationCreditStep{
			BaseStep:     rte.NewBaseStep("credit"),
			accountStore: accountStore,
			failAtStep:   scenario == 2,
		}

		coord.RegisterStep(debitStep)
		coord.RegisterStep(creditStep)

		// Build and execute transaction
		txID := ti.GenerateTxID(fmt.Sprintf("balance-%d", atomic.AddInt64(&txIDCounter, 1)))
		tx, err := rte.NewTransactionWithID(txID, "transfer").
			WithStepRegistry(coord).
			WithLockKeys(fmt.Sprintf("account:%s", fromAccount.ID), fmt.Sprintf("account:%s", toAccount.ID)).
			WithInput(map[string]any{
				"from_account_id": fromAccount.ID,
				"to_account_id":   toAccount.ID,
				"amount":          amount,
			}).
			AddStep("debit").
			AddStep("credit").
			Build()

		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction
		_, _ = coord.Execute(context.Background(), tx)

		// Property: Total balance must be conserved regardless of outcome
		finalTotal := accountStore.TotalBalance()
		if finalTotal != initialTotal {
			rt.Fatalf("Balance conservation violated: initial=%d, final=%d, scenario=%d",
				initialTotal, finalTotal, scenario)
		}
	})
}

// balanceConservationDebitStep is a test step that debits from an account
type balanceConservationDebitStep struct {
	*rte.BaseStep
	accountStore *MockAccountStore
	failAtStep   bool
}

func (s *balanceConservationDebitStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	if s.failAtStep {
		return fmt.Errorf("simulated debit failure")
	}

	fromID := txCtx.Input["from_account_id"].(string)
	amount := txCtx.Input["amount"].(int64)

	acc, ok := s.accountStore.GetAccount(fromID)
	if !ok {
		return fmt.Errorf("account not found: %s", fromID)
	}

	if err := acc.Debit(amount); err != nil {
		return err
	}

	txCtx.SetOutput("debited_amount", amount)
	return nil
}

func (s *balanceConservationDebitStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	fromID := txCtx.Input["from_account_id"].(string)
	amount := txCtx.Input["amount"].(int64)

	acc, ok := s.accountStore.GetAccount(fromID)
	if !ok {
		return fmt.Errorf("account not found: %s", fromID)
	}

	acc.Credit(amount)
	return nil
}

func (s *balanceConservationDebitStep) SupportsCompensation() bool {
	return true
}

// balanceConservationCreditStep is a test step that credits to an account
type balanceConservationCreditStep struct {
	*rte.BaseStep
	accountStore *MockAccountStore
	failAtStep   bool
}

func (s *balanceConservationCreditStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	if s.failAtStep {
		return fmt.Errorf("simulated credit failure")
	}

	toID := txCtx.Input["to_account_id"].(string)
	amount := txCtx.Input["amount"].(int64)

	acc, ok := s.accountStore.GetAccount(toID)
	if !ok {
		return fmt.Errorf("account not found: %s", toID)
	}

	acc.Credit(amount)
	txCtx.SetOutput("credited_amount", amount)
	return nil
}

func (s *balanceConservationCreditStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	toID := txCtx.Input["to_account_id"].(string)
	amount := txCtx.Input["amount"].(int64)

	acc, ok := s.accountStore.GetAccount(toID)
	if !ok {
		return fmt.Errorf("account not found: %s", toID)
	}

	if err := acc.Debit(amount); err != nil {
		return err
	}
	return nil
}

func (s *balanceConservationCreditStep) SupportsCompensation() bool {
	return true
}

// ============================================================================
// Property 2: Compensation Completeness (补偿完整性)
// For any transaction that fails at step N, for all completed steps 0 to N-1
// that support compensation, the compensation operations SHALL be executed
// in reverse order exactly once.
// ============================================================================

// TestProperty_CompensationCompleteness_Integration tests compensation completeness using real MySQL and Redis.
// Property 2: Compensation Completeness
// *For any* transaction that fails at step N, *for all* completed steps 0 to N-1
// that support compensation, the compensation operations SHALL be executed in reverse order exactly once.
func TestProperty_CompensationCompleteness_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random number of steps (at least 2 to have something to compensate)
		numSteps := rapid.IntRange(2, 5).Draw(rt, "numSteps")
		// Fail at step 1 or later (so at least step 0 completes and needs compensation)
		failAtStep := rapid.IntRange(1, numSteps-1).Draw(rt, "failAtStep")

		// Track compensation order
		var compensationMu sync.Mutex
		compensationOrder := make([]int, 0)

		// Create a fresh circuit breaker for this iteration to avoid state leakage
		breaker := memory.NewMemoryBreaker()

		// Create coordinator with fresh breaker
		coord := rte.NewCoordinator(
			rte.WithStore(ti.StoreAdapter),
			rte.WithLocker(ti.Locker),
			rte.WithBreaker(breaker),
			rte.WithEventBus(ti.EventBus),
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
			stepNames[i] = fmt.Sprintf("step-%d", i)
		}

		// Register steps with compensation support
		for i := 0; i < numSteps; i++ {
			stepIndex := i
			step := &compensationTestStep{
				BaseStep:          rte.NewBaseStep(stepNames[i]),
				stepIndex:         stepIndex,
				failAtStep:        failAtStep,
				compensationOrder: &compensationOrder,
				compensationMu:    &compensationMu,
			}
			coord.RegisterStep(step)
		}

		// Build transaction with all steps
		txID := ti.GenerateTxID(fmt.Sprintf("comp-%d", atomic.AddInt64(&txIDCounter, 1)))
		builder := rte.NewTransactionWithID(txID, "test-tx").WithStepRegistry(coord)
		for _, name := range stepNames {
			builder.AddStep(name)
		}

		tx, err := builder.Build()
		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction (should fail and compensate)
		result, execErr := coord.Execute(context.Background(), tx)
		if execErr != nil && result == nil {
			rt.Fatalf("failed to execute transaction: %v", execErr)
		}

		// Property: Transaction should be compensated
		if result.Status != rte.TxStatusCompensated {
			// Debug: get transaction details
			storedTx, _ := ti.StoreAdapter.GetTransaction(context.Background(), tx.TxID())
			rt.Logf("Transaction status: %s, error: %s", storedTx.Status, storedTx.ErrorMsg)
			rt.Fatalf("expected COMPENSATED status, got %s (error: %v)", result.Status, result.Error)
		}

		// Property: All completed steps should be compensated
		// Steps 0 to failAtStep-1 should be compensated (failAtStep failed, so it wasn't completed)
		expectedCompensations := failAtStep // Steps 0 to failAtStep-1

		if len(compensationOrder) != expectedCompensations {
			// Debug: Check step statuses on failure
			debugSteps, debugErr := ti.StoreAdapter.GetSteps(context.Background(), tx.TxID())
			if debugErr == nil {
				for _, stepRecord := range debugSteps {
					rt.Logf("Step %d (%s): status=%s, error=%s", stepRecord.StepIndex, stepRecord.StepName, stepRecord.Status, stepRecord.ErrorMsg)
				}
			}
			rt.Fatalf("expected %d compensations, got %d: %v", expectedCompensations, len(compensationOrder), compensationOrder)
		}

		// Property: Compensation should be in reverse order
		// The first compensation should be for the step just before the failed step
		for i := 0; i < len(compensationOrder); i++ {
			expectedStepIndex := failAtStep - 1 - i
			if compensationOrder[i] != expectedStepIndex {
				rt.Fatalf("compensation order incorrect: expected step %d at position %d, got step %d. Full order: %v",
					expectedStepIndex, i, compensationOrder[i], compensationOrder)
			}
		}

		// Property: Verify step records in store
		steps, err := ti.StoreAdapter.GetSteps(context.Background(), tx.TxID())
		if err != nil {
			rt.Fatalf("failed to get steps: %v", err)
		}

		for _, stepRecord := range steps {
			if stepRecord.StepIndex < failAtStep {
				// Completed steps should be compensated
				if stepRecord.Status != rte.StepStatusCompensated {
					rt.Fatalf("step %d should be COMPENSATED, got %s", stepRecord.StepIndex, stepRecord.Status)
				}
			} else if stepRecord.StepIndex == failAtStep {
				// Failed step should be FAILED
				if stepRecord.Status != rte.StepStatusFailed {
					rt.Fatalf("step %d should be FAILED, got %s", stepRecord.StepIndex, stepRecord.Status)
				}
			} else {
				// Steps after failed step should be PENDING
				if stepRecord.Status != rte.StepStatusPending {
					rt.Fatalf("step %d should be PENDING, got %s", stepRecord.StepIndex, stepRecord.Status)
				}
			}
		}
	})
}

// compensationTestStep is a test step for compensation completeness testing
type compensationTestStep struct {
	*rte.BaseStep
	stepIndex         int
	failAtStep        int
	compensationOrder *[]int
	compensationMu    *sync.Mutex
}

func (s *compensationTestStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	if s.stepIndex == s.failAtStep {
		return fmt.Errorf("simulated failure at step %d", s.stepIndex)
	}
	return nil
}

func (s *compensationTestStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	s.compensationMu.Lock()
	*s.compensationOrder = append(*s.compensationOrder, s.stepIndex)
	s.compensationMu.Unlock()
	return nil
}

func (s *compensationTestStep) SupportsCompensation() bool {
	return true
}

// ============================================================================
// Property 3: State Transition Validity (状态转换有效性)
// For any transaction and for any sequence of operations, the transaction status
// SHALL only transition through valid paths as defined by the state machine.
// ============================================================================

// TestProperty_StateTransitionValidity_Integration tests state transition validity using real MySQL and Redis.
// Property 3: State Transition Validity
// *For any* transaction and *for any* sequence of operations, the transaction status
// SHALL only transition through valid paths as defined by the state machine.
func TestProperty_StateTransitionValidity_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random number of steps
		numSteps := rapid.IntRange(1, 5).Draw(rt, "numSteps")
		failAtStep := rapid.IntRange(-1, numSteps-1).Draw(rt, "failAtStep") // -1 means no failure
		supportsCompensation := rapid.Bool().Draw(rt, "supportsCompensation")

		// Track state transitions
		var transitionMu sync.Mutex
		stateTransitions := make([]rte.TxStatus, 0)

		// Create event collector to track state changes
		collector := NewEventCollector()
		eventBus := event.NewMemoryEventBus()
		eventBus.SubscribeAll(collector.Handle)

		// Create a fresh circuit breaker for this iteration to avoid state leakage
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
			stepNames[i] = fmt.Sprintf("step-%d", i)
		}

		// Register steps
		for i := 0; i < numSteps; i++ {
			stepIndex := i
			step := &stateTransitionTestStep{
				BaseStep:             rte.NewBaseStep(stepNames[i]),
				stepIndex:            stepIndex,
				failAtStep:           failAtStep,
				supportsCompensation: supportsCompensation,
			}
			coord.RegisterStep(step)
		}

		// Build transaction with all steps
		txID := ti.GenerateTxID(fmt.Sprintf("state-%d", atomic.AddInt64(&txIDCounter, 1)))
		builder := rte.NewTransactionWithID(txID, "test-tx").WithStepRegistry(coord)
		for _, name := range stepNames {
			builder.AddStep(name)
		}

		tx, err := builder.Build()
		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction
		result, _ := coord.Execute(context.Background(), tx)

		// Get final stored transaction
		storedTx, err := ti.StoreAdapter.GetTransaction(context.Background(), tx.TxID())
		if err != nil {
			rt.Fatalf("failed to get stored transaction: %v", err)
		}

		// Record final state
		transitionMu.Lock()
		stateTransitions = append(stateTransitions, storedTx.Status)
		transitionMu.Unlock()

		// Property: Final state must be terminal or FAILED
		isValidFinalState := rte.IsTxTerminal(storedTx.Status) || storedTx.Status == rte.TxStatusFailed
		if !isValidFinalState {
			rt.Fatalf("final state %s is not a valid final state", storedTx.Status)
		}

		// Property: Result status must match stored status
		if result.Status != storedTx.Status {
			rt.Fatalf("result status %s does not match stored status %s", result.Status, storedTx.Status)
		}

		// Property: If no failure, status should be COMPLETED
		if failAtStep == -1 && storedTx.Status != rte.TxStatusCompleted {
			rt.Fatalf("expected COMPLETED for successful transaction, got %s", storedTx.Status)
		}

		// Property: If failure with compensation support and at least one step completed, status should be COMPENSATED
		if failAtStep > 0 && supportsCompensation {
			if storedTx.Status != rte.TxStatusCompensated {
				rt.Fatalf("expected COMPENSATED for failed transaction with compensation, got %s", storedTx.Status)
			}
		}

		// Property: If failure at first step or without compensation support, status should be FAILED
		if failAtStep == 0 || (failAtStep >= 0 && !supportsCompensation) {
			if storedTx.Status != rte.TxStatusFailed {
				rt.Fatalf("expected FAILED for failed transaction at first step or without compensation, got %s", storedTx.Status)
			}
		}

		// Property: Terminal states should not allow further transitions
		if rte.IsTxTerminal(storedTx.Status) {
			// Verify no valid transitions from terminal state
			for _, targetStatus := range []rte.TxStatus{
				rte.TxStatusCreated, rte.TxStatusLocked, rte.TxStatusExecuting,
				rte.TxStatusConfirming, rte.TxStatusCompleted, rte.TxStatusFailed,
				rte.TxStatusCompensating, rte.TxStatusCompensated,
			} {
				if rte.ValidateTxTransition(storedTx.Status, targetStatus) {
					rt.Fatalf("terminal state %s should not allow transition to %s", storedTx.Status, targetStatus)
				}
			}
		}
	})
}

// stateTransitionTestStep is a test step for state transition testing
type stateTransitionTestStep struct {
	*rte.BaseStep
	stepIndex            int
	failAtStep           int
	supportsCompensation bool
}

func (s *stateTransitionTestStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	if s.stepIndex == s.failAtStep {
		return fmt.Errorf("simulated failure at step %d", s.stepIndex)
	}
	return nil
}

func (s *stateTransitionTestStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	if !s.supportsCompensation {
		return rte.ErrCompensationNotSupported
	}
	return nil
}

func (s *stateTransitionTestStep) SupportsCompensation() bool {
	return s.supportsCompensation
}

// Ensure steps implement rte.Step interface
var _ rte.Step = (*balanceConservationDebitStep)(nil)
var _ rte.Step = (*balanceConservationCreditStep)(nil)
var _ rte.Step = (*compensationTestStep)(nil)
var _ rte.Step = (*stateTransitionTestStep)(nil)
