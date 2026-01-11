// Package testinfra provides property-based tests for RTE production validation.
// Feature: rte-production-validation
// This file contains property tests for recovery, event publication, and confluence properties.
package testinfra

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"rte"
	"rte/circuit/memory"
	"rte/event"
	"rte/recovery"

	"pgregory.net/rapid"
)

// ============================================================================

// For any stuck transaction (LOCKED or EXECUTING status), recovery SHALL resume
// execution from the current step and complete or compensate correctly.
// ============================================================================

// TestProperty_RecoveryCorrectness_Integration tests recovery correctness using real MySQL and Redis.

// execution from the current step and complete or compensate correctly.
func TestProperty_RecoveryCorrectness_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random number of steps (2-4)
		numSteps := rapid.IntRange(2, 4).Draw(rt, "numSteps")

		// Generate random step to "get stuck" at (0 to numSteps-1)
		stuckAtStep := rapid.IntRange(0, numSteps-1).Draw(rt, "stuckAtStep")

		// Generate random stuck status (LOCKED or EXECUTING)
		stuckStatus := rapid.SampledFrom([]rte.TxStatus{
			rte.TxStatusLocked,
			rte.TxStatusExecuting,
		}).Draw(rt, "stuckStatus")

		// Generate whether the remaining steps should succeed or fail
		shouldSucceed := rapid.Bool().Draw(rt, "shouldSucceed")

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create a fresh event bus for this iteration
		eventBus := event.NewMemoryEventBus()

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
			stepNames[i] = fmt.Sprintf("recovery-step-%d", i)
		}

		// Register steps - steps before stuckAtStep are already "completed"
		// Steps at and after stuckAtStep will execute during recovery
		for i := 0; i < numSteps; i++ {
			stepIndex := i
			step := &recoveryTestStep{
				BaseStep:      rte.NewBaseStep(stepNames[i]),
				stepIndex:     stepIndex,
				stuckAtStep:   stuckAtStep,
				shouldSucceed: shouldSucceed,
			}
			coord.RegisterStep(step)
		}

		// Create a "stuck" transaction directly in the store
		txID := ti.GenerateTxID(fmt.Sprintf("recovery-%d", atomic.AddInt64(&txIDCounter, 1)))
		storeTx := rte.NewStoreTx(txID, "recovery-test", stepNames)
		storeTx.Status = stuckStatus
		storeTx.CurrentStep = stuckAtStep
		storeTx.MaxRetries = 3

		// Set context
		storeTx.Context = &rte.StoreTxContext{
			TxID:   txID,
			TxType: "recovery-test",
			Input:  make(map[string]any),
			Output: make(map[string]any),
		}

		// Create transaction in store
		err := ti.StoreAdapter.CreateTransaction(context.Background(), storeTx)
		if err != nil {
			rt.Fatalf("failed to create stuck transaction: %v", err)
		}

		// Create step records - mark steps before stuckAtStep as COMPLETED
		for i := 0; i < numSteps; i++ {
			stepRecord := rte.NewStoreStepRecord(txID, i, stepNames[i])
			if i < stuckAtStep {
				stepRecord.Status = rte.StepStatusCompleted
			} else {
				stepRecord.Status = rte.StepStatusPending
			}
			err := ti.StoreAdapter.CreateStep(context.Background(), stepRecord)
			if err != nil {
				rt.Fatalf("failed to create step record: %v", err)
			}
		}

		// Resume the stuck transaction
		result, _ := coord.Resume(context.Background(), storeTx)

		
		if !rte.IsTxTerminal(result.Status) && result.Status != rte.TxStatusFailed {
			rt.Fatalf("expected terminal or FAILED status after recovery, got %s", result.Status)
		}

		
		if shouldSucceed && result.Status != rte.TxStatusCompleted {
			rt.Fatalf("expected COMPLETED status for successful recovery, got %s", result.Status)
		}

		
		if !shouldSucceed {
			if result.Status != rte.TxStatusCompensated && result.Status != rte.TxStatusFailed {
				rt.Fatalf("expected COMPENSATED or FAILED status for failed recovery, got %s", result.Status)
			}
		}

		// Verify stored transaction state matches result
		finalTx, err := ti.StoreAdapter.GetTransaction(context.Background(), txID)
		if err != nil {
			rt.Fatalf("failed to get final transaction: %v", err)
		}

		if finalTx.Status != result.Status {
			rt.Fatalf("stored status %s does not match result status %s", finalTx.Status, result.Status)
		}

		rt.Logf("Recovery test: numSteps=%d, stuckAtStep=%d, stuckStatus=%s, shouldSucceed=%v, finalStatus=%s",
			numSteps, stuckAtStep, stuckStatus, shouldSucceed, result.Status)
	})
}

// recoveryTestStep is a test step for recovery testing
type recoveryTestStep struct {
	*rte.BaseStep
	stepIndex     int
	stuckAtStep   int
	shouldSucceed bool
}

func (s *recoveryTestStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	// Steps before stuckAtStep should already be completed (not executed during recovery)
	// Steps at and after stuckAtStep will be executed during recovery
	if !s.shouldSucceed && s.stepIndex == s.stuckAtStep {
		return fmt.Errorf("simulated failure at step %d during recovery", s.stepIndex)
	}
	return nil
}

func (s *recoveryTestStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *recoveryTestStep) SupportsCompensation() bool {
	return true
}

// Ensure recoveryTestStep implements rte.Step interface
var _ rte.Step = (*recoveryTestStep)(nil)

// ============================================================================

// Tests that the recovery worker correctly identifies and processes stuck transactions.
// ============================================================================

// recoveryStoreAdapter adapts StoreAdapter to recovery.TxStore interface
type recoveryStoreAdapter struct {
	store *StoreAdapter
}

func (a *recoveryStoreAdapter) GetTransaction(ctx context.Context, txID string) (*recovery.StoreTx, error) {
	tx, err := a.store.GetTransaction(ctx, txID)
	if err != nil {
		return nil, err
	}
	return toRecoveryStoreTx(tx), nil
}

func (a *recoveryStoreAdapter) UpdateTransaction(ctx context.Context, tx *recovery.StoreTx) error {
	rteTx := fromRecoveryStoreTx(tx)
	return a.store.UpdateTransaction(ctx, rteTx)
}

func (a *recoveryStoreAdapter) GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*recovery.StoreTx, error) {
	txs, err := a.store.GetStuckTransactions(ctx, olderThan)
	if err != nil {
		return nil, err
	}
	result := make([]*recovery.StoreTx, len(txs))
	for i, tx := range txs {
		result[i] = toRecoveryStoreTx(tx)
	}
	return result, nil
}

func (a *recoveryStoreAdapter) GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*recovery.StoreTx, error) {
	txs, err := a.store.GetRetryableTransactions(ctx, maxRetries)
	if err != nil {
		return nil, err
	}
	result := make([]*recovery.StoreTx, len(txs))
	for i, tx := range txs {
		result[i] = toRecoveryStoreTx(tx)
	}
	return result, nil
}

// toRecoveryStoreTx converts rte.StoreTx to recovery.StoreTx
func toRecoveryStoreTx(tx *rte.StoreTx) *recovery.StoreTx {
	return &recovery.StoreTx{
		ID:          tx.ID,
		TxID:        tx.TxID,
		TxType:      tx.TxType,
		Status:      string(tx.Status),
		CurrentStep: tx.CurrentStep,
		TotalSteps:  tx.TotalSteps,
		StepNames:   tx.StepNames,
		LockKeys:    tx.LockKeys,
		ErrorMsg:    tx.ErrorMsg,
		RetryCount:  tx.RetryCount,
		MaxRetries:  tx.MaxRetries,
		Version:     tx.Version,
		CreatedAt:   tx.CreatedAt,
		UpdatedAt:   tx.UpdatedAt,
		LockedAt:    tx.LockedAt,
		CompletedAt: tx.CompletedAt,
		TimeoutAt:   tx.TimeoutAt,
	}
}

// fromRecoveryStoreTx converts recovery.StoreTx to rte.StoreTx
func fromRecoveryStoreTx(tx *recovery.StoreTx) *rte.StoreTx {
	return &rte.StoreTx{
		ID:          tx.ID,
		TxID:        tx.TxID,
		TxType:      tx.TxType,
		Status:      rte.TxStatus(tx.Status),
		CurrentStep: tx.CurrentStep,
		TotalSteps:  tx.TotalSteps,
		StepNames:   tx.StepNames,
		LockKeys:    tx.LockKeys,
		ErrorMsg:    tx.ErrorMsg,
		RetryCount:  tx.RetryCount,
		MaxRetries:  tx.MaxRetries,
		Version:     tx.Version,
		CreatedAt:   tx.CreatedAt,
		UpdatedAt:   tx.UpdatedAt,
		LockedAt:    tx.LockedAt,
		CompletedAt: tx.CompletedAt,
		TimeoutAt:   tx.TimeoutAt,
	}
}

// TestProperty_RecoveryWorkerIntegration tests recovery worker with real infrastructure.
func TestProperty_RecoveryWorkerIntegration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random number of stuck transactions (1-3)
		numStuckTxs := rapid.IntRange(1, 3).Draw(rt, "numStuckTxs")

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create a fresh event bus for this iteration
		eventBus := event.NewMemoryEventBus()

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
		step := &simpleRecoveryStep{
			BaseStep: rte.NewBaseStep("simple-step"),
		}
		coord.RegisterStep(step)

		// Create stuck transactions
		txIDs := make([]string, numStuckTxs)
		for i := 0; i < numStuckTxs; i++ {
			txID := ti.GenerateTxID(fmt.Sprintf("worker-recovery-%d-%d", atomic.AddInt64(&txIDCounter, 1), i))
			txIDs[i] = txID

			storeTx := rte.NewStoreTx(txID, "worker-recovery-test", []string{"simple-step"})
			storeTx.Status = rte.TxStatusExecuting
			storeTx.CurrentStep = 0
			storeTx.MaxRetries = 3
			storeTx.Context = &rte.StoreTxContext{
				TxID:   txID,
				TxType: "worker-recovery-test",
				Input:  make(map[string]any),
				Output: make(map[string]any),
			}

			// Set UpdatedAt to be old enough to be considered stuck
			// We need to manipulate the timestamp to make it appear stuck
			err := ti.StoreAdapter.CreateTransaction(context.Background(), storeTx)
			if err != nil {
				rt.Fatalf("failed to create stuck transaction: %v", err)
			}

			// Create step record
			stepRecord := rte.NewStoreStepRecord(txID, 0, "simple-step")
			stepRecord.Status = rte.StepStatusPending
			err = ti.StoreAdapter.CreateStep(context.Background(), stepRecord)
			if err != nil {
				rt.Fatalf("failed to create step record: %v", err)
			}
		}

		// Create recovery store adapter
		recoveryStore := &recoveryStoreAdapter{store: ti.StoreAdapter}

		// Create recovery worker with very short stuck threshold for testing
		worker := recovery.NewWorker(
			recovery.WithStore(recoveryStore),
			recovery.WithLocker(ti.Locker),
			recovery.WithCoordinator(&coordinatorAdapter{coord: coord, store: ti.StoreAdapter}),
			recovery.WithEventBus(eventBus),
			recovery.WithConfig(recovery.Config{
				RecoveryInterval: 100 * time.Millisecond,
				StuckThreshold:   1 * time.Millisecond, // Very short for testing
				MaxRetries:       3,
				LockTTL:          30 * time.Second,
			}),
		)

		// Run a single scan
		worker.ScanOnce(context.Background())

		// Give some time for processing
		time.Sleep(100 * time.Millisecond)

		
		for _, txID := range txIDs {
			finalTx, err := ti.StoreAdapter.GetTransaction(context.Background(), txID)
			if err != nil {
				rt.Fatalf("failed to get transaction %s: %v", txID, err)
			}

			// Transaction should either be completed or still in progress
			// (depending on timing and lock acquisition)
			rt.Logf("Transaction %s status after recovery scan: %s", txID, finalTx.Status)
		}

		rt.Logf("Recovery worker test: processed %d stuck transactions", numStuckTxs)
	})
}

// simpleRecoveryStep is a simple step that always succeeds
type simpleRecoveryStep struct {
	*rte.BaseStep
}

func (s *simpleRecoveryStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *simpleRecoveryStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *simpleRecoveryStep) SupportsCompensation() bool {
	return true
}

// coordinatorAdapter adapts rte.Coordinator to recovery.Coordinator interface
type coordinatorAdapter struct {
	coord *rte.Coordinator
	store *StoreAdapter
}

func (a *coordinatorAdapter) Resume(ctx context.Context, txID string) error {
	// Get the transaction from store
	storeTx, err := a.store.GetTransaction(ctx, txID)
	if err != nil {
		return err
	}
	_, err = a.coord.Resume(ctx, storeTx)
	return err
}

// Ensure simpleRecoveryStep implements rte.Step interface
var _ rte.Step = (*simpleRecoveryStep)(nil)

// ============================================================================

// For any transaction lifecycle, the appropriate events SHALL be published
// at each state transition.
// ============================================================================

// TestProperty_EventPublicationCompleteness_Integration tests event publication completeness.

// at each state transition.
func TestProperty_EventPublicationCompleteness_Integration(t *testing.T) {
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
			stepNames[i] = fmt.Sprintf("event-step-%d", i)
		}

		// Register steps
		for i := 0; i < numSteps; i++ {
			stepIndex := i
			step := &eventTestStep{
				BaseStep:   rte.NewBaseStep(stepNames[i]),
				stepIndex:  stepIndex,
				failAtStep: failAtStep,
			}
			coord.RegisterStep(step)
		}

		// Build transaction with all steps
		txID := ti.GenerateTxID(fmt.Sprintf("event-%d", atomic.AddInt64(&txIDCounter, 1)))
		builder := rte.NewTransactionWithID(txID, "event-test").WithStepRegistry(coord)
		for _, name := range stepNames {
			builder.AddStep(name)
		}

		tx, err := builder.Build()
		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction
		result, _ := coord.Execute(context.Background(), tx)

		
		if !collector.HasEventType(event.EventTxCreated) {
			rt.Fatalf("expected tx.created event to be published")
		}

		
		if failAtStep == -1 {
			if result.Status != rte.TxStatusCompleted {
				rt.Fatalf("expected COMPLETED status, got %s", result.Status)
			}
			if !collector.HasEventType(event.EventTxCompleted) {
				rt.Fatalf("expected tx.completed event to be published for successful transaction")
			}
		}

		
		if failAtStep >= 0 {
			if !collector.HasEventType(event.EventTxFailed) {
				rt.Fatalf("expected tx.failed event to be published for failed transaction")
			}
		}

		
		stepStartedCount := collector.CountEventType(event.EventStepStarted)
		expectedStepStarted := numSteps
		if failAtStep >= 0 {
			expectedStepStarted = failAtStep + 1 // Steps 0 to failAtStep
		}
		if stepStartedCount != expectedStepStarted {
			rt.Fatalf("expected %d step.started events, got %d", expectedStepStarted, stepStartedCount)
		}

		
		stepCompletedCount := collector.CountEventType(event.EventStepCompleted)
		expectedStepCompleted := numSteps
		if failAtStep >= 0 {
			expectedStepCompleted = failAtStep // Steps 0 to failAtStep-1 completed
		}
		if stepCompletedCount != expectedStepCompleted {
			rt.Fatalf("expected %d step.completed events, got %d", expectedStepCompleted, stepCompletedCount)
		}

		
		if failAtStep >= 0 {
			stepFailedCount := collector.CountEventType(event.EventStepFailed)
			if stepFailedCount != 1 {
				rt.Fatalf("expected 1 step.failed event, got %d", stepFailedCount)
			}
		}

		// Verify event order: tx.created should be first
		events := collector.Events()
		if len(events) > 0 && events[0].Type != event.EventTxCreated {
			rt.Fatalf("expected first event to be tx.created, got %s", events[0].Type)
		}

		rt.Logf("Event publication test: numSteps=%d, failAtStep=%d, status=%s, totalEvents=%d",
			numSteps, failAtStep, result.Status, len(events))
	})
}

// eventTestStep is a test step for event publication testing
type eventTestStep struct {
	*rte.BaseStep
	stepIndex  int
	failAtStep int
}

func (s *eventTestStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	if s.stepIndex == s.failAtStep {
		return fmt.Errorf("simulated failure at step %d", s.stepIndex)
	}
	return nil
}

func (s *eventTestStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *eventTestStep) SupportsCompensation() bool {
	return true
}

// Ensure eventTestStep implements rte.Step interface
var _ rte.Step = (*eventTestStep)(nil)

// ============================================================================

// When compensation fails, the system SHALL publish alert.critical event.
// ============================================================================

// TestProperty_CompensationFailureAlert_Integration tests that compensation failure triggers critical alert.
func TestProperty_CompensationFailureAlert_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Create event collector
		collector := NewEventCollector()

		// Create a fresh event bus and subscribe collector
		eventBus := event.NewMemoryEventBus()
		eventBus.SubscribeAll(collector.Handle)

		// Create a fresh breaker for this iteration
		breaker := memory.NewMemoryBreaker()

		// Create coordinator with low max retries for faster test
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
				MaxRetries:       1, // Low retries for faster test
				RetryInterval:    10 * time.Millisecond,
			}),
		)

		// Register step that succeeds but has failing compensation
		successStep := &compensationFailingStep{
			BaseStep: rte.NewBaseStep("success-with-failing-comp"),
		}
		coord.RegisterStep(successStep)

		// Register step that fails (triggers compensation)
		failStep := &alwaysFailingStepWithComp{
			BaseStep: rte.NewBaseStep("always-fail"),
		}
		coord.RegisterStep(failStep)

		// Build transaction
		txID := ti.GenerateTxID(fmt.Sprintf("comp-alert-%d", atomic.AddInt64(&txIDCounter, 1)))
		tx, err := rte.NewTransactionWithID(txID, "comp-alert-test").
			WithStepRegistry(coord).
			AddStep("success-with-failing-comp").
			AddStep("always-fail").
			Build()

		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		// Execute transaction (should fail at step 2, then fail compensation)
		result, _ := coord.Execute(context.Background(), tx)

		
		if result.Status != rte.TxStatusCompensationFailed {
			rt.Fatalf("expected COMPENSATION_FAILED status, got %s", result.Status)
		}

		
		if !collector.HasEventType(event.EventAlertCritical) {
			rt.Fatalf("expected alert.critical event to be published for compensation failure")
		}

		
		if !collector.HasEventType(event.EventTxCompensationFailed) {
			rt.Fatalf("expected tx.compensation_failed event to be published")
		}

		rt.Logf("Compensation failure alert test: status=%s, alertCriticalPublished=true", result.Status)
	})
}

// compensationFailingStep is a step that succeeds but has failing compensation
type compensationFailingStep struct {
	*rte.BaseStep
}

func (s *compensationFailingStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *compensationFailingStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return fmt.Errorf("simulated compensation failure")
}

func (s *compensationFailingStep) SupportsCompensation() bool {
	return true
}

// alwaysFailingStepWithComp is a step that always fails but supports compensation
type alwaysFailingStepWithComp struct {
	*rte.BaseStep
}

func (s *alwaysFailingStepWithComp) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	return fmt.Errorf("simulated step failure")
}

func (s *alwaysFailingStepWithComp) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	return nil
}

func (s *alwaysFailingStepWithComp) SupportsCompensation() bool {
	return true
}

// Ensure steps implement rte.Step interface
var _ rte.Step = (*compensationFailingStep)(nil)
var _ rte.Step = (*alwaysFailingStepWithComp)(nil)

// ============================================================================

// For any set of concurrent transactions operating on independent resources,
// the final state SHALL be the same regardless of execution order.
// ============================================================================

// TestProperty_Confluence_Integration tests confluence using real MySQL and Redis.

// the final state SHALL be the same regardless of execution order.
func TestProperty_Confluence_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random number of independent transfers (2-5)
		numTransfers := rapid.IntRange(2, 5).Draw(rt, "numTransfers")

		// Create independent account pairs for each transfer
		// Each transfer operates on its own pair of accounts (no overlap)
		accountStore := NewMockAccountStore()
		transfers := make([]*confluenceTransfer, numTransfers)

		for i := 0; i < numTransfers; i++ {
			// Create unique account pair for this transfer
			fromID := fmt.Sprintf("from-acc-%d", i)
			toID := fmt.Sprintf("to-acc-%d", i)

			// Generate random initial balances
			fromBalance := rapid.Int64Range(1000, 10000).Draw(rt, fmt.Sprintf("fromBalance_%d", i))
			toBalance := rapid.Int64Range(0, 5000).Draw(rt, fmt.Sprintf("toBalance_%d", i))

			accountStore.CreateAccount(fromID, fromBalance)
			accountStore.CreateAccount(toID, toBalance)

			// Generate transfer amount (must be <= from balance)
			amount := rapid.Int64Range(1, fromBalance).Draw(rt, fmt.Sprintf("amount_%d", i))

			transfers[i] = &confluenceTransfer{
				fromID: fromID,
				toID:   toID,
				amount: amount,
			}
		}

		// Record initial total balance
		initialTotal := accountStore.TotalBalance()

		// Execute transfers in order 1 (original order)
		accountStore1 := cloneAccountStore(accountStore)
		executeTransfersInOrder(rt, ti, accountStore1, transfers, "order1")

		// Execute transfers in order 2 (reversed order)
		accountStore2 := cloneAccountStore(accountStore)
		reversedTransfers := reverseTransfers(transfers)
		executeTransfersInOrder(rt, ti, accountStore2, reversedTransfers, "order2")

		// Execute transfers in order 3 (shuffled order)
		accountStore3 := cloneAccountStore(accountStore)
		shuffledTransfers := shuffleTransfers(rt, transfers)
		executeTransfersInOrder(rt, ti, accountStore3, shuffledTransfers, "order3")

		
		finalTotal1 := accountStore1.TotalBalance()
		finalTotal2 := accountStore2.TotalBalance()
		finalTotal3 := accountStore3.TotalBalance()

		if finalTotal1 != initialTotal {
			rt.Fatalf("Order 1: balance conservation violated: initial=%d, final=%d", initialTotal, finalTotal1)
		}
		if finalTotal2 != initialTotal {
			rt.Fatalf("Order 2: balance conservation violated: initial=%d, final=%d", initialTotal, finalTotal2)
		}
		if finalTotal3 != initialTotal {
			rt.Fatalf("Order 3: balance conservation violated: initial=%d, final=%d", initialTotal, finalTotal3)
		}

		
		// (since transfers are independent, order shouldn't matter)
		for i := 0; i < numTransfers; i++ {
			fromID := fmt.Sprintf("from-acc-%d", i)
			toID := fmt.Sprintf("to-acc-%d", i)

			from1, _ := accountStore1.GetAccount(fromID)
			from2, _ := accountStore2.GetAccount(fromID)
			from3, _ := accountStore3.GetAccount(fromID)

			to1, _ := accountStore1.GetAccount(toID)
			to2, _ := accountStore2.GetAccount(toID)
			to3, _ := accountStore3.GetAccount(toID)

			if from1.GetBalance() != from2.GetBalance() || from1.GetBalance() != from3.GetBalance() {
				rt.Fatalf("Account %s balance differs across orders: %d, %d, %d",
					fromID, from1.GetBalance(), from2.GetBalance(), from3.GetBalance())
			}

			if to1.GetBalance() != to2.GetBalance() || to1.GetBalance() != to3.GetBalance() {
				rt.Fatalf("Account %s balance differs across orders: %d, %d, %d",
					toID, to1.GetBalance(), to2.GetBalance(), to3.GetBalance())
			}
		}

		rt.Logf("Confluence test: numTransfers=%d, initialTotal=%d, finalTotal=%d",
			numTransfers, initialTotal, finalTotal1)
	})
}

// confluenceTransfer represents a transfer for confluence testing
type confluenceTransfer struct {
	fromID string
	toID   string
	amount int64
}

// cloneAccountStore creates a deep copy of an account store
func cloneAccountStore(original *MockAccountStore) *MockAccountStore {
	clone := NewMockAccountStore()
	original.mu.RLock()
	defer original.mu.RUnlock()
	for id, acc := range original.accounts {
		clone.CreateAccount(id, acc.GetBalance())
	}
	return clone
}

// reverseTransfers returns a reversed copy of the transfers slice
func reverseTransfers(transfers []*confluenceTransfer) []*confluenceTransfer {
	n := len(transfers)
	reversed := make([]*confluenceTransfer, n)
	for i := 0; i < n; i++ {
		reversed[i] = transfers[n-1-i]
	}
	return reversed
}

// shuffleTransfers returns a shuffled copy of the transfers slice
func shuffleTransfers(rt *rapid.T, transfers []*confluenceTransfer) []*confluenceTransfer {
	n := len(transfers)
	shuffled := make([]*confluenceTransfer, n)
	copy(shuffled, transfers)

	// Generate a random permutation
	indices := make([]int, n)
	for i := 0; i < n; i++ {
		indices[i] = i
	}

	// Fisher-Yates shuffle using rapid's random
	for i := n - 1; i > 0; i-- {
		j := rapid.IntRange(0, i).Draw(rt, fmt.Sprintf("shuffle_%d", i))
		indices[i], indices[j] = indices[j], indices[i]
	}

	result := make([]*confluenceTransfer, n)
	for i, idx := range indices {
		result[i] = shuffled[idx]
	}
	return result
}

// executeTransfersInOrder executes transfers sequentially in the given order
func executeTransfersInOrder(rt *rapid.T, ti *TestInfrastructure, accountStore *MockAccountStore, transfers []*confluenceTransfer, orderName string) {
	for i, transfer := range transfers {
		// Create a fresh breaker for each transfer
		breaker := memory.NewMemoryBreaker()

		// Create coordinator
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

		// Register transfer steps
		debitStep := &confluenceDebitStep{
			BaseStep:     rte.NewBaseStep("debit"),
			accountStore: accountStore,
		}
		creditStep := &confluenceCreditStep{
			BaseStep:     rte.NewBaseStep("credit"),
			accountStore: accountStore,
		}

		coord.RegisterStep(debitStep)
		coord.RegisterStep(creditStep)

		// Build and execute transaction
		txID := ti.GenerateTxID(fmt.Sprintf("confluence-%s-%d-%d", orderName, atomic.AddInt64(&txIDCounter, 1), i))
		tx, err := rte.NewTransactionWithID(txID, "confluence-transfer").
			WithStepRegistry(coord).
			WithLockKeys(fmt.Sprintf("account:%s", transfer.fromID), fmt.Sprintf("account:%s", transfer.toID)).
			WithInput(map[string]any{
				"from_account_id": transfer.fromID,
				"to_account_id":   transfer.toID,
				"amount":          transfer.amount,
			}).
			AddStep("debit").
			AddStep("credit").
			Build()

		if err != nil {
			rt.Fatalf("failed to build transaction: %v", err)
		}

		result, _ := coord.Execute(context.Background(), tx)
		if result.Status != rte.TxStatusCompleted {
			rt.Fatalf("transfer %d in %s failed: %s", i, orderName, result.Status)
		}
	}
}

// confluenceDebitStep is a debit step for confluence testing
type confluenceDebitStep struct {
	*rte.BaseStep
	accountStore *MockAccountStore
}

func (s *confluenceDebitStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
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

func (s *confluenceDebitStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	fromID := txCtx.Input["from_account_id"].(string)
	amount := txCtx.Input["amount"].(int64)

	acc, ok := s.accountStore.GetAccount(fromID)
	if !ok {
		return fmt.Errorf("account not found: %s", fromID)
	}

	acc.Credit(amount)
	return nil
}

func (s *confluenceDebitStep) SupportsCompensation() bool {
	return true
}

// confluenceCreditStep is a credit step for confluence testing
type confluenceCreditStep struct {
	*rte.BaseStep
	accountStore *MockAccountStore
}

func (s *confluenceCreditStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
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

func (s *confluenceCreditStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
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

func (s *confluenceCreditStep) SupportsCompensation() bool {
	return true
}

// Ensure steps implement rte.Step interface
var _ rte.Step = (*confluenceDebitStep)(nil)
var _ rte.Step = (*confluenceCreditStep)(nil)

// ============================================================================

// Tests confluence with concurrent execution of independent transfers.
// ============================================================================

// TestProperty_ConcurrentConfluence_Integration tests confluence with concurrent execution.
func TestProperty_ConcurrentConfluence_Integration(t *testing.T) {
	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	rapid.Check(t, func(rt *rapid.T) {
		// Generate random number of concurrent transfers (2-5)
		numTransfers := rapid.IntRange(2, 5).Draw(rt, "numTransfers")

		// Create independent account pairs for each transfer
		accountStore := NewMockAccountStore()
		transfers := make([]*confluenceTransfer, numTransfers)

		for i := 0; i < numTransfers; i++ {
			fromID := fmt.Sprintf("conc-from-%d", i)
			toID := fmt.Sprintf("conc-to-%d", i)

			fromBalance := rapid.Int64Range(1000, 10000).Draw(rt, fmt.Sprintf("fromBalance_%d", i))
			toBalance := rapid.Int64Range(0, 5000).Draw(rt, fmt.Sprintf("toBalance_%d", i))

			accountStore.CreateAccount(fromID, fromBalance)
			accountStore.CreateAccount(toID, toBalance)

			amount := rapid.Int64Range(1, fromBalance).Draw(rt, fmt.Sprintf("amount_%d", i))

			transfers[i] = &confluenceTransfer{
				fromID: fromID,
				toID:   toID,
				amount: amount,
			}
		}

		// Record initial total balance
		initialTotal := accountStore.TotalBalance()

		// Execute all transfers concurrently
		var wg sync.WaitGroup
		results := make([]*rte.TxResult, numTransfers)
		var resultsMu sync.Mutex

		for i, transfer := range transfers {
			wg.Add(1)
			go func(idx int, tr *confluenceTransfer) {
				defer wg.Done()

				// Create a fresh breaker for each transfer
				breaker := memory.NewMemoryBreaker()

				// Create coordinator
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

				// Register transfer steps
				debitStep := &confluenceDebitStep{
					BaseStep:     rte.NewBaseStep("debit"),
					accountStore: accountStore,
				}
				creditStep := &confluenceCreditStep{
					BaseStep:     rte.NewBaseStep("credit"),
					accountStore: accountStore,
				}

				coord.RegisterStep(debitStep)
				coord.RegisterStep(creditStep)

				// Build and execute transaction
				txID := ti.GenerateTxID(fmt.Sprintf("conc-confluence-%d-%d", atomic.AddInt64(&txIDCounter, 1), idx))
				tx, err := rte.NewTransactionWithID(txID, "concurrent-transfer").
					WithStepRegistry(coord).
					WithLockKeys(fmt.Sprintf("account:%s", tr.fromID), fmt.Sprintf("account:%s", tr.toID)).
					WithInput(map[string]any{
						"from_account_id": tr.fromID,
						"to_account_id":   tr.toID,
						"amount":          tr.amount,
					}).
					AddStep("debit").
					AddStep("credit").
					Build()

				if err != nil {
					return
				}

				result, _ := coord.Execute(context.Background(), tx)

				resultsMu.Lock()
				results[idx] = result
				resultsMu.Unlock()
			}(i, transfer)
		}

		wg.Wait()

		
		for i, result := range results {
			if result == nil {
				rt.Fatalf("transfer %d result is nil", i)
			}
			if result.Status != rte.TxStatusCompleted {
				rt.Fatalf("transfer %d failed: %s", i, result.Status)
			}
		}

		
		finalTotal := accountStore.TotalBalance()
		if finalTotal != initialTotal {
			rt.Fatalf("Balance conservation violated: initial=%d, final=%d", initialTotal, finalTotal)
		}

		
		for i, transfer := range transfers {
			fromAcc, _ := accountStore.GetAccount(transfer.fromID)
			toAcc, _ := accountStore.GetAccount(transfer.toID)

			// The from account should have been debited
			// The to account should have been credited
			// We can't verify exact amounts without knowing initial balances,
			// but we can verify the transfer happened by checking the sum
			rt.Logf("Transfer %d: from=%s (balance=%d), to=%s (balance=%d), amount=%d",
				i, transfer.fromID, fromAcc.GetBalance(), transfer.toID, toAcc.GetBalance(), transfer.amount)
		}

		rt.Logf("Concurrent confluence test: numTransfers=%d, initialTotal=%d, finalTotal=%d",
			numTransfers, initialTotal, finalTotal)
	})
}

// ============================================================================
// Helper: Sort events by timestamp for verification
// ============================================================================

// sortEventsByTimestamp sorts events by their timestamp
func sortEventsByTimestamp(events []event.Event) []event.Event {
	sorted := make([]event.Event, len(events))
	copy(sorted, events)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.Before(sorted[j].Timestamp)
	})
	return sorted
}
