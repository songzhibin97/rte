// Package testinfra provides scenario tests for RTE production validation.
// These tests validate typical business scenarios including:
// - Saving to forex transfer flow
// - External service failure compensation
// - Uncertain timeout handling
// - Multi-step transaction execution and compensation
// - Concurrent transfer correctness
package testinfra

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"rte"
	"rte/event"
)

// ============================================================================
// Scenario Test Infrastructure
// ============================================================================

// ScenarioAccount represents a test account with balance for scenario tests
type ScenarioAccount struct {
	ID       int64
	UserID   int64
	Type     string // "saving", "forex"
	Currency string
	Balance  float64
	mu       sync.Mutex
}

// Debit debits amount from account
func (a *ScenarioAccount) Debit(amount float64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.Balance < amount {
		return fmt.Errorf("insufficient balance: have %.2f, need %.2f", a.Balance, amount)
	}
	a.Balance -= amount
	return nil
}

// Credit credits amount to account
func (a *ScenarioAccount) Credit(amount float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Balance += amount
}

// GetBalance returns current balance
func (a *ScenarioAccount) GetBalance() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.Balance
}

// ScenarioAccountStore manages accounts for scenario tests
type ScenarioAccountStore struct {
	accounts map[int64]*ScenarioAccount
	logs     []string
	mu       sync.RWMutex
}

// NewScenarioAccountStore creates a new scenario account store
func NewScenarioAccountStore() *ScenarioAccountStore {
	return &ScenarioAccountStore{
		accounts: make(map[int64]*ScenarioAccount),
		logs:     make([]string, 0),
	}
}

// CreateAccount creates a new account
func (s *ScenarioAccountStore) CreateAccount(id, userID int64, accType, currency string, balance float64) *ScenarioAccount {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc := &ScenarioAccount{
		ID:       id,
		UserID:   userID,
		Type:     accType,
		Currency: currency,
		Balance:  balance,
	}
	s.accounts[id] = acc
	return acc
}

// GetAccount returns an account by ID
func (s *ScenarioAccountStore) GetAccount(id int64) (*ScenarioAccount, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	acc, ok := s.accounts[id]
	return acc, ok
}

// TotalBalance returns total balance across all accounts
func (s *ScenarioAccountStore) TotalBalance() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var total float64
	for _, acc := range s.accounts {
		total += acc.GetBalance()
	}
	return total
}

// AddLog adds a log entry
func (s *ScenarioAccountStore) AddLog(entry string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = append(s.logs, entry)
}

// GetLogs returns all log entries
func (s *ScenarioAccountStore) GetLogs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]string, len(s.logs))
	copy(result, s.logs)
	return result
}

// ============================================================================
// Scenario Steps
// ============================================================================

// scenarioDebitStep debits from source account
type scenarioDebitStep struct {
	*rte.BaseStep
	store *ScenarioAccountStore
}

func newScenarioDebitStep(store *ScenarioAccountStore) *scenarioDebitStep {
	return &scenarioDebitStep{
		BaseStep: rte.NewBaseStep("scenario_debit"),
		store:    store,
	}
}

func (s *scenarioDebitStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, err := rte.GetInputAs[int64](txCtx, "from_account_id")
	if err != nil {
		return err
	}
	amount, err := rte.GetInputAs[float64](txCtx, "amount")
	if err != nil {
		return err
	}

	acc, ok := s.store.GetAccount(accountID)
	if !ok {
		return fmt.Errorf("account %d not found", accountID)
	}

	if err := acc.Debit(amount); err != nil {
		return err
	}

	s.store.AddLog(fmt.Sprintf("DEBIT: account=%d amount=%.2f", accountID, amount))
	txCtx.SetOutput("debited", true)
	txCtx.SetOutput("debit_amount", amount)
	return nil
}

func (s *scenarioDebitStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "from_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")

	acc, ok := s.store.GetAccount(accountID)
	if !ok {
		return fmt.Errorf("account %d not found for compensation", accountID)
	}

	acc.Credit(amount)
	s.store.AddLog(fmt.Sprintf("COMPENSATE_DEBIT: account=%d amount=%.2f", accountID, amount))
	return nil
}

func (s *scenarioDebitStep) SupportsCompensation() bool {
	return true
}

// scenarioCreditStep credits to target account
type scenarioCreditStep struct {
	*rte.BaseStep
	store *ScenarioAccountStore
}

func newScenarioCreditStep(store *ScenarioAccountStore) *scenarioCreditStep {
	return &scenarioCreditStep{
		BaseStep: rte.NewBaseStep("scenario_credit"),
		store:    store,
	}
}

func (s *scenarioCreditStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, err := rte.GetInputAs[int64](txCtx, "to_account_id")
	if err != nil {
		return err
	}
	amount, err := rte.GetInputAs[float64](txCtx, "amount")
	if err != nil {
		return err
	}

	acc, ok := s.store.GetAccount(accountID)
	if !ok {
		return fmt.Errorf("account %d not found", accountID)
	}

	acc.Credit(amount)
	s.store.AddLog(fmt.Sprintf("CREDIT: account=%d amount=%.2f", accountID, amount))
	txCtx.SetOutput("credited", true)
	return nil
}

func (s *scenarioCreditStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")

	acc, ok := s.store.GetAccount(accountID)
	if !ok {
		return fmt.Errorf("account %d not found for compensation", accountID)
	}

	if err := acc.Debit(amount); err != nil {
		return err
	}
	s.store.AddLog(fmt.Sprintf("COMPENSATE_CREDIT: account=%d amount=%.2f", accountID, amount))
	return nil
}

func (s *scenarioCreditStep) SupportsCompensation() bool {
	return true
}

// scenarioFinalizeStep finalizes the transfer
type scenarioFinalizeStep struct {
	*rte.BaseStep
	store *ScenarioAccountStore
}

func newScenarioFinalizeStep(store *ScenarioAccountStore) *scenarioFinalizeStep {
	return &scenarioFinalizeStep{
		BaseStep: rte.NewBaseStep("scenario_finalize"),
		store:    store,
	}
}

func (s *scenarioFinalizeStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")
	s.store.AddLog(fmt.Sprintf("FINALIZE: ticket=%s", ticket))
	txCtx.SetOutput("finalized", true)
	return nil
}

func (s *scenarioFinalizeStep) SupportsCompensation() bool {
	return false
}

// ============================================================================
// Saving to Forex Transfer Scenario Test
// ============================================================================

// TestScenario_SavingToForexTransfer tests the complete saving to forex transfer flow
func TestScenario_SavingToForexTransfer(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	// Create account store
	accountStore := NewScenarioAccountStore()

	// Create test accounts
	savingAccount := accountStore.CreateAccount(1, 100, "saving", "USD", 10000.0)
	forexAccount := accountStore.CreateAccount(2, 100, "forex", "USD", 5000.0)

	initialTotal := accountStore.TotalBalance()
	t.Logf("Initial total balance: %.2f", initialTotal)

	// Register steps
	ti.Engine.RegisterStep(newScenarioDebitStep(accountStore))
	ti.Engine.RegisterStep(newScenarioCreditStep(accountStore))
	ti.Engine.RegisterStep(newScenarioFinalizeStep(accountStore))

	// Subscribe to events for verification
	collector := NewEventCollector()
	ti.EventBus.SubscribeAll(collector.Handle)

	// Build and execute transfer transaction
	transferAmount := 1000.0
	txID := ti.GenerateTxID("saving-forex")
	tx, err := ti.Engine.NewTransactionWithID(txID, "saving_to_forex").
		WithLockKeys(ti.GenerateTxID("acc-1"), ti.GenerateTxID("acc-2")).
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          transferAmount,
			"ticket":          txID,
		}).
		AddStep("scenario_debit").
		AddStep("scenario_credit").
		AddStep("scenario_finalize").
		Build()

	if err != nil {
		t.Fatalf("Failed to build transaction: %v", err)
	}

	result, err := ti.Engine.Execute(ctx, tx)
	if err != nil {
		t.Fatalf("Transaction execution failed: %v", err)
	}

	// Verify transaction completed
	if result.Status != rte.TxStatusCompleted {
		t.Errorf("Expected status COMPLETED, got %s", result.Status)
	}

	// Verify balance changes
	expectedSavingBalance := 10000.0 - transferAmount
	expectedForexBalance := 5000.0 + transferAmount

	actualSavingBalance := savingAccount.GetBalance()
	actualForexBalance := forexAccount.GetBalance()

	if actualSavingBalance != expectedSavingBalance {
		t.Errorf("Saving account balance: expected %.2f, got %.2f", expectedSavingBalance, actualSavingBalance)
	}
	if actualForexBalance != expectedForexBalance {
		t.Errorf("Forex account balance: expected %.2f, got %.2f", expectedForexBalance, actualForexBalance)
	}

	// Verify balance conservation
	finalTotal := accountStore.TotalBalance()
	if initialTotal != finalTotal {
		t.Errorf("Balance conservation violated: initial %.2f, final %.2f", initialTotal, finalTotal)
	}

	// Verify logs
	logs := accountStore.GetLogs()
	if len(logs) != 3 {
		t.Errorf("Expected 3 log entries, got %d: %v", len(logs), logs)
	}

	// Verify events
	AssertEventPublished(t, collector, event.EventTxCreated)
	AssertEventPublished(t, collector, event.EventTxCompleted)
	AssertEventPublished(t, collector, event.EventStepStarted)
	AssertEventPublished(t, collector, event.EventStepCompleted)

	// Verify in database
	storedTx, err := ti.StoreAdapter.GetTransaction(ctx, txID)
	if err != nil {
		t.Fatalf("Failed to get transaction from store: %v", err)
	}
	if storedTx.Status != rte.TxStatusCompleted {
		t.Errorf("Stored transaction status: expected COMPLETED, got %s", storedTx.Status)
	}

	t.Logf("Transfer completed successfully: %.2f from saving to forex", transferAmount)
	t.Logf("Final balances - Saving: %.2f, Forex: %.2f", actualSavingBalance, actualForexBalance)
}

// ============================================================================
// External Service Failure Compensation Scenario Test
// ============================================================================

// scenarioExternalStep simulates an external service call that can fail
type scenarioExternalStep struct {
	*rte.BaseStep
	store       *ScenarioAccountStore
	shouldFail  bool
	failureMsg  string
	executed    int32
	compensated int32
}

func newScenarioExternalStep(store *ScenarioAccountStore, shouldFail bool, failureMsg string) *scenarioExternalStep {
	return &scenarioExternalStep{
		BaseStep:   rte.NewBaseStep("scenario_external"),
		store:      store,
		shouldFail: shouldFail,
		failureMsg: failureMsg,
	}
}

func (s *scenarioExternalStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	atomic.AddInt32(&s.executed, 1)

	if s.shouldFail {
		s.store.AddLog(fmt.Sprintf("EXTERNAL_FAILED: %s", s.failureMsg))
		return errors.New(s.failureMsg)
	}

	s.store.AddLog("EXTERNAL_SUCCESS")
	txCtx.SetOutput("external_completed", true)
	return nil
}

func (s *scenarioExternalStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	atomic.AddInt32(&s.compensated, 1)
	s.store.AddLog("EXTERNAL_COMPENSATED")
	return nil
}

func (s *scenarioExternalStep) SupportsCompensation() bool {
	return true
}

func (s *scenarioExternalStep) ExecutedCount() int32 {
	return atomic.LoadInt32(&s.executed)
}

func (s *scenarioExternalStep) CompensatedCount() int32 {
	return atomic.LoadInt32(&s.compensated)
}

// TestScenario_ExternalServiceFailureCompensation tests compensation when external service fails
func TestScenario_ExternalServiceFailureCompensation(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	// Create account store
	accountStore := NewScenarioAccountStore()

	// Create test accounts
	savingAccount := accountStore.CreateAccount(1, 100, "saving", "USD", 10000.0)
	forexAccount := accountStore.CreateAccount(2, 100, "forex", "USD", 5000.0)

	initialTotal := accountStore.TotalBalance()
	initialSavingBalance := savingAccount.GetBalance()
	initialForexBalance := forexAccount.GetBalance()

	// Register steps - external step will fail
	debitStep := newScenarioDebitStep(accountStore)
	externalStep := newScenarioExternalStep(accountStore, true, "external service unavailable")
	creditStep := newScenarioCreditStep(accountStore)

	ti.Engine.RegisterStep(debitStep)
	ti.Engine.RegisterStep(externalStep)
	ti.Engine.RegisterStep(creditStep)

	// Subscribe to events
	collector := NewEventCollector()
	ti.EventBus.SubscribeAll(collector.Handle)

	// Build and execute transfer transaction
	transferAmount := 1000.0
	txID := ti.GenerateTxID("external-fail")
	tx, err := ti.Engine.NewTransactionWithID(txID, "external_fail_test").
		WithLockKeys(ti.GenerateTxID("acc-1"), ti.GenerateTxID("acc-2")).
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          transferAmount,
			"ticket":          txID,
		}).
		AddStep("scenario_debit").
		AddStep("scenario_external").
		AddStep("scenario_credit").
		Build()

	if err != nil {
		t.Fatalf("Failed to build transaction: %v", err)
	}

	result, _ := ti.Engine.Execute(ctx, tx)

	// Verify transaction was compensated
	if result.Status != rte.TxStatusCompensated {
		t.Errorf("Expected status COMPENSATED, got %s", result.Status)
	}

	// Verify balances restored to initial values
	finalSavingBalance := savingAccount.GetBalance()
	finalForexBalance := forexAccount.GetBalance()

	if finalSavingBalance != initialSavingBalance {
		t.Errorf("Saving balance not restored: expected %.2f, got %.2f", initialSavingBalance, finalSavingBalance)
	}
	if finalForexBalance != initialForexBalance {
		t.Errorf("Forex balance not restored: expected %.2f, got %.2f", initialForexBalance, finalForexBalance)
	}

	// Verify balance conservation
	finalTotal := accountStore.TotalBalance()
	if initialTotal != finalTotal {
		t.Errorf("Balance conservation violated: initial %.2f, final %.2f", initialTotal, finalTotal)
	}

	// Verify logs show compensation
	logs := accountStore.GetLogs()
	hasDebit := false
	hasExternalFailed := false
	hasCompensateDebit := false

	for _, log := range logs {
		if log == fmt.Sprintf("DEBIT: account=%d amount=%.2f", 1, transferAmount) {
			hasDebit = true
		}
		if log == "EXTERNAL_FAILED: external service unavailable" {
			hasExternalFailed = true
		}
		if log == fmt.Sprintf("COMPENSATE_DEBIT: account=%d amount=%.2f", 1, transferAmount) {
			hasCompensateDebit = true
		}
	}

	if !hasDebit {
		t.Error("Expected DEBIT log entry")
	}
	if !hasExternalFailed {
		t.Error("Expected EXTERNAL_FAILED log entry")
	}
	if !hasCompensateDebit {
		t.Error("Expected COMPENSATE_DEBIT log entry")
	}

	// Verify events
	AssertEventPublished(t, collector, event.EventStepFailed)

	t.Logf("Compensation completed successfully after external service failure")
	t.Logf("Logs: %v", logs)
}

// ============================================================================
// Uncertain Timeout Scenario Test
// ============================================================================

// scenarioIdempotentStep simulates a step with idempotency support
type scenarioIdempotentStep struct {
	*rte.BaseStep
	store           *ScenarioAccountStore
	executionCount  int32
	actuallySucceed bool
	idempotencyMap  sync.Map // tracks executed idempotency keys
}

func newScenarioIdempotentStep(store *ScenarioAccountStore, actuallySucceed bool) *scenarioIdempotentStep {
	return &scenarioIdempotentStep{
		BaseStep:        rte.NewBaseStep("scenario_idempotent"),
		store:           store,
		actuallySucceed: actuallySucceed,
	}
}

func (s *scenarioIdempotentStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	atomic.AddInt32(&s.executionCount, 1)

	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")

	idemKey := fmt.Sprintf("%s:%d", ticket, accountID)

	// Check if already executed (simulating idempotency check)
	if _, loaded := s.idempotencyMap.LoadOrStore(idemKey, true); loaded {
		s.store.AddLog(fmt.Sprintf("IDEMPOTENT_SKIP: key=%s", idemKey))
		txCtx.SetOutput("idempotent_hit", true)
		return nil
	}

	// Perform the actual operation
	acc, ok := s.store.GetAccount(accountID)
	if !ok {
		return fmt.Errorf("account %d not found", accountID)
	}

	acc.Credit(amount)
	s.store.AddLog(fmt.Sprintf("IDEMPOTENT_EXECUTE: key=%s amount=%.2f", idemKey, amount))
	txCtx.SetOutput("idempotent_executed", true)
	return nil
}

func (s *scenarioIdempotentStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")

	acc, ok := s.store.GetAccount(accountID)
	if !ok {
		return fmt.Errorf("account %d not found", accountID)
	}

	if err := acc.Debit(amount); err != nil {
		return err
	}
	s.store.AddLog(fmt.Sprintf("IDEMPOTENT_COMPENSATE: account=%d amount=%.2f", accountID, amount))
	return nil
}

func (s *scenarioIdempotentStep) SupportsCompensation() bool {
	return true
}

func (s *scenarioIdempotentStep) SupportsIdempotency() bool {
	return true
}

func (s *scenarioIdempotentStep) IdempotencyKey(txCtx *rte.TxContext) string {
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	return fmt.Sprintf("idempotent:%s:%d", ticket, accountID)
}

func (s *scenarioIdempotentStep) ExecutionCount() int32 {
	return atomic.LoadInt32(&s.executionCount)
}

// TestScenario_UncertainTimeoutIdempotency tests idempotency handling for uncertain timeout
func TestScenario_UncertainTimeoutIdempotency(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	// Create account store
	accountStore := NewScenarioAccountStore()

	// Create test account
	account := accountStore.CreateAccount(1, 100, "forex", "USD", 5000.0)
	initialBalance := account.GetBalance()

	// Register idempotent step
	idempotentStep := newScenarioIdempotentStep(accountStore, true)
	ti.Engine.RegisterStep(idempotentStep)

	// First execution
	amount := 100.0
	ticket := ti.GenerateTxID("idem-ticket")
	txID1 := ti.GenerateTxID("idem-tx-1")

	tx1, err := ti.Engine.NewTransactionWithID(txID1, "idempotent_test").
		WithInput(map[string]any{
			"to_account_id": int64(1),
			"amount":        amount,
			"ticket":        ticket,
		}).
		AddStep("scenario_idempotent").
		Build()

	if err != nil {
		t.Fatalf("Failed to build transaction 1: %v", err)
	}

	result1, err := ti.Engine.Execute(ctx, tx1)
	if err != nil {
		t.Fatalf("Transaction 1 execution failed: %v", err)
	}

	if result1.Status != rte.TxStatusCompleted {
		t.Errorf("Transaction 1: expected COMPLETED, got %s", result1.Status)
	}

	balanceAfterFirst := account.GetBalance()
	expectedBalance := initialBalance + amount

	if balanceAfterFirst != expectedBalance {
		t.Errorf("Balance after first execution: expected %.2f, got %.2f", expectedBalance, balanceAfterFirst)
	}

	// Second execution with same idempotency key (simulating retry after timeout)
	txID2 := ti.GenerateTxID("idem-tx-2")
	tx2, err := ti.Engine.NewTransactionWithID(txID2, "idempotent_test").
		WithInput(map[string]any{
			"to_account_id": int64(1),
			"amount":        amount,
			"ticket":        ticket, // Same ticket = same idempotency key
		}).
		AddStep("scenario_idempotent").
		Build()

	if err != nil {
		t.Fatalf("Failed to build transaction 2: %v", err)
	}

	result2, err := ti.Engine.Execute(ctx, tx2)
	if err != nil {
		t.Fatalf("Transaction 2 execution failed: %v", err)
	}

	if result2.Status != rte.TxStatusCompleted {
		t.Errorf("Transaction 2: expected COMPLETED, got %s", result2.Status)
	}

	// Balance should NOT change on second execution (idempotency)
	balanceAfterSecond := account.GetBalance()
	if balanceAfterSecond != expectedBalance {
		t.Errorf("Balance after second execution: expected %.2f (unchanged), got %.2f", expectedBalance, balanceAfterSecond)
	}

	// Verify execution count - step was called twice but operation only executed once
	execCount := idempotentStep.ExecutionCount()
	if execCount != 2 {
		t.Errorf("Expected 2 step executions, got %d", execCount)
	}

	// Verify logs show idempotency skip
	logs := accountStore.GetLogs()
	hasExecute := false
	hasSkip := false
	for _, log := range logs {
		if log == fmt.Sprintf("IDEMPOTENT_EXECUTE: key=%s:%d amount=%.2f", ticket, 1, amount) {
			hasExecute = true
		}
		if log == fmt.Sprintf("IDEMPOTENT_SKIP: key=%s:%d", ticket, 1) {
			hasSkip = true
		}
	}

	if !hasExecute {
		t.Error("Expected IDEMPOTENT_EXECUTE log entry")
	}
	if !hasSkip {
		t.Error("Expected IDEMPOTENT_SKIP log entry")
	}

	t.Logf("Idempotency test passed: operation executed once despite two calls")
	t.Logf("Logs: %v", logs)
}

// ============================================================================
// Multi-Step Transaction Scenario Test
// ============================================================================

// multiStepTracker tracks execution and compensation order
type multiStepTracker struct {
	executionOrder    []string
	compensationOrder []string
	mu                sync.Mutex
}

func newMultiStepTracker() *multiStepTracker {
	return &multiStepTracker{
		executionOrder:    make([]string, 0),
		compensationOrder: make([]string, 0),
	}
}

func (t *multiStepTracker) RecordExecution(stepName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.executionOrder = append(t.executionOrder, stepName)
}

func (t *multiStepTracker) RecordCompensation(stepName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.compensationOrder = append(t.compensationOrder, stepName)
}

func (t *multiStepTracker) GetExecutionOrder() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]string, len(t.executionOrder))
	copy(result, t.executionOrder)
	return result
}

func (t *multiStepTracker) GetCompensationOrder() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	result := make([]string, len(t.compensationOrder))
	copy(result, t.compensationOrder)
	return result
}

// trackedStep is a step that records its execution and compensation
type trackedStep struct {
	*rte.BaseStep
	tracker    *multiStepTracker
	shouldFail bool
}

func newTrackedStep(name string, tracker *multiStepTracker, shouldFail bool) *trackedStep {
	return &trackedStep{
		BaseStep:   rte.NewBaseStep(name),
		tracker:    tracker,
		shouldFail: shouldFail,
	}
}

func (s *trackedStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	s.tracker.RecordExecution(s.Name())
	if s.shouldFail {
		return fmt.Errorf("step %s failed intentionally", s.Name())
	}
	txCtx.SetOutput(s.Name()+"_completed", true)
	return nil
}

func (s *trackedStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	s.tracker.RecordCompensation(s.Name())
	return nil
}

func (s *trackedStep) SupportsCompensation() bool {
	return true
}

// TestScenario_MultiStepTransactionSuccess tests successful multi-step transaction execution
func TestScenario_MultiStepTransactionSuccess(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	// Create tracker
	tracker := newMultiStepTracker()

	// Register 5 steps
	for i := 1; i <= 5; i++ {
		step := newTrackedStep(fmt.Sprintf("multi_step_%d", i), tracker, false)
		ti.Engine.RegisterStep(step)
	}

	// Build and execute transaction
	txID := ti.GenerateTxID("multi-step-success")
	tx, err := ti.Engine.NewTransactionWithID(txID, "multi_step_test").
		AddSteps("multi_step_1", "multi_step_2", "multi_step_3", "multi_step_4", "multi_step_5").
		Build()

	if err != nil {
		t.Fatalf("Failed to build transaction: %v", err)
	}

	result, err := ti.Engine.Execute(ctx, tx)
	if err != nil {
		t.Fatalf("Transaction execution failed: %v", err)
	}

	// Verify transaction completed
	if result.Status != rte.TxStatusCompleted {
		t.Errorf("Expected status COMPLETED, got %s", result.Status)
	}

	// Verify execution order
	execOrder := tracker.GetExecutionOrder()
	expectedOrder := []string{"multi_step_1", "multi_step_2", "multi_step_3", "multi_step_4", "multi_step_5"}

	if len(execOrder) != len(expectedOrder) {
		t.Errorf("Expected %d steps executed, got %d", len(expectedOrder), len(execOrder))
	}

	for i, expected := range expectedOrder {
		if i < len(execOrder) && execOrder[i] != expected {
			t.Errorf("Step %d: expected %s, got %s", i, expected, execOrder[i])
		}
	}

	// Verify no compensation occurred
	compOrder := tracker.GetCompensationOrder()
	if len(compOrder) != 0 {
		t.Errorf("Expected no compensations, got %d: %v", len(compOrder), compOrder)
	}

	// Verify all steps in database
	steps, err := ti.StoreAdapter.GetSteps(ctx, txID)
	if err != nil {
		t.Fatalf("Failed to get steps: %v", err)
	}

	if len(steps) != 5 {
		t.Errorf("Expected 5 steps in database, got %d", len(steps))
	}

	for _, step := range steps {
		if step.Status != rte.StepStatusCompleted {
			t.Errorf("Step %s should be COMPLETED, got %s", step.StepName, step.Status)
		}
	}

	t.Logf("Multi-step transaction completed successfully")
	t.Logf("Execution order: %v", execOrder)
}

// TestScenario_MultiStepTransactionCompensation tests multi-step transaction compensation
func TestScenario_MultiStepTransactionCompensation(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	// Create tracker
	tracker := newMultiStepTracker()

	// Register steps - step 4 will fail
	for i := 1; i <= 5; i++ {
		shouldFail := (i == 4)
		step := newTrackedStep(fmt.Sprintf("multi_comp_step_%d", i), tracker, shouldFail)
		ti.Engine.RegisterStep(step)
	}

	// Build and execute transaction
	txID := ti.GenerateTxID("multi-step-comp")
	tx, err := ti.Engine.NewTransactionWithID(txID, "multi_step_comp_test").
		AddSteps("multi_comp_step_1", "multi_comp_step_2", "multi_comp_step_3", "multi_comp_step_4", "multi_comp_step_5").
		Build()

	if err != nil {
		t.Fatalf("Failed to build transaction: %v", err)
	}

	result, _ := ti.Engine.Execute(ctx, tx)

	// Verify transaction was compensated
	if result.Status != rte.TxStatusCompensated {
		t.Errorf("Expected status COMPENSATED, got %s", result.Status)
	}

	// Verify execution order (steps 1-4 executed, step 4 failed)
	execOrder := tracker.GetExecutionOrder()
	expectedExecOrder := []string{"multi_comp_step_1", "multi_comp_step_2", "multi_comp_step_3", "multi_comp_step_4"}

	if len(execOrder) != len(expectedExecOrder) {
		t.Errorf("Expected %d steps executed, got %d: %v", len(expectedExecOrder), len(execOrder), execOrder)
	}

	for i, expected := range expectedExecOrder {
		if i < len(execOrder) && execOrder[i] != expected {
			t.Errorf("Execution step %d: expected %s, got %s", i, expected, execOrder[i])
		}
	}

	// Verify compensation order (reverse of completed steps: 3, 2, 1)
	compOrder := tracker.GetCompensationOrder()
	expectedCompOrder := []string{"multi_comp_step_3", "multi_comp_step_2", "multi_comp_step_1"}

	if len(compOrder) != len(expectedCompOrder) {
		t.Errorf("Expected %d compensations, got %d: %v", len(expectedCompOrder), len(compOrder), compOrder)
	}

	for i, expected := range expectedCompOrder {
		if i < len(compOrder) && compOrder[i] != expected {
			t.Errorf("Compensation step %d: expected %s, got %s", i, expected, compOrder[i])
		}
	}

	// Verify step statuses in database
	steps, err := ti.StoreAdapter.GetSteps(ctx, txID)
	if err != nil {
		t.Fatalf("Failed to get steps: %v", err)
	}

	for _, step := range steps {
		switch step.StepIndex {
		case 0, 1, 2: // Steps 1-3 should be compensated
			if step.Status != rte.StepStatusCompensated {
				t.Errorf("Step %d (%s) should be COMPENSATED, got %s", step.StepIndex, step.StepName, step.Status)
			}
		case 3: // Step 4 should be failed
			if step.Status != rte.StepStatusFailed {
				t.Errorf("Step %d (%s) should be FAILED, got %s", step.StepIndex, step.StepName, step.Status)
			}
		case 4: // Step 5 should be pending (never executed)
			if step.Status != rte.StepStatusPending {
				t.Errorf("Step %d (%s) should be PENDING, got %s", step.StepIndex, step.StepName, step.Status)
			}
		}
	}

	t.Logf("Multi-step compensation completed successfully")
	t.Logf("Execution order: %v", execOrder)
	t.Logf("Compensation order: %v", compOrder)
}

// ============================================================================
// Concurrent Transfer Scenario Test
// ============================================================================

// TestScenario_ConcurrentTransfers tests concurrent transfer execution with balance conservation
func TestScenario_ConcurrentTransfers(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	// Create account store with multiple accounts
	accountStore := NewScenarioAccountStore()

	// Create 5 accounts with initial balances
	numAccounts := 5
	initialBalancePerAccount := 10000.0
	for i := 1; i <= numAccounts; i++ {
		accountStore.CreateAccount(int64(i), 100, "checking", "USD", initialBalancePerAccount)
	}

	initialTotal := accountStore.TotalBalance()
	t.Logf("Initial total balance: %.2f across %d accounts", initialTotal, numAccounts)

	// Register steps
	ti.Engine.RegisterStep(newScenarioDebitStep(accountStore))
	ti.Engine.RegisterStep(newScenarioCreditStep(accountStore))
	ti.Engine.RegisterStep(newScenarioFinalizeStep(accountStore))

	// Execute concurrent transfers
	numTransfers := 20
	transferAmount := 100.0

	var wg sync.WaitGroup
	results := make(chan *rte.TxResult, numTransfers)
	errors := make(chan error, numTransfers)

	for i := 0; i < numTransfers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Random source and destination (different accounts)
			fromID := int64((idx % numAccounts) + 1)
			toID := int64(((idx + 1) % numAccounts) + 1)
			if fromID == toID {
				toID = int64(((idx + 2) % numAccounts) + 1)
			}

			txID := ti.GenerateTxID(fmt.Sprintf("concurrent-%d", idx))
			tx, err := ti.Engine.NewTransactionWithID(txID, "concurrent_transfer").
				WithLockKeys(
					ti.GenerateTxID(fmt.Sprintf("acc-%d", fromID)),
					ti.GenerateTxID(fmt.Sprintf("acc-%d", toID)),
				).
				WithInput(map[string]any{
					"from_account_id": fromID,
					"to_account_id":   toID,
					"amount":          transferAmount,
					"ticket":          txID,
				}).
				AddStep("scenario_debit").
				AddStep("scenario_credit").
				AddStep("scenario_finalize").
				Build()

			if err != nil {
				errors <- fmt.Errorf("build failed for tx %d: %v", idx, err)
				return
			}

			result, err := ti.Engine.Execute(ctx, tx)
			if err != nil {
				errors <- fmt.Errorf("execute failed for tx %d: %v", idx, err)
				return
			}

			results <- result
		}(i)
	}

	// Wait for all transfers to complete
	wg.Wait()
	close(results)
	close(errors)

	// Collect results
	successCount := 0
	failCount := 0
	compensatedCount := 0

	for result := range results {
		switch result.Status {
		case rte.TxStatusCompleted:
			successCount++
		case rte.TxStatusCompensated:
			compensatedCount++
		default:
			failCount++
		}
	}

	for err := range errors {
		t.Logf("Error: %v", err)
		failCount++
	}

	t.Logf("Results - Success: %d, Compensated: %d, Failed: %d", successCount, compensatedCount, failCount)

	// Verify balance conservation (most important check)
	finalTotal := accountStore.TotalBalance()
	if initialTotal != finalTotal {
		t.Errorf("Balance conservation violated: initial %.2f, final %.2f, diff %.2f",
			initialTotal, finalTotal, finalTotal-initialTotal)
	} else {
		t.Logf("Balance conservation verified: %.2f", finalTotal)
	}

	// Log final balances
	for i := 1; i <= numAccounts; i++ {
		acc, _ := accountStore.GetAccount(int64(i))
		t.Logf("Account %d final balance: %.2f", i, acc.GetBalance())
	}

	// At least some transfers should succeed
	if successCount == 0 {
		t.Error("Expected at least some successful transfers")
	}
}

// TestScenario_ConcurrentTransfersSameLock tests concurrent transfers with same lock key
func TestScenario_ConcurrentTransfersSameLock(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	// Create account store
	accountStore := NewScenarioAccountStore()

	// Create two accounts
	accountStore.CreateAccount(1, 100, "saving", "USD", 10000.0)
	accountStore.CreateAccount(2, 100, "forex", "USD", 5000.0)

	initialTotal := accountStore.TotalBalance()

	// Register steps
	ti.Engine.RegisterStep(newScenarioDebitStep(accountStore))
	ti.Engine.RegisterStep(newScenarioCreditStep(accountStore))
	ti.Engine.RegisterStep(newScenarioFinalizeStep(accountStore))

	// Execute concurrent transfers with SAME lock key
	numTransfers := 5
	transferAmount := 100.0
	sharedLockKey := ti.GenerateTxID("shared-lock")

	var wg sync.WaitGroup
	results := make(chan *rte.TxResult, numTransfers)

	for i := 0; i < numTransfers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			txID := ti.GenerateTxID(fmt.Sprintf("same-lock-%d", idx))
			tx, err := ti.Engine.NewTransactionWithID(txID, "same_lock_transfer").
				WithLockKeys(sharedLockKey).
				WithInput(map[string]any{
					"from_account_id": int64(1),
					"to_account_id":   int64(2),
					"amount":          transferAmount,
					"ticket":          txID,
				}).
				AddStep("scenario_debit").
				AddStep("scenario_credit").
				AddStep("scenario_finalize").
				Build()

			if err != nil {
				return
			}

			result, _ := ti.Engine.Execute(ctx, tx)
			results <- result
		}(i)
	}

	wg.Wait()
	close(results)

	// Collect results
	successCount := 0
	for result := range results {
		if result.Status == rte.TxStatusCompleted {
			successCount++
		}
	}

	// Verify balance conservation
	finalTotal := accountStore.TotalBalance()
	if initialTotal != finalTotal {
		t.Errorf("Balance conservation violated: initial %.2f, final %.2f", initialTotal, finalTotal)
	}

	// At least one should succeed
	if successCount == 0 {
		t.Error("Expected at least one successful transfer")
	}

	t.Logf("Concurrent same-lock transfers: %d/%d succeeded", successCount, numTransfers)
	t.Logf("Balance conservation verified: %.2f", finalTotal)
}
