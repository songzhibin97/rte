// Package examples demonstrates how to use the RTE (Reliable Transaction Engine)
// for implementing distributed transactions in financial transfer scenarios.
//
// This example shows a complete Saving-to-Forex transfer implementation,
// including step definitions, transaction building, and execution.
package examples

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"rte"
	"rte/circuit/memory"
	"rte/event"
	"rte/lock"
)

// ============================================================================
// Data Models
// ============================================================================

// Account represents a user account with balance
type Account struct {
	ID        int64
	UserID    int64
	Type      string // "saving", "forex"
	Currency  string
	Balance   float64
	Version   int64
	UpdatedAt time.Time
}

// PendingLog represents a pending transaction log
type PendingLog struct {
	ID              int64
	Ticket          string
	AccountID       int64
	TransactionType string // "IN", "OUT"
	Amount          float64
	Status          string // "PENDING", "COMPLETE", "FAILED"
	CreatedAt       time.Time
}

// TransferRequest represents a transfer request
type TransferRequest struct {
	UserID        int64
	FromAccountID int64
	ToAccountID   int64
	Amount        float64
	Currency      string
	Ticket        string
}

// ============================================================================
// Mock Services (Replace with real implementations in production)
// ============================================================================

// AccountStore simulates database operations for accounts
type AccountStore struct {
	mu       sync.Mutex
	accounts map[int64]*Account
	logs     map[int64]*PendingLog
	logSeq   int64
}

// NewAccountStore creates a new account store
func NewAccountStore() *AccountStore {
	return &AccountStore{
		accounts: make(map[int64]*Account),
		logs:     make(map[int64]*PendingLog),
	}
}

// CreateAccount creates a new account
func (s *AccountStore) CreateAccount(acc *Account) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts[acc.ID] = acc
}

// GetAccount retrieves an account by ID
func (s *AccountStore) GetAccount(accountID int64) (*Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[accountID]
	if !ok {
		return nil, errors.New("account not found")
	}
	copy := *acc
	return &copy, nil
}

// UpdateBalance updates account balance atomically
func (s *AccountStore) UpdateBalance(accountID int64, delta float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[accountID]
	if !ok {
		return errors.New("account not found")
	}
	if acc.Balance+delta < 0 {
		return errors.New("insufficient balance")
	}
	acc.Balance += delta
	acc.Version++
	acc.UpdatedAt = time.Now()
	return nil
}

// CreatePendingLog creates a pending log entry
func (s *AccountStore) CreatePendingLog(ticket string, accountID int64, txType string, amount float64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logSeq++
	log := &PendingLog{
		ID:              s.logSeq,
		Ticket:          ticket,
		AccountID:       accountID,
		TransactionType: txType,
		Amount:          amount,
		Status:          "PENDING",
		CreatedAt:       time.Now(),
	}
	s.logs[log.ID] = log
	return log.ID
}

// UpdateLogStatus updates the status of a pending log
func (s *AccountStore) UpdateLogStatus(logID int64, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.logs[logID]
	if !ok {
		return errors.New("log not found")
	}
	log.Status = status
	return nil
}

// ExternalService simulates PlatformB Manager or similar external service
type ExternalService struct {
	mu           sync.Mutex
	transactions map[string]bool
}

// NewExternalService creates a new external service
func NewExternalService() *ExternalService {
	return &ExternalService{
		transactions: make(map[string]bool),
	}
}

// Deposit performs a deposit operation on the external service
func (s *ExternalService) Deposit(ctx context.Context, accountID int64, amount float64, ticket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Simulate external service call
	s.transactions[ticket] = true
	return nil
}

// Withdraw performs a withdrawal operation on the external service
func (s *ExternalService) Withdraw(ctx context.Context, accountID int64, amount float64, ticket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transactions[ticket] = true
	return nil
}

// CheckTransaction checks if a transaction exists (for idempotency)
func (s *ExternalService) CheckTransaction(ctx context.Context, accountID int64, ticket string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.transactions[ticket], nil
}

// ============================================================================
// Transfer Steps Implementation
// ============================================================================

// DebitSavingStep deducts balance from the saving account
// This step:
// 1. Creates a pending log for audit trail
// 2. Deducts the amount from the source account
// 3. Supports compensation to restore balance on failure
type DebitSavingStep struct {
	*rte.BaseStep
	store *AccountStore
}

// NewDebitSavingStep creates a new debit saving step
func NewDebitSavingStep(store *AccountStore) *DebitSavingStep {
	return &DebitSavingStep{
		BaseStep: rte.NewBaseStep("debit_saving"),
		store:    store,
	}
}

// Execute performs the debit operation
func (s *DebitSavingStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	// Get input parameters
	accountID, err := rte.GetInputAs[int64](txCtx, "from_account_id")
	if err != nil {
		return fmt.Errorf("missing from_account_id: %w", err)
	}
	amount, err := rte.GetInputAs[float64](txCtx, "amount")
	if err != nil {
		return fmt.Errorf("missing amount: %w", err)
	}
	ticket, err := rte.GetInputAs[string](txCtx, "ticket")
	if err != nil {
		return fmt.Errorf("missing ticket: %w", err)
	}

	// Create pending log first (for audit trail)
	logID := s.store.CreatePendingLog(ticket, accountID, "OUT", -amount)
	txCtx.SetOutput("debit_log_id", logID)

	// Deduct balance from source account
	if err := s.store.UpdateBalance(accountID, -amount); err != nil {
		return fmt.Errorf("failed to debit account: %w", err)
	}

	txCtx.SetOutput("debited", true)
	return nil
}

// Compensate restores the balance on failure
func (s *DebitSavingStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "from_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")
	logID, _ := rte.GetOutputAs[int64](txCtx, "debit_log_id")

	// Restore balance
	if err := s.store.UpdateBalance(accountID, amount); err != nil {
		return fmt.Errorf("failed to restore balance: %w", err)
	}

	// Update log status
	if logID > 0 {
		s.store.UpdateLogStatus(logID, "FAILED")
	}

	return nil
}

// SupportsCompensation returns true as this step can be compensated
func (s *DebitSavingStep) SupportsCompensation() bool {
	return true
}

// IdempotencyKey returns a unique key for idempotency checking
func (s *DebitSavingStep) IdempotencyKey(txCtx *rte.TxContext) string {
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")
	accountID, _ := rte.GetInputAs[int64](txCtx, "from_account_id")
	return fmt.Sprintf("debit:%s:%d", ticket, accountID)
}

// SupportsIdempotency returns true as this step supports idempotency
func (s *DebitSavingStep) SupportsIdempotency() bool {
	return true
}

// PlatformBDepositStep calls the external PlatformB service to deposit funds
type PlatformBDepositStep struct {
	*rte.BaseStep
	external *ExternalService
	store    *AccountStore
}

// NewPlatformBDepositStep creates a new PlatformB deposit step
func NewPlatformBDepositStep(external *ExternalService, store *AccountStore) *PlatformBDepositStep {
	return &PlatformBDepositStep{
		BaseStep: rte.NewBaseStep("platform_b_deposit"),
		external: external,
		store:    store,
	}
}

// Execute performs the external deposit operation
func (s *PlatformBDepositStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")

	// Create pending log for the credit side
	logID := s.store.CreatePendingLog(ticket, accountID, "IN", amount)
	txCtx.SetOutput("credit_log_id", logID)

	// Call external service
	err := s.external.Deposit(ctx, accountID, amount, ticket)
	if err != nil {
		// On timeout, check if operation actually succeeded
		if errors.Is(err, context.DeadlineExceeded) {
			completed, checkErr := s.external.CheckTransaction(ctx, accountID, ticket)
			if checkErr == nil && completed {
				txCtx.SetOutput("external_deposited", true)
				return nil
			}
		}
		return fmt.Errorf("external deposit failed: %w", err)
	}

	txCtx.SetOutput("external_deposited", true)
	return nil
}

// Compensate withdraws the deposited amount on failure
func (s *PlatformBDepositStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")
	logID, _ := rte.GetOutputAs[int64](txCtx, "credit_log_id")

	// Withdraw to reverse the deposit
	compensateTicket := ticket + "_compensate"
	if err := s.external.Withdraw(ctx, accountID, amount, compensateTicket); err != nil {
		return fmt.Errorf("compensation withdraw failed: %w", err)
	}

	// Update log status
	if logID > 0 {
		s.store.UpdateLogStatus(logID, "FAILED")
	}

	return nil
}

// SupportsCompensation returns true
func (s *PlatformBDepositStep) SupportsCompensation() bool {
	return true
}

// IdempotencyKey returns a unique key for idempotency
func (s *PlatformBDepositStep) IdempotencyKey(txCtx *rte.TxContext) string {
	ticket, _ := rte.GetInputAs[string](txCtx, "ticket")
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	return fmt.Sprintf("platform_b_deposit:%s:%d", ticket, accountID)
}

// SupportsIdempotency returns true
func (s *PlatformBDepositStep) SupportsIdempotency() bool {
	return true
}

// CreditForexStep credits the forex account balance
type CreditForexStep struct {
	*rte.BaseStep
	store *AccountStore
}

// NewCreditForexStep creates a new credit forex step
func NewCreditForexStep(store *AccountStore) *CreditForexStep {
	return &CreditForexStep{
		BaseStep: rte.NewBaseStep("credit_forex"),
		store:    store,
	}
}

// Execute credits the target account
func (s *CreditForexStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")

	if err := s.store.UpdateBalance(accountID, amount); err != nil {
		return fmt.Errorf("failed to credit account: %w", err)
	}

	txCtx.SetOutput("credited", true)
	return nil
}

// Compensate reverses the credit
func (s *CreditForexStep) Compensate(ctx context.Context, txCtx *rte.TxContext) error {
	accountID, _ := rte.GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := rte.GetInputAs[float64](txCtx, "amount")

	return s.store.UpdateBalance(accountID, -amount)
}

// SupportsCompensation returns true
func (s *CreditForexStep) SupportsCompensation() bool {
	return true
}

// FinalizeTransferStep marks all pending logs as complete
type FinalizeTransferStep struct {
	*rte.BaseStep
	store *AccountStore
}

// NewFinalizeTransferStep creates a new finalize step
func NewFinalizeTransferStep(store *AccountStore) *FinalizeTransferStep {
	return &FinalizeTransferStep{
		BaseStep: rte.NewBaseStep("finalize_transfer"),
		store:    store,
	}
}

// Execute finalizes the transfer by updating log statuses
func (s *FinalizeTransferStep) Execute(ctx context.Context, txCtx *rte.TxContext) error {
	// Update debit log
	if debitLogID, err := rte.GetOutputAs[int64](txCtx, "debit_log_id"); err == nil && debitLogID > 0 {
		s.store.UpdateLogStatus(debitLogID, "COMPLETE")
	}

	// Update credit log
	if creditLogID, err := rte.GetOutputAs[int64](txCtx, "credit_log_id"); err == nil && creditLogID > 0 {
		s.store.UpdateLogStatus(creditLogID, "COMPLETE")
	}

	txCtx.SetOutput("finalized", true)
	return nil
}

// SupportsCompensation returns false (last step, no compensation needed)
func (s *FinalizeTransferStep) SupportsCompensation() bool {
	return false
}

// ============================================================================
// Example Usage
// ============================================================================

// RunSavingToForexTransferExample demonstrates a complete saving-to-forex transfer
func RunSavingToForexTransferExample() error {
	// Initialize dependencies
	accountStore := NewAccountStore()
	externalService := NewExternalService()

	// Create test accounts
	accountStore.CreateAccount(&Account{
		ID:       1,
		UserID:   100,
		Type:     "saving",
		Currency: "USD",
		Balance:  10000.0,
	})
	accountStore.CreateAccount(&Account{
		ID:       2,
		UserID:   100,
		Type:     "forex",
		Currency: "USD",
		Balance:  5000.0,
	})

	// Create mock store for RTE (in production, use MySQL store)
	rteStore := newMockRTEStore()
	locker := newMockLocker()
	breaker := memory.NewMemoryBreaker()
	eventBus := event.NewMemoryEventBus()

	// Create RTE engine with configuration
	engine := rte.NewEngine(
		rte.WithEngineStore(rteStore),
		rte.WithEngineLocker(locker),
		rte.WithEngineBreaker(breaker),
		rte.WithEngineEventBus(eventBus),
		rte.WithEngineConfig(rte.Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
		}),
	)

	// Register transfer steps
	engine.RegisterStep(NewDebitSavingStep(accountStore))
	engine.RegisterStep(NewPlatformBDepositStep(externalService, accountStore))
	engine.RegisterStep(NewCreditForexStep(accountStore))
	engine.RegisterStep(NewFinalizeTransferStep(accountStore))

	// Subscribe to events for monitoring
	engine.Subscribe(event.EventTxCompleted, func(ctx context.Context, e event.Event) error {
		fmt.Printf("Transaction %s completed successfully\n", e.TxID)
		return nil
	})

	engine.Subscribe(event.EventTxFailed, func(ctx context.Context, e event.Event) error {
		fmt.Printf("Transaction %s failed: %v\n", e.TxID, e.Error)
		return nil
	})

	// Build the transfer transaction
	transferAmount := 1000.0
	tx, err := engine.NewTransaction("saving_to_forex").
		WithLockKeys("account:1", "account:2"). // Lock both accounts
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          transferAmount,
			"ticket":          fmt.Sprintf("TRF-%d", time.Now().UnixNano()),
		}).
		AddStep("debit_saving").       // Step 1: Debit saving account
		AddStep("platform_b_deposit"). // Step 2: Call external PlatformB deposit
		AddStep("credit_forex").       // Step 3: Credit forex account
		AddStep("finalize_transfer").  // Step 4: Finalize logs
		Build()

	if err != nil {
		return fmt.Errorf("failed to build transaction: %w", err)
	}

	// Execute the transaction
	ctx := context.Background()
	result, err := engine.Execute(ctx, tx)
	if err != nil {
		return fmt.Errorf("transaction execution failed: %w", err)
	}

	// Check result
	if result.Status != rte.TxStatusCompleted {
		return fmt.Errorf("unexpected transaction status: %s", result.Status)
	}

	// Verify balances
	fromAcc, _ := accountStore.GetAccount(1)
	toAcc, _ := accountStore.GetAccount(2)

	fmt.Printf("Transfer completed in %v\n", result.Duration)
	fmt.Printf("From account balance: %.2f -> %.2f\n", 10000.0, fromAcc.Balance)
	fmt.Printf("To account balance: %.2f -> %.2f\n", 5000.0, toAcc.Balance)

	// Verify balance conservation
	expectedTotal := 15000.0
	actualTotal := fromAcc.Balance + toAcc.Balance
	if actualTotal != expectedTotal {
		return fmt.Errorf("balance conservation violated: expected %.2f, got %.2f", expectedTotal, actualTotal)
	}

	fmt.Println("Balance conservation verified!")
	return nil
}

// ============================================================================
// Mock Implementations (For demonstration - use real implementations in production)
// ============================================================================

// mockRTEStore implements rte.TxStore for demonstration
type mockRTEStore struct {
	mu          sync.Mutex
	txs         map[string]*rte.StoreTx
	steps       map[string]*rte.StoreStepRecord
	idempotency map[string][]byte
}

func newMockRTEStore() *mockRTEStore {
	return &mockRTEStore{
		txs:         make(map[string]*rte.StoreTx),
		steps:       make(map[string]*rte.StoreStepRecord),
		idempotency: make(map[string][]byte),
	}
}

func (s *mockRTEStore) CreateTransaction(ctx context.Context, tx *rte.StoreTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txs[tx.TxID] = tx
	return nil
}

func (s *mockRTEStore) GetTransaction(ctx context.Context, txID string) (*rte.StoreTx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, ok := s.txs[txID]
	if !ok {
		return nil, rte.ErrTransactionNotFound
	}
	return tx, nil
}

func (s *mockRTEStore) UpdateTransaction(ctx context.Context, tx *rte.StoreTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txs[tx.TxID] = tx
	return nil
}

func (s *mockRTEStore) CreateStep(ctx context.Context, step *rte.StoreStepRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s:%d", step.TxID, step.StepIndex)
	s.steps[key] = step
	return nil
}

func (s *mockRTEStore) GetStep(ctx context.Context, txID string, stepIndex int) (*rte.StoreStepRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s:%d", txID, stepIndex)
	step, ok := s.steps[key]
	if !ok {
		return nil, rte.ErrStepNotFound
	}
	return step, nil
}

func (s *mockRTEStore) UpdateStep(ctx context.Context, step *rte.StoreStepRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("%s:%d", step.TxID, step.StepIndex)
	s.steps[key] = step
	return nil
}

func (s *mockRTEStore) GetSteps(ctx context.Context, txID string) ([]*rte.StoreStepRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []*rte.StoreStepRecord
	for key, step := range s.steps {
		if len(key) > len(txID) && key[:len(txID)] == txID {
			result = append(result, step)
		}
	}
	return result, nil
}

func (s *mockRTEStore) GetPendingTransactions(ctx context.Context, olderThan time.Duration) ([]*rte.StoreTx, error) {
	return nil, nil
}

func (s *mockRTEStore) GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*rte.StoreTx, error) {
	return nil, nil
}

func (s *mockRTEStore) GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*rte.StoreTx, error) {
	return nil, nil
}

func (s *mockRTEStore) ListTransactions(ctx context.Context, filter *rte.StoreTxFilter) ([]*rte.StoreTx, int64, error) {
	return nil, 0, nil
}

func (s *mockRTEStore) CheckIdempotency(ctx context.Context, key string) (bool, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result, ok := s.idempotency[key]
	return ok, result, nil
}

func (s *mockRTEStore) MarkIdempotency(ctx context.Context, key string, result []byte, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idempotency[key] = result
	return nil
}

func (s *mockRTEStore) DeleteExpiredIdempotency(ctx context.Context) (int64, error) {
	return 0, nil
}

// mockLocker implements lock.Locker for demonstration
type mockLocker struct {
	mu    sync.Mutex
	locks map[string]bool
}

func newMockLocker() *mockLocker {
	return &mockLocker{
		locks: make(map[string]bool),
	}
}

func (l *mockLocker) Acquire(ctx context.Context, keys []string, ttl time.Duration) (lock.LockHandle, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, key := range keys {
		if l.locks[key] {
			return nil, rte.ErrLockAcquisitionFailed
		}
	}
	for _, key := range keys {
		l.locks[key] = true
	}
	return &mockLockHandle{locker: l, keys: keys}, nil
}

type mockLockHandle struct {
	locker *mockLocker
	keys   []string
}

func (h *mockLockHandle) Extend(ctx context.Context, ttl time.Duration) error {
	return nil
}

func (h *mockLockHandle) Release(ctx context.Context) error {
	h.locker.mu.Lock()
	defer h.locker.mu.Unlock()
	for _, key := range h.keys {
		delete(h.locker.locks, key)
	}
	return nil
}

func (h *mockLockHandle) Keys() []string {
	return h.keys
}
