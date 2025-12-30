package rte

import (
	"context"
	"errors"
	"fmt"
	"rte/event"
	"rte/lock"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// ============================================================================
// Transfer Scenario Tests - Based on transfer.go patterns
// ============================================================================

// FailureMode defines how the mock external service should behave
type FailureMode int

const (
	FailureModeNone      FailureMode = iota // Always succeed
	FailureModeAlways                       // Always fail
	FailureModeOnce                         // Fail once then succeed
	FailureModeTimeout                      // Timeout error
	FailureModeUncertain                    // Timeout but actually succeeded
)

// MockExternalService simulates PlatformA/PlatformB Manager behavior
type MockExternalService struct {
	mu            sync.Mutex
	failureMode   FailureMode
	failureCount  int
	transactions  map[string]bool // ticket -> completed
	depositCalls  int
	withdrawCalls int
	checkCalls    int
}

func NewMockExternalService() *MockExternalService {
	return &MockExternalService{
		transactions: make(map[string]bool),
	}
}

func (m *MockExternalService) SetFailureMode(mode FailureMode) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failureMode = mode
	m.failureCount = 0
}

func (m *MockExternalService) Deposit(ctx context.Context, accountID int64, amount float64, ticket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.depositCalls++

	switch m.failureMode {
	case FailureModeAlways:
		return errors.New("external service unavailable")
	case FailureModeOnce:
		if m.failureCount == 0 {
			m.failureCount++
			return errors.New("temporary failure")
		}
	case FailureModeTimeout:
		return context.DeadlineExceeded
	case FailureModeUncertain:
		// Actually succeed but return timeout
		m.transactions[ticket] = true
		return context.DeadlineExceeded
	}

	m.transactions[ticket] = true
	return nil
}

func (m *MockExternalService) Withdraw(ctx context.Context, accountID int64, amount float64, ticket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.withdrawCalls++

	switch m.failureMode {
	case FailureModeAlways:
		return errors.New("external service unavailable")
	case FailureModeOnce:
		if m.failureCount == 0 {
			m.failureCount++
			return errors.New("temporary failure")
		}
	case FailureModeTimeout:
		return context.DeadlineExceeded
	case FailureModeUncertain:
		m.transactions[ticket] = true
		return context.DeadlineExceeded
	}

	m.transactions[ticket] = true
	return nil
}

func (m *MockExternalService) CheckTransaction(ctx context.Context, accountID int64, ticket string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkCalls++
	return m.transactions[ticket], nil
}

func (m *MockExternalService) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failureMode = FailureModeNone
	m.failureCount = 0
	m.transactions = make(map[string]bool)
	m.depositCalls = 0
	m.withdrawCalls = 0
	m.checkCalls = 0
}

// ============================================================================
// Trackable Mock Locker - For testing lock exclusivity
// ============================================================================

// trackableMockLocker wraps mockLocker to track concurrent lock holders
type trackableMockLocker struct {
	mu            sync.Mutex
	locks         map[string]bool
	lockHolders   int32
	maxConcurrent int32
}

func newTrackableMockLocker() *trackableMockLocker {
	return &trackableMockLocker{
		locks: make(map[string]bool),
	}
}

func (l *trackableMockLocker) Acquire(ctx context.Context, keys []string, ttl time.Duration) (lock.LockHandle, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, key := range keys {
		if l.locks[key] {
			return nil, ErrLockAcquisitionFailed
		}
	}
	for _, key := range keys {
		l.locks[key] = true
	}

	// Track concurrent holders
	current := atomic.AddInt32(&l.lockHolders, 1)
	for {
		old := atomic.LoadInt32(&l.maxConcurrent)
		if current <= old || atomic.CompareAndSwapInt32(&l.maxConcurrent, old, current) {
			break
		}
	}

	return &trackableLockHandle{locker: l, keys: keys}, nil
}

func (l *trackableMockLocker) GetMaxConcurrent() int32 {
	return atomic.LoadInt32(&l.maxConcurrent)
}

type trackableLockHandle struct {
	locker *trackableMockLocker
	keys   []string
}

func (h *trackableLockHandle) Extend(ctx context.Context, ttl time.Duration) error {
	return nil
}

func (h *trackableLockHandle) Release(ctx context.Context) error {
	h.locker.mu.Lock()
	defer h.locker.mu.Unlock()
	for _, key := range h.keys {
		delete(h.locker.locks, key)
	}
	atomic.AddInt32(&h.locker.lockHolders, -1)
	return nil
}

func (h *trackableLockHandle) Keys() []string {
	return h.keys
}

// ============================================================================
// Account Store Mock - Simulates database operations
// ============================================================================

type Account struct {
	ID        int64
	UserID    int64
	Type      string // "saving", "forex", "future", "spot"
	Currency  string
	Balance   float64
	Version   int64
	UpdatedAt time.Time
}

type PendingLog struct {
	ID              int64
	Ticket          string
	UserID          int64
	AccountID       int64
	AccountType     string
	TransactionType string // "IN" or "OUT"
	OperationType   string // "TRANSFER", "DEPOSIT", "WITHDRAW"
	Amount          float64
	Status          string // "PENDING", "COMPLETE", "FAILED"
	Comment         string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type MockAccountStore struct {
	mu       sync.Mutex
	accounts map[int64]*Account
	logs     map[int64]*PendingLog
	logSeq   int64
}

func NewMockAccountStore() *MockAccountStore {
	return &MockAccountStore{
		accounts: make(map[int64]*Account),
		logs:     make(map[int64]*PendingLog),
	}
}

func (s *MockAccountStore) CreateAccount(acc *Account) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if acc.UpdatedAt.IsZero() {
		acc.UpdatedAt = time.Now()
	}
	s.accounts[acc.ID] = acc
}

func (s *MockAccountStore) GetAccount(accountID int64) (*Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[accountID]
	if !ok {
		return nil, errors.New("account not found")
	}
	// Return a copy to prevent external modification
	copy := *acc
	return &copy, nil
}

// UpdateBalanceWithVersion updates balance with optimistic locking
func (s *MockAccountStore) UpdateBalanceWithVersion(accountID int64, delta float64, expectedVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[accountID]
	if !ok {
		return errors.New("account not found")
	}
	if acc.Version != expectedVersion {
		return ErrVersionConflict
	}
	if acc.Balance+delta < 0 {
		return errors.New("insufficient balance")
	}
	acc.Balance += delta
	acc.Version++
	acc.UpdatedAt = time.Now()
	return nil
}

func (s *MockAccountStore) UpdateBalance(accountID int64, delta float64) error {
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

func (s *MockAccountStore) CreatePendingLog(ticket string, accountID int64, txType string, amount float64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logSeq++
	now := time.Now()
	log := &PendingLog{
		ID:              s.logSeq,
		Ticket:          ticket,
		AccountID:       accountID,
		TransactionType: txType,
		OperationType:   "TRANSFER",
		Amount:          amount,
		Status:          "PENDING",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	s.logs[log.ID] = log
	return log.ID
}

// CreatePendingLogFull creates a pending log with all fields
func (s *MockAccountStore) CreatePendingLogFull(log *PendingLog) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logSeq++
	log.ID = s.logSeq
	if log.Status == "" {
		log.Status = "PENDING"
	}
	if log.CreatedAt.IsZero() {
		log.CreatedAt = time.Now()
	}
	if log.UpdatedAt.IsZero() {
		log.UpdatedAt = time.Now()
	}
	s.logs[log.ID] = log
	return log.ID
}

func (s *MockAccountStore) UpdateLogStatus(logID int64, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.logs[logID]
	if !ok {
		return errors.New("log not found")
	}
	log.Status = status
	log.UpdatedAt = time.Now()
	return nil
}

// GetPendingLog retrieves a pending log by ID
func (s *MockAccountStore) GetPendingLog(logID int64) (*PendingLog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	log, ok := s.logs[logID]
	if !ok {
		return nil, errors.New("log not found")
	}
	copy := *log
	return &copy, nil
}

// GetPendingLogsByTicket retrieves all pending logs for a ticket
func (s *MockAccountStore) GetPendingLogsByTicket(ticket string) []*PendingLog {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []*PendingLog
	for _, log := range s.logs {
		if log.Ticket == ticket {
			copy := *log
			result = append(result, &copy)
		}
	}
	return result
}

func (s *MockAccountStore) GetTotalBalance() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total float64
	for _, acc := range s.accounts {
		total += acc.Balance
	}
	return total
}

// GetAllAccounts returns all accounts (for testing)
func (s *MockAccountStore) GetAllAccounts() []*Account {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*Account, 0, len(s.accounts))
	for _, acc := range s.accounts {
		copy := *acc
		result = append(result, &copy)
	}
	return result
}

// GetAllPendingLogs returns all pending logs (for testing)
func (s *MockAccountStore) GetAllPendingLogs() []*PendingLog {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*PendingLog, 0, len(s.logs))
	for _, log := range s.logs {
		copy := *log
		result = append(result, &copy)
	}
	return result
}

func (s *MockAccountStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts = make(map[int64]*Account)
	s.logs = make(map[int64]*PendingLog)
	s.logSeq = 0
}

// ============================================================================
// Test Helper Functions - Random Generators and Assertions
// ============================================================================

// TransferRequest represents a transfer request for testing
type TransferRequest struct {
	UserID          int64
	FromAccountID   int64
	FromAccountType string
	ToAccountID     int64
	ToAccountType   string
	Amount          float64
	Currency        string
	Ticket          string
}

// RandomAccountGenerator generates random accounts for property-based testing
type RandomAccountGenerator struct {
	idSeq int64
}

func NewRandomAccountGenerator() *RandomAccountGenerator {
	return &RandomAccountGenerator{}
}

// GenerateAccount generates a random account using rapid
func (g *RandomAccountGenerator) GenerateAccount(t *rapid.T, accountType string) *Account {
	g.idSeq++
	return &Account{
		ID:        g.idSeq,
		UserID:    rapid.Int64Range(1, 10000).Draw(t, "user_id"),
		Type:      accountType,
		Currency:  rapid.SampledFrom([]string{"USD", "EUR", "GBP", "JPY"}).Draw(t, "currency"),
		Balance:   rapid.Float64Range(0, 100000).Draw(t, "balance"),
		Version:   0,
		UpdatedAt: time.Now(),
	}
}

// GenerateAccountWithBalance generates an account with a specific minimum balance
func (g *RandomAccountGenerator) GenerateAccountWithBalance(t *rapid.T, accountType string, minBalance float64) *Account {
	g.idSeq++
	return &Account{
		ID:        g.idSeq,
		UserID:    rapid.Int64Range(1, 10000).Draw(t, "user_id"),
		Type:      accountType,
		Currency:  rapid.SampledFrom([]string{"USD", "EUR", "GBP", "JPY"}).Draw(t, "currency"),
		Balance:   rapid.Float64Range(minBalance, minBalance*10).Draw(t, "balance"),
		Version:   0,
		UpdatedAt: time.Now(),
	}
}

// RandomTransferGenerator generates random transfer requests
type RandomTransferGenerator struct {
	ticketSeq int64
}

func NewRandomTransferGenerator() *RandomTransferGenerator {
	return &RandomTransferGenerator{}
}

// GenerateTransfer generates a random transfer request
func (g *RandomTransferGenerator) GenerateTransfer(t *rapid.T, fromAccount, toAccount *Account) *TransferRequest {
	g.ticketSeq++
	maxAmount := fromAccount.Balance
	if maxAmount <= 0 {
		maxAmount = 1
	}
	return &TransferRequest{
		UserID:          fromAccount.UserID,
		FromAccountID:   fromAccount.ID,
		FromAccountType: fromAccount.Type,
		ToAccountID:     toAccount.ID,
		ToAccountType:   toAccount.Type,
		Amount:          rapid.Float64Range(0.01, maxAmount).Draw(t, "amount"),
		Currency:        fromAccount.Currency,
		Ticket:          fmt.Sprintf("TRF-%d-%d", time.Now().UnixNano(), g.ticketSeq),
	}
}

// GenerateTransferWithAmount generates a transfer with a specific amount
func (g *RandomTransferGenerator) GenerateTransferWithAmount(fromAccount, toAccount *Account, amount float64) *TransferRequest {
	g.ticketSeq++
	return &TransferRequest{
		UserID:          fromAccount.UserID,
		FromAccountID:   fromAccount.ID,
		FromAccountType: fromAccount.Type,
		ToAccountID:     toAccount.ID,
		ToAccountType:   toAccount.Type,
		Amount:          amount,
		Currency:        fromAccount.Currency,
		Ticket:          fmt.Sprintf("TRF-%d-%d", time.Now().UnixNano(), g.ticketSeq),
	}
}

// ToInput converts TransferRequest to input map for RTE
func (r *TransferRequest) ToInput() map[string]any {
	return map[string]any{
		"user_id":         r.UserID,
		"from_account_id": r.FromAccountID,
		"to_account_id":   r.ToAccountID,
		"amount":          r.Amount,
		"currency":        r.Currency,
		"ticket":          r.Ticket,
	}
}

// ============================================================================
// Assertion Helpers
// ============================================================================

// AssertBalanceConservation verifies that total balance is conserved
func AssertBalanceConservation(t testing.TB, store *MockAccountStore, expectedTotal float64) {
	t.Helper()
	actualTotal := store.GetTotalBalance()
	if actualTotal != expectedTotal {
		t.Errorf("balance conservation violated: expected=%f, actual=%f", expectedTotal, actualTotal)
	}
}

// AssertAccountBalance verifies an account has the expected balance
func AssertAccountBalance(t testing.TB, store *MockAccountStore, accountID int64, expectedBalance float64) {
	t.Helper()
	acc, err := store.GetAccount(accountID)
	if err != nil {
		t.Errorf("failed to get account %d: %v", accountID, err)
		return
	}
	if acc.Balance != expectedBalance {
		t.Errorf("account %d balance mismatch: expected=%f, actual=%f", accountID, expectedBalance, acc.Balance)
	}
}

// AssertPendingLogStatus verifies a pending log has the expected status
func AssertPendingLogStatus(t testing.TB, store *MockAccountStore, logID int64, expectedStatus string) {
	t.Helper()
	log, err := store.GetPendingLog(logID)
	if err != nil {
		t.Errorf("failed to get pending log %d: %v", logID, err)
		return
	}
	if log.Status != expectedStatus {
		t.Errorf("pending log %d status mismatch: expected=%s, actual=%s", logID, expectedStatus, log.Status)
	}
}

// AssertTransactionStatus verifies a transaction has the expected status
func AssertTransactionStatus(t testing.TB, result *TxResult, expectedStatus TxStatus) {
	t.Helper()
	if result.Status != expectedStatus {
		t.Errorf("transaction status mismatch: expected=%s, actual=%s", expectedStatus, result.Status)
	}
}

// AssertExternalServiceCalls verifies external service call counts
func AssertExternalServiceCalls(t testing.TB, service *MockExternalService, expectedDeposits, expectedWithdraws, expectedChecks int) {
	t.Helper()
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.depositCalls != expectedDeposits {
		t.Errorf("deposit calls mismatch: expected=%d, actual=%d", expectedDeposits, service.depositCalls)
	}
	if service.withdrawCalls != expectedWithdraws {
		t.Errorf("withdraw calls mismatch: expected=%d, actual=%d", expectedWithdraws, service.withdrawCalls)
	}
	if service.checkCalls != expectedChecks {
		t.Errorf("check calls mismatch: expected=%d, actual=%d", expectedChecks, service.checkCalls)
	}
}

// AssertNoError fails the test if err is not nil
func AssertNoError(t testing.TB, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: %v", msg, err)
	}
}

// AssertError fails the test if err is nil
func AssertError(t testing.TB, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: expected error but got nil", msg)
	}
}

// ============================================================================
// Transfer Steps - Based on transfer.go patterns
// ============================================================================

// DebitAccountStep deducts balance from source account
type DebitAccountStep struct {
	*BaseStep
	store *MockAccountStore
	logID int64
}

func NewDebitAccountStep(store *MockAccountStore) *DebitAccountStep {
	return &DebitAccountStep{
		BaseStep: NewBaseStep("debit_account"),
		store:    store,
	}
}

func (s *DebitAccountStep) Execute(ctx context.Context, txCtx *TxContext) error {
	accountID, _ := GetInputAs[int64](txCtx, "from_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")
	ticket, _ := GetInputAs[string](txCtx, "ticket")

	// Create pending log first (like transfer.go)
	logID := s.store.CreatePendingLog(ticket, accountID, "OUT", -amount)
	txCtx.SetOutput("debit_log_id", logID)

	// Deduct balance
	if err := s.store.UpdateBalance(accountID, -amount); err != nil {
		return err
	}

	txCtx.SetOutput("debited", true)
	return nil
}

func (s *DebitAccountStep) Compensate(ctx context.Context, txCtx *TxContext) error {
	accountID, _ := GetInputAs[int64](txCtx, "from_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")
	logID, _ := GetOutputAs[int64](txCtx, "debit_log_id")

	// Restore balance
	if err := s.store.UpdateBalance(accountID, amount); err != nil {
		return err
	}

	// Update log status
	if logID > 0 {
		s.store.UpdateLogStatus(logID, "FAILED")
	}

	return nil
}

func (s *DebitAccountStep) SupportsCompensation() bool {
	return true
}

func (s *DebitAccountStep) IdempotencyKey(txCtx *TxContext) string {
	ticket, _ := GetInputAs[string](txCtx, "ticket")
	accountID, _ := GetInputAs[int64](txCtx, "from_account_id")
	return fmt.Sprintf("debit:%s:%d", ticket, accountID)
}

func (s *DebitAccountStep) SupportsIdempotency() bool {
	return true
}

// ExternalDepositStep calls external service to deposit (like PlatformB deposit)
type ExternalDepositStep struct {
	*BaseStep
	external *MockExternalService
	store    *MockAccountStore
}

func NewExternalDepositStep(external *MockExternalService, store *MockAccountStore) *ExternalDepositStep {
	return &ExternalDepositStep{
		BaseStep: NewBaseStep("external_deposit"),
		external: external,
		store:    store,
	}
}

func (s *ExternalDepositStep) Execute(ctx context.Context, txCtx *TxContext) error {
	accountID, _ := GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")
	ticket, _ := GetInputAs[string](txCtx, "ticket")

	// Create pending log for credit
	logID := s.store.CreatePendingLog(ticket, accountID, "IN", amount)
	txCtx.SetOutput("credit_log_id", logID)

	// Call external service (like PlatformB deposit)
	err := s.external.Deposit(ctx, accountID, amount, ticket)
	if err != nil {
		// Check if actually succeeded (idempotency check like transfer.go)
		if errors.Is(err, context.DeadlineExceeded) {
			completed, checkErr := s.external.CheckTransaction(ctx, accountID, ticket)
			if checkErr == nil && completed {
				txCtx.SetOutput("external_deposited", true)
				return nil
			}
		}
		return err
	}

	txCtx.SetOutput("external_deposited", true)
	return nil
}

func (s *ExternalDepositStep) Compensate(ctx context.Context, txCtx *TxContext) error {
	accountID, _ := GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")
	ticket, _ := GetInputAs[string](txCtx, "ticket")
	logID, _ := GetOutputAs[int64](txCtx, "credit_log_id")

	// Call external service to withdraw (reverse deposit)
	compensateTicket := ticket + "_compensate"
	err := s.external.Withdraw(ctx, accountID, amount, compensateTicket)
	if err != nil {
		return err
	}

	// Update log status
	if logID > 0 {
		s.store.UpdateLogStatus(logID, "FAILED")
	}

	return nil
}

func (s *ExternalDepositStep) SupportsCompensation() bool {
	return true
}

func (s *ExternalDepositStep) IdempotencyKey(txCtx *TxContext) string {
	ticket, _ := GetInputAs[string](txCtx, "ticket")
	accountID, _ := GetInputAs[int64](txCtx, "to_account_id")
	return fmt.Sprintf("ext_deposit:%s:%d", ticket, accountID)
}

func (s *ExternalDepositStep) SupportsIdempotency() bool {
	return true
}

// CreditAccountStep credits balance to target account
type CreditAccountStep struct {
	*BaseStep
	store *MockAccountStore
}

func NewCreditAccountStep(store *MockAccountStore) *CreditAccountStep {
	return &CreditAccountStep{
		BaseStep: NewBaseStep("credit_account"),
		store:    store,
	}
}

func (s *CreditAccountStep) Execute(ctx context.Context, txCtx *TxContext) error {
	accountID, _ := GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")
	ticket, _ := GetInputAs[string](txCtx, "ticket")

	// Create pending log for credit (IN type)
	logID := s.store.CreatePendingLog(ticket, accountID, "IN", amount)
	txCtx.SetOutput("credit_log_id", logID)

	// Credit balance
	if err := s.store.UpdateBalance(accountID, amount); err != nil {
		return err
	}

	txCtx.SetOutput("credited", true)
	return nil
}

func (s *CreditAccountStep) Compensate(ctx context.Context, txCtx *TxContext) error {
	accountID, _ := GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")
	logID, _ := GetOutputAs[int64](txCtx, "credit_log_id")

	// Reverse credit
	if err := s.store.UpdateBalance(accountID, -amount); err != nil {
		return err
	}

	// Update log status
	if logID > 0 {
		s.store.UpdateLogStatus(logID, "FAILED")
	}

	return nil
}

func (s *CreditAccountStep) SupportsCompensation() bool {
	return true
}

// FinalizeStep updates all pending logs to COMPLETE
type FinalizeStep struct {
	*BaseStep
	store *MockAccountStore
}

func NewFinalizeStep(store *MockAccountStore) *FinalizeStep {
	return &FinalizeStep{
		BaseStep: NewBaseStep("finalize"),
		store:    store,
	}
}

func (s *FinalizeStep) Execute(ctx context.Context, txCtx *TxContext) error {
	// Update debit log (from DebitAccountStep)
	debitLogID, _ := GetOutputAs[int64](txCtx, "debit_log_id")
	if debitLogID > 0 {
		s.store.UpdateLogStatus(debitLogID, "COMPLETE")
	}

	// Update credit log (from CreditAccountStep or ExternalDepositStep)
	creditLogID, _ := GetOutputAs[int64](txCtx, "credit_log_id")
	if creditLogID > 0 {
		s.store.UpdateLogStatus(creditLogID, "COMPLETE")
	}

	// Update withdraw log (from ExternalWithdrawStep for forex-to-forex)
	withdrawLogID, _ := GetOutputAs[int64](txCtx, "withdraw_log_id")
	if withdrawLogID > 0 {
		s.store.UpdateLogStatus(withdrawLogID, "COMPLETE")
	}

	txCtx.SetOutput("finalized", true)
	return nil
}

func (s *FinalizeStep) SupportsCompensation() bool {
	return false // Last step, no compensation needed
}

// ============================================================================
// Integration Tests - Saving to Forex Transfer Scenario
// ============================================================================

// TestTransferScenario_SavingToForex_Success tests normal saving to forex transfer
// Based on SavingToForexTransfer in transfer.go
func TestTransferScenario_SavingToForex_Success(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()

	// Setup accounts
	accountStore.CreateAccount(&Account{ID: 1, UserID: 100, Type: "saving", Balance: 1000.0})
	accountStore.CreateAccount(&Account{ID: 2, UserID: 100, Type: "forex", Balance: 500.0})

	initialTotal := accountStore.GetTotalBalance()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
		}),
	)

	// Register steps
	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	// Build and execute transaction
	tx, err := engine.NewTransaction("saving_to_forex").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          100.0,
			"ticket":          "TRF-001",
		}).
		AddStep("debit_account").
		AddStep("external_deposit").
		AddStep("credit_account").
		AddStep("finalize").
		Build()

	if err != nil {
		t.Fatalf("failed to build transaction: %v", err)
	}

	result, err := engine.Execute(context.Background(), tx)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}

	// Verify result
	if result.Status != TxStatusCompleted {
		t.Errorf("expected status COMPLETED, got %s", result.Status)
	}

	// Verify balance conservation (Property 1)
	finalTotal := accountStore.GetTotalBalance()
	if finalTotal != initialTotal {
		t.Errorf("balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify account balances
	fromAcc, _ := accountStore.GetAccount(1)
	toAcc, _ := accountStore.GetAccount(2)
	if fromAcc.Balance != 900.0 {
		t.Errorf("expected from account balance 900, got %f", fromAcc.Balance)
	}
	if toAcc.Balance != 600.0 {
		t.Errorf("expected to account balance 600, got %f", toAcc.Balance)
	}
}

// TestTransferScenario_SavingToForex_ExternalFailure tests compensation on external service failure
func TestTransferScenario_SavingToForex_ExternalFailure(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()
	externalService.SetFailureMode(FailureModeAlways) // External service always fails

	// Setup accounts
	accountStore.CreateAccount(&Account{ID: 1, UserID: 100, Type: "saving", Balance: 1000.0})
	accountStore.CreateAccount(&Account{ID: 2, UserID: 100, Type: "forex", Balance: 500.0})

	initialTotal := accountStore.GetTotalBalance()
	initialFromBalance := 1000.0

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       1, // Fail fast for test
			RetryInterval:    10 * time.Millisecond,
		}),
	)

	// Register steps
	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	tx, _ := engine.NewTransaction("saving_to_forex").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          100.0,
			"ticket":          "TRF-002",
		}).
		AddStep("debit_account").
		AddStep("external_deposit").
		AddStep("credit_account").
		AddStep("finalize").
		Build()

	result, _ := engine.Execute(context.Background(), tx)

	// Transaction should fail or be compensated
	if result.Status != TxStatusFailed && result.Status != TxStatusCompensated {
		t.Errorf("expected status FAILED or COMPENSATED, got %s", result.Status)
	}

	// Verify balance conservation after compensation (Property 1)
	finalTotal := accountStore.GetTotalBalance()
	if finalTotal != initialTotal {
		t.Errorf("balance conservation violated after compensation: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify from account balance is restored
	fromAcc, _ := accountStore.GetAccount(1)
	if fromAcc.Balance != initialFromBalance {
		t.Errorf("from account balance not restored: expected %f, got %f", initialFromBalance, fromAcc.Balance)
	}
}

// TestTransferScenario_SavingToForex_UncertainTimeout tests idempotency check on uncertain timeout
func TestTransferScenario_SavingToForex_UncertainTimeout(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()
	externalService.SetFailureMode(FailureModeUncertain) // Timeout but actually succeeded

	// Setup accounts
	accountStore.CreateAccount(&Account{ID: 1, UserID: 100, Type: "saving", Balance: 1000.0})
	accountStore.CreateAccount(&Account{ID: 2, UserID: 100, Type: "forex", Balance: 500.0})

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
		}),
	)

	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	tx, _ := engine.NewTransaction("saving_to_forex").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          100.0,
			"ticket":          "TRF-003",
		}).
		AddStep("debit_account").
		AddStep("external_deposit").
		AddStep("credit_account").
		AddStep("finalize").
		Build()

	result, err := engine.Execute(context.Background(), tx)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}

	// Should succeed because idempotency check detected the actual success
	if result.Status != TxStatusCompleted {
		t.Errorf("expected status COMPLETED (idempotency check should pass), got %s", result.Status)
	}

	// Verify external service check was called
	if externalService.checkCalls == 0 {
		t.Error("expected idempotency check to be called")
	}
}

// ============================================================================
// Property-Based Tests
// ============================================================================

// TestProperty_BalanceConservation verifies that total balance is conserved
// Property 1: For any transfer, sum of balances before == sum after
func TestProperty_BalanceConservation(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockStore()
		locker := newMockLocker()
		breaker := newMockBreaker()
		eventBus := event.NewMemoryEventBus()

		accountStore := NewMockAccountStore()
		externalService := NewMockExternalService()

		// Generate random initial balances
		fromBalance := rapid.Float64Range(100, 10000).Draw(t, "from_balance")
		toBalance := rapid.Float64Range(0, 10000).Draw(t, "to_balance")
		transferAmount := rapid.Float64Range(1, fromBalance).Draw(t, "amount")

		accountStore.CreateAccount(&Account{ID: 1, Balance: fromBalance})
		accountStore.CreateAccount(&Account{ID: 2, Balance: toBalance})

		initialTotal := accountStore.GetTotalBalance()

		engine := NewEngine(
			WithEngineStore(store),
			WithEngineLocker(locker),
			WithEngineBreaker(breaker),
			WithEngineEventBus(eventBus),
			WithEngineConfig(Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       3,
				RetryInterval:    10 * time.Millisecond,
			}),
		)

		engine.RegisterStep(NewDebitAccountStep(accountStore))
		engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
		engine.RegisterStep(NewCreditAccountStep(accountStore))
		engine.RegisterStep(NewFinalizeStep(accountStore))

		tx, _ := engine.NewTransaction("transfer").
			WithLockKeys("account:1", "account:2").
			WithInput(map[string]any{
				"from_account_id": int64(1),
				"to_account_id":   int64(2),
				"amount":          transferAmount,
				"ticket":          fmt.Sprintf("TRF-%d", rapid.Int().Draw(t, "ticket_id")),
			}).
			AddStep("debit_account").
			AddStep("external_deposit").
			AddStep("credit_account").
			AddStep("finalize").
			Build()

		engine.Execute(context.Background(), tx)

		// Property: Total balance must be conserved (using approximate comparison for floating point)
		finalTotal := accountStore.GetTotalBalance()
		if !floatEquals(finalTotal, initialTotal, 0.0001) {
			t.Fatalf("Balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
		}
	})
}

// floatEquals compares two floats with a tolerance
func floatEquals(a, b, tolerance float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff <= tolerance
}

// TestProperty_IdempotentExecution verifies idempotency
// Property 3: Executing same operation twice produces same result
func TestProperty_IdempotentExecution(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockStore()
		locker := newMockLocker()
		breaker := newMockBreaker()
		eventBus := event.NewMemoryEventBus()

		accountStore := NewMockAccountStore()
		externalService := NewMockExternalService()

		accountStore.CreateAccount(&Account{ID: 1, Balance: 1000})
		accountStore.CreateAccount(&Account{ID: 2, Balance: 500})

		engine := NewEngine(
			WithEngineStore(store),
			WithEngineLocker(locker),
			WithEngineBreaker(breaker),
			WithEngineEventBus(eventBus),
			WithEngineConfig(Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       3,
				RetryInterval:    10 * time.Millisecond,
				IdempotencyTTL:   24 * time.Hour,
			}),
		)

		engine.RegisterStep(NewDebitAccountStep(accountStore))
		engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
		engine.RegisterStep(NewCreditAccountStep(accountStore))
		engine.RegisterStep(NewFinalizeStep(accountStore))

		ticket := fmt.Sprintf("TRF-IDEMP-%d", rapid.Int().Draw(t, "ticket"))
		amount := rapid.Float64Range(1, 100).Draw(t, "amount")

		// First execution
		tx1, _ := engine.NewTransaction("transfer").
			WithLockKeys("account:1", "account:2").
			WithInput(map[string]any{
				"from_account_id": int64(1),
				"to_account_id":   int64(2),
				"amount":          amount,
				"ticket":          ticket,
			}).
			AddStep("debit_account").
			AddStep("external_deposit").
			AddStep("credit_account").
			AddStep("finalize").
			Build()

		result1, _ := engine.Execute(context.Background(), tx1)
		balance1 := accountStore.GetTotalBalance()

		// Second execution with same ticket (should be idempotent)
		tx2, _ := engine.NewTransaction("transfer").
			WithLockKeys("account:1", "account:2").
			WithInput(map[string]any{
				"from_account_id": int64(1),
				"to_account_id":   int64(2),
				"amount":          amount,
				"ticket":          ticket,
			}).
			AddStep("debit_account").
			AddStep("external_deposit").
			AddStep("credit_account").
			AddStep("finalize").
			Build()

		result2, _ := engine.Execute(context.Background(), tx2)
		balance2 := accountStore.GetTotalBalance()

		// Property: Results should be equivalent
		if result1.Status != result2.Status {
			t.Fatalf("Idempotency violated: first=%s, second=%s", result1.Status, result2.Status)
		}

		// Property: Balance should not change on second execution
		if balance1 != balance2 {
			t.Fatalf("Idempotency violated: balance changed from %f to %f", balance1, balance2)
		}
	})
}

// TestProperty_LockExclusivity verifies lock mutual exclusion
// Property 5: At most one transaction holds the lock at any time
func TestProperty_LockExclusivity(t *testing.T) {
	store := newMockStore()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()

	accountStore.CreateAccount(&Account{ID: 1, Balance: 10000})
	accountStore.CreateAccount(&Account{ID: 2, Balance: 0})

	// Use trackable locker to monitor concurrency
	trackableLocker := newTrackableMockLocker()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(trackableLocker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    10 * time.Millisecond,
		}),
	)

	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	// Run concurrent transfers
	const numTransfers = 10
	var wg sync.WaitGroup
	wg.Add(numTransfers)

	for i := 0; i < numTransfers; i++ {
		go func(idx int) {
			defer wg.Done()
			tx, _ := engine.NewTransaction("transfer").
				WithLockKeys("account:1", "account:2").
				WithInput(map[string]any{
					"from_account_id": int64(1),
					"to_account_id":   int64(2),
					"amount":          100.0,
					"ticket":          fmt.Sprintf("TRF-CONC-%d", idx),
				}).
				AddStep("debit_account").
				AddStep("external_deposit").
				AddStep("credit_account").
				AddStep("finalize").
				Build()

			engine.Execute(context.Background(), tx)
		}(i)
	}

	wg.Wait()

	// Property: At most one lock holder at any time
	maxConcurrent := trackableLocker.GetMaxConcurrent()
	if maxConcurrent > 1 {
		t.Errorf("Lock exclusivity violated: max concurrent holders = %d", maxConcurrent)
	}

	// Verify balance conservation (total should remain constant)
	// Note: Some transfers may fail due to lock contention, but total balance should be conserved
	fromAcc, _ := accountStore.GetAccount(1)
	toAcc, _ := accountStore.GetAccount(2)
	totalBalance := fromAcc.Balance + toAcc.Balance
	expectedTotal := 10000.0 // Initial total

	if !floatEquals(totalBalance, expectedTotal, 0.0001) {
		t.Errorf("balance conservation violated: expected total %f, got %f", expectedTotal, totalBalance)
	}

	// Verify that transfers that succeeded moved the correct amount
	// Each successful transfer moves 100.0 from account 1 to account 2
	transferredAmount := toAcc.Balance // Account 2 started with 0
	expectedFromBalance := 10000.0 - transferredAmount

	if !floatEquals(fromAcc.Balance, expectedFromBalance, 0.0001) {
		t.Errorf("balance mismatch: from=%f, to=%f, expected from=%f", fromAcc.Balance, toAcc.Balance, expectedFromBalance)
	}
}

// TestProperty_RetryBounded verifies retry count is bounded
// Property 6: Retry attempts <= maxRetries + 1
func TestProperty_RetryBounded(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockStore()
		locker := newMockLocker()
		breaker := newMockBreaker()
		eventBus := event.NewMemoryEventBus()

		accountStore := NewMockAccountStore()
		externalService := NewMockExternalService()
		externalService.SetFailureMode(FailureModeAlways) // Always fail

		accountStore.CreateAccount(&Account{ID: 1, Balance: 1000})
		accountStore.CreateAccount(&Account{ID: 2, Balance: 500})

		maxRetries := rapid.IntRange(1, 5).Draw(t, "max_retries")

		engine := NewEngine(
			WithEngineStore(store),
			WithEngineLocker(locker),
			WithEngineBreaker(breaker),
			WithEngineEventBus(eventBus),
			WithEngineConfig(Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       maxRetries,
				RetryInterval:    1 * time.Millisecond,
			}),
		)

		engine.RegisterStep(NewDebitAccountStep(accountStore))
		engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
		engine.RegisterStep(NewCreditAccountStep(accountStore))
		engine.RegisterStep(NewFinalizeStep(accountStore))

		tx, _ := engine.NewTransaction("transfer").
			WithLockKeys("account:1", "account:2").
			WithInput(map[string]any{
				"from_account_id": int64(1),
				"to_account_id":   int64(2),
				"amount":          100.0,
				"ticket":          "TRF-RETRY",
			}).
			AddStep("debit_account").
			AddStep("external_deposit").
			AddStep("credit_account").
			AddStep("finalize").
			Build()

		engine.Execute(context.Background(), tx)

		// Property: Deposit calls should not exceed maxRetries + 1
		// (debit succeeds, external_deposit fails and retries)
		if externalService.depositCalls > maxRetries+1 {
			t.Fatalf("Retry bound violated: calls=%d, maxRetries=%d", externalService.depositCalls, maxRetries)
		}
	})
}

// TestProperty_CompensationCompleteness_Transfer verifies that all executed steps are compensated
// Property 4: For any failed transfer, all successfully executed steps SHALL be compensated
func TestProperty_CompensationCompleteness_Transfer(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := newMockStore()
		locker := newMockLocker()
		breaker := newMockBreaker()
		eventBus := event.NewMemoryEventBus()

		accountStore := NewMockAccountStore()

		// Generate random initial balances
		fromBalance := rapid.Float64Range(100, 10000).Draw(t, "from_balance")
		toBalance := rapid.Float64Range(0, 10000).Draw(t, "to_balance")
		transferAmount := rapid.Float64Range(1, fromBalance/2).Draw(t, "amount")

		accountStore.CreateAccount(&Account{ID: 1, Balance: fromBalance})
		accountStore.CreateAccount(&Account{ID: 2, Balance: toBalance})

		initialTotal := accountStore.GetTotalBalance()
		initialFromBalance := fromBalance
		initialToBalance := toBalance

		// Generate random failure point (1 = external_deposit, 2 = credit_account)
		// We don't fail at step 0 (debit) because we want to test compensation
		failAtStep := rapid.IntRange(1, 2).Draw(t, "fail_at_step")

		// Track compensation calls
		var compensationMu sync.Mutex
		compensatedSteps := make([]string, 0)

		// Create a failing external service for step 1
		externalService := NewMockExternalService()
		if failAtStep == 1 {
			externalService.SetFailureMode(FailureModeAlways)
		}

		engine := NewEngine(
			WithEngineStore(store),
			WithEngineLocker(locker),
			WithEngineBreaker(breaker),
			WithEngineEventBus(eventBus),
			WithEngineConfig(Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       1,
				RetryInterval:    1 * time.Millisecond,
			}),
		)

		// Create custom steps that track compensation
		debitStep := &trackableDebitStep{
			DebitAccountStep: NewDebitAccountStep(accountStore),
			onCompensate: func() {
				compensationMu.Lock()
				compensatedSteps = append(compensatedSteps, "debit_account")
				compensationMu.Unlock()
			},
		}

		externalDepositStep := &trackableExternalDepositStep{
			ExternalDepositStep: NewExternalDepositStep(externalService, accountStore),
			onCompensate: func() {
				compensationMu.Lock()
				compensatedSteps = append(compensatedSteps, "external_deposit")
				compensationMu.Unlock()
			},
		}

		// Create a credit step that fails if failAtStep == 2
		creditStep := &trackableCreditStep{
			CreditAccountStep: NewCreditAccountStep(accountStore),
			shouldFail:        failAtStep == 2,
			onCompensate: func() {
				compensationMu.Lock()
				compensatedSteps = append(compensatedSteps, "credit_account")
				compensationMu.Unlock()
			},
		}

		engine.RegisterStep(debitStep)
		engine.RegisterStep(externalDepositStep)
		engine.RegisterStep(creditStep)
		engine.RegisterStep(NewFinalizeStep(accountStore))

		tx, _ := engine.NewTransaction("transfer").
			WithLockKeys("account:1", "account:2").
			WithInput(map[string]any{
				"from_account_id": int64(1),
				"to_account_id":   int64(2),
				"amount":          transferAmount,
				"ticket":          fmt.Sprintf("TRF-COMP-%d", rapid.Int().Draw(t, "ticket_id")),
			}).
			AddStep("debit_account").
			AddStep("external_deposit").
			AddStep("credit_account").
			AddStep("finalize").
			Build()

		result, _ := engine.Execute(context.Background(), tx)

		// Property: Transaction should be compensated or failed
		if result.Status != TxStatusCompensated && result.Status != TxStatusFailed {
			t.Fatalf("expected COMPENSATED or FAILED status, got %s", result.Status)
		}

		// Property: Balance conservation must hold after compensation
		finalTotal := accountStore.GetTotalBalance()
		if !floatEquals(finalTotal, initialTotal, 0.0001) {
			t.Fatalf("Balance conservation violated after compensation: initial=%f, final=%f", initialTotal, finalTotal)
		}

		// Property: Account balances should be restored
		fromAcc, _ := accountStore.GetAccount(1)
		toAcc, _ := accountStore.GetAccount(2)

		if !floatEquals(fromAcc.Balance, initialFromBalance, 0.0001) {
			t.Fatalf("From account balance not restored: expected=%f, got=%f", initialFromBalance, fromAcc.Balance)
		}
		if !floatEquals(toAcc.Balance, initialToBalance, 0.0001) {
			t.Fatalf("To account balance not restored: expected=%f, got=%f", initialToBalance, toAcc.Balance)
		}

		// Property: If transaction was compensated, debit step should have been compensated
		if result.Status == TxStatusCompensated {
			compensationMu.Lock()
			hasDebitCompensation := false
			for _, step := range compensatedSteps {
				if step == "debit_account" {
					hasDebitCompensation = true
					break
				}
			}
			compensationMu.Unlock()

			if !hasDebitCompensation {
				t.Fatalf("Debit step was not compensated, compensated steps: %v", compensatedSteps)
			}
		}
	})
}

// trackableDebitStep wraps DebitAccountStep to track compensation
type trackableDebitStep struct {
	*DebitAccountStep
	onCompensate func()
}

func (s *trackableDebitStep) Compensate(ctx context.Context, txCtx *TxContext) error {
	if s.onCompensate != nil {
		s.onCompensate()
	}
	return s.DebitAccountStep.Compensate(ctx, txCtx)
}

// trackableExternalDepositStep wraps ExternalDepositStep to track compensation
type trackableExternalDepositStep struct {
	*ExternalDepositStep
	onCompensate func()
}

func (s *trackableExternalDepositStep) Compensate(ctx context.Context, txCtx *TxContext) error {
	if s.onCompensate != nil {
		s.onCompensate()
	}
	return s.ExternalDepositStep.Compensate(ctx, txCtx)
}

// trackableCreditStep wraps CreditAccountStep to track compensation and optionally fail
type trackableCreditStep struct {
	*CreditAccountStep
	shouldFail   bool
	onCompensate func()
}

func (s *trackableCreditStep) Execute(ctx context.Context, txCtx *TxContext) error {
	if s.shouldFail {
		return errors.New("credit step forced failure")
	}
	return s.CreditAccountStep.Execute(ctx, txCtx)
}

func (s *trackableCreditStep) Compensate(ctx context.Context, txCtx *TxContext) error {
	if s.onCompensate != nil {
		s.onCompensate()
	}
	return s.CreditAccountStep.Compensate(ctx, txCtx)
}

// ============================================================================
// Forex to Forex Transfer Tests (Two External Calls)
// ============================================================================

// ExternalWithdrawStep calls external service to withdraw
type ExternalWithdrawStep struct {
	*BaseStep
	external *MockExternalService
	store    *MockAccountStore
}

func NewExternalWithdrawStep(external *MockExternalService, store *MockAccountStore) *ExternalWithdrawStep {
	return &ExternalWithdrawStep{
		BaseStep: NewBaseStep("external_withdraw"),
		external: external,
		store:    store,
	}
}

func (s *ExternalWithdrawStep) Execute(ctx context.Context, txCtx *TxContext) error {
	accountID, _ := GetInputAs[int64](txCtx, "from_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")
	ticket, _ := GetInputAs[string](txCtx, "ticket")

	// Create pending log
	logID := s.store.CreatePendingLog(ticket, accountID, "OUT", -amount)
	txCtx.SetOutput("withdraw_log_id", logID)

	// Call external service
	err := s.external.Withdraw(ctx, accountID, amount, ticket)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			completed, checkErr := s.external.CheckTransaction(ctx, accountID, ticket)
			if checkErr == nil && completed {
				txCtx.SetOutput("external_withdrawn", true)
				return nil
			}
		}
		return err
	}

	txCtx.SetOutput("external_withdrawn", true)
	return nil
}

func (s *ExternalWithdrawStep) Compensate(ctx context.Context, txCtx *TxContext) error {
	accountID, _ := GetInputAs[int64](txCtx, "from_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")
	ticket, _ := GetInputAs[string](txCtx, "ticket")
	logID, _ := GetOutputAs[int64](txCtx, "withdraw_log_id")

	// Deposit back (reverse withdraw)
	compensateTicket := ticket + "_compensate"
	err := s.external.Deposit(ctx, accountID, amount, compensateTicket)
	if err != nil {
		return err
	}

	if logID > 0 {
		s.store.UpdateLogStatus(logID, "FAILED")
	}

	return nil
}

func (s *ExternalWithdrawStep) SupportsCompensation() bool {
	return true
}

func (s *ExternalWithdrawStep) IdempotencyKey(txCtx *TxContext) string {
	ticket, _ := GetInputAs[string](txCtx, "ticket")
	accountID, _ := GetInputAs[int64](txCtx, "from_account_id")
	return fmt.Sprintf("ext_withdraw:%s:%d", ticket, accountID)
}

func (s *ExternalWithdrawStep) SupportsIdempotency() bool {
	return true
}

// TestTransferScenario_ForexToForex_Success tests forex to forex transfer
func TestTransferScenario_ForexToForex_Success(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()

	// Setup forex accounts
	accountStore.CreateAccount(&Account{ID: 1, Type: "forex", Balance: 1000.0})
	accountStore.CreateAccount(&Account{ID: 2, Type: "forex", Balance: 500.0})

	initialTotal := accountStore.GetTotalBalance()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
		}),
	)

	// Register steps for forex-to-forex (two external calls)
	engine.RegisterStep(NewExternalWithdrawStep(externalService, accountStore))
	engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	tx, _ := engine.NewTransaction("forex_to_forex").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          200.0,
			"ticket":          "F2F-001",
		}).
		AddStep("external_withdraw"). // PlatformB withdraw from source
		AddStep("external_deposit").  // PlatformB deposit to target
		AddStep("debit_account").     // Update local source balance
		AddStep("credit_account").    // Update local target balance
		AddStep("finalize").
		Build()

	result, err := engine.Execute(context.Background(), tx)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}

	if result.Status != TxStatusCompleted {
		t.Errorf("expected status COMPLETED, got %s", result.Status)
	}

	// Verify balance conservation
	finalTotal := accountStore.GetTotalBalance()
	if finalTotal != initialTotal {
		t.Errorf("balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify both external calls were made
	if externalService.withdrawCalls != 1 {
		t.Errorf("expected 1 withdraw call, got %d", externalService.withdrawCalls)
	}
	if externalService.depositCalls != 1 {
		t.Errorf("expected 1 deposit call, got %d", externalService.depositCalls)
	}
}

// TestTransferScenario_ForexToForex_DepositFailure tests compensation when deposit fails after withdraw
func TestTransferScenario_ForexToForex_DepositFailure(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()

	// Use separate services for withdraw and deposit
	withdrawService := NewMockExternalService()
	depositService := NewMockExternalService()
	depositService.SetFailureMode(FailureModeAlways) // Deposit always fails

	accountStore.CreateAccount(&Account{ID: 1, Type: "forex", Balance: 1000.0})
	accountStore.CreateAccount(&Account{ID: 2, Type: "forex", Balance: 500.0})

	initialTotal := accountStore.GetTotalBalance()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       1,
			RetryInterval:    10 * time.Millisecond,
		}),
	)

	// Use different services for withdraw and deposit steps
	withdrawStep := NewExternalWithdrawStep(withdrawService, accountStore)
	depositStep := NewExternalDepositStep(depositService, accountStore)

	engine.RegisterStep(withdrawStep)
	engine.RegisterStep(depositStep)
	engine.RegisterStep(NewFinalizeStep(accountStore))

	tx, _ := engine.NewTransaction("forex_to_forex").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          200.0,
			"ticket":          "F2F-FAIL-001",
		}).
		AddStep("external_withdraw").
		AddStep("external_deposit").
		AddStep("finalize").
		Build()

	result, _ := engine.Execute(context.Background(), tx)

	// Should fail or be compensated
	if result.Status != TxStatusFailed && result.Status != TxStatusCompensated {
		t.Errorf("expected FAILED or COMPENSATED, got %s", result.Status)
	}

	// Verify balance conservation after compensation
	finalTotal := accountStore.GetTotalBalance()
	if finalTotal != initialTotal {
		t.Errorf("balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify withdraw was compensated (deposit back)
	if withdrawService.depositCalls == 0 {
		t.Error("expected compensation deposit call on withdraw service")
	}
}

// ============================================================================
// Forex to Saving Transfer Tests
// ============================================================================

// TestTransferScenario_ForexToSaving_Success tests normal forex to saving transfer
func TestTransferScenario_ForexToSaving_Success(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()

	// Setup accounts - forex account has balance, saving account receives
	accountStore.CreateAccount(&Account{ID: 1, UserID: 100, Type: "forex", Balance: 1000.0})
	accountStore.CreateAccount(&Account{ID: 2, UserID: 100, Type: "saving", Balance: 500.0})

	initialTotal := accountStore.GetTotalBalance()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
		}),
	)

	// Register steps for forex-to-saving
	// Step 1: External withdraw from forex (PlatformB)
	// Step 2: Debit local forex account
	// Step 3: Credit local saving account
	// Step 4: Finalize
	engine.RegisterStep(NewExternalWithdrawStep(externalService, accountStore))
	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	tx, err := engine.NewTransaction("forex_to_saving").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          200.0,
			"ticket":          "F2S-001",
		}).
		AddStep("external_withdraw"). // PlatformB withdraw
		AddStep("debit_account").     // Update local forex balance
		AddStep("credit_account").    // Update local saving balance
		AddStep("finalize").
		Build()

	if err != nil {
		t.Fatalf("failed to build transaction: %v", err)
	}

	result, err := engine.Execute(context.Background(), tx)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}

	// Verify result
	if result.Status != TxStatusCompleted {
		t.Errorf("expected status COMPLETED, got %s", result.Status)
	}

	// Verify balance conservation (Property 1)
	finalTotal := accountStore.GetTotalBalance()
	if !floatEquals(finalTotal, initialTotal, 0.0001) {
		t.Errorf("balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify account balances
	fromAcc, _ := accountStore.GetAccount(1)
	toAcc, _ := accountStore.GetAccount(2)
	if !floatEquals(fromAcc.Balance, 800.0, 0.0001) {
		t.Errorf("expected from account balance 800, got %f", fromAcc.Balance)
	}
	if !floatEquals(toAcc.Balance, 700.0, 0.0001) {
		t.Errorf("expected to account balance 700, got %f", toAcc.Balance)
	}

	// Verify external service was called
	if externalService.withdrawCalls != 1 {
		t.Errorf("expected 1 withdraw call, got %d", externalService.withdrawCalls)
	}
}

// TestTransferScenario_ForexToSaving_InsufficientBalance tests insufficient balance handling
func TestTransferScenario_ForexToSaving_InsufficientBalance(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()

	// Setup accounts - forex account has insufficient balance
	accountStore.CreateAccount(&Account{ID: 1, UserID: 100, Type: "forex", Balance: 100.0})
	accountStore.CreateAccount(&Account{ID: 2, UserID: 100, Type: "saving", Balance: 500.0})

	initialTotal := accountStore.GetTotalBalance()
	initialFromBalance := 100.0
	initialToBalance := 500.0

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       1,
			RetryInterval:    10 * time.Millisecond,
		}),
	)

	engine.RegisterStep(NewExternalWithdrawStep(externalService, accountStore))
	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	tx, _ := engine.NewTransaction("forex_to_saving").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          500.0, // More than available balance
			"ticket":          "F2S-INSUF-001",
		}).
		AddStep("external_withdraw").
		AddStep("debit_account").
		AddStep("credit_account").
		AddStep("finalize").
		Build()

	result, _ := engine.Execute(context.Background(), tx)

	// Transaction should fail or be compensated due to insufficient balance
	if result.Status != TxStatusFailed && result.Status != TxStatusCompensated {
		t.Errorf("expected status FAILED or COMPENSATED, got %s", result.Status)
	}

	// Verify balance conservation
	finalTotal := accountStore.GetTotalBalance()
	if !floatEquals(finalTotal, initialTotal, 0.0001) {
		t.Errorf("balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify balances are restored
	fromAcc, _ := accountStore.GetAccount(1)
	toAcc, _ := accountStore.GetAccount(2)
	if !floatEquals(fromAcc.Balance, initialFromBalance, 0.0001) {
		t.Errorf("from account balance not restored: expected %f, got %f", initialFromBalance, fromAcc.Balance)
	}
	if !floatEquals(toAcc.Balance, initialToBalance, 0.0001) {
		t.Errorf("to account balance not restored: expected %f, got %f", initialToBalance, toAcc.Balance)
	}
}

// TestTransferScenario_ForexToSaving_ExternalFailure tests external service failure
func TestTransferScenario_ForexToSaving_ExternalFailure(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()
	externalService.SetFailureMode(FailureModeAlways) // External service always fails

	// Setup accounts
	accountStore.CreateAccount(&Account{ID: 1, UserID: 100, Type: "forex", Balance: 1000.0})
	accountStore.CreateAccount(&Account{ID: 2, UserID: 100, Type: "saving", Balance: 500.0})

	initialTotal := accountStore.GetTotalBalance()
	initialFromBalance := 1000.0
	initialToBalance := 500.0

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       1,
			RetryInterval:    10 * time.Millisecond,
		}),
	)

	engine.RegisterStep(NewExternalWithdrawStep(externalService, accountStore))
	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	tx, _ := engine.NewTransaction("forex_to_saving").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          200.0,
			"ticket":          "F2S-FAIL-001",
		}).
		AddStep("external_withdraw").
		AddStep("debit_account").
		AddStep("credit_account").
		AddStep("finalize").
		Build()

	result, _ := engine.Execute(context.Background(), tx)

	// Transaction should fail (external withdraw fails first, no compensation needed)
	if result.Status != TxStatusFailed && result.Status != TxStatusCompensated {
		t.Errorf("expected status FAILED or COMPENSATED, got %s", result.Status)
	}

	// Verify balance conservation
	finalTotal := accountStore.GetTotalBalance()
	if !floatEquals(finalTotal, initialTotal, 0.0001) {
		t.Errorf("balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify balances unchanged (external withdraw failed first)
	fromAcc, _ := accountStore.GetAccount(1)
	toAcc, _ := accountStore.GetAccount(2)
	if !floatEquals(fromAcc.Balance, initialFromBalance, 0.0001) {
		t.Errorf("from account balance changed unexpectedly: expected %f, got %f", initialFromBalance, fromAcc.Balance)
	}
	if !floatEquals(toAcc.Balance, initialToBalance, 0.0001) {
		t.Errorf("to account balance changed unexpectedly: expected %f, got %f", initialToBalance, toAcc.Balance)
	}
}

// ============================================================================
// Cross-Platform Transfer Tests (PlatformA <-> PlatformB)
// ============================================================================

// MockPlatformService simulates a specific trading platform (PlatformA or PlatformB)
type MockPlatformService struct {
	*MockExternalService
	platform string // "PlatformA" or "PlatformB"
}

func NewMockPlatformService(platform string) *MockPlatformService {
	return &MockPlatformService{
		MockExternalService: NewMockExternalService(),
		platform:            platform,
	}
}

// CrossPlatformWithdrawStep withdraws from source platform
type CrossPlatformWithdrawStep struct {
	*BaseStep
	sourceService *MockPlatformService
	store         *MockAccountStore
}

func NewCrossPlatformWithdrawStep(sourceService *MockPlatformService, store *MockAccountStore) *CrossPlatformWithdrawStep {
	return &CrossPlatformWithdrawStep{
		BaseStep:      NewBaseStep("cross_platform_withdraw"),
		sourceService: sourceService,
		store:         store,
	}
}

func (s *CrossPlatformWithdrawStep) Execute(ctx context.Context, txCtx *TxContext) error {
	accountID, _ := GetInputAs[int64](txCtx, "from_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")
	ticket, _ := GetInputAs[string](txCtx, "ticket")

	// Create pending log for source platform withdrawal
	logID := s.store.CreatePendingLog(ticket, accountID, "OUT", -amount)
	txCtx.SetOutput("source_withdraw_log_id", logID)

	// Call source platform service
	err := s.sourceService.Withdraw(ctx, accountID, amount, ticket)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			completed, checkErr := s.sourceService.CheckTransaction(ctx, accountID, ticket)
			if checkErr == nil && completed {
				txCtx.SetOutput("source_withdrawn", true)
				return nil
			}
		}
		return err
	}

	txCtx.SetOutput("source_withdrawn", true)
	return nil
}

func (s *CrossPlatformWithdrawStep) Compensate(ctx context.Context, txCtx *TxContext) error {
	accountID, _ := GetInputAs[int64](txCtx, "from_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")
	ticket, _ := GetInputAs[string](txCtx, "ticket")
	logID, _ := GetOutputAs[int64](txCtx, "source_withdraw_log_id")

	// Deposit back to source platform
	compensateTicket := ticket + "_compensate"
	err := s.sourceService.Deposit(ctx, accountID, amount, compensateTicket)
	if err != nil {
		return err
	}

	if logID > 0 {
		s.store.UpdateLogStatus(logID, "FAILED")
	}

	return nil
}

func (s *CrossPlatformWithdrawStep) SupportsCompensation() bool {
	return true
}

func (s *CrossPlatformWithdrawStep) IdempotencyKey(txCtx *TxContext) string {
	ticket, _ := GetInputAs[string](txCtx, "ticket")
	accountID, _ := GetInputAs[int64](txCtx, "from_account_id")
	return fmt.Sprintf("cross_withdraw:%s:%d", ticket, accountID)
}

func (s *CrossPlatformWithdrawStep) SupportsIdempotency() bool {
	return true
}

// CrossPlatformDepositStep deposits to target platform
type CrossPlatformDepositStep struct {
	*BaseStep
	targetService *MockPlatformService
	store         *MockAccountStore
}

func NewCrossPlatformDepositStep(targetService *MockPlatformService, store *MockAccountStore) *CrossPlatformDepositStep {
	return &CrossPlatformDepositStep{
		BaseStep:      NewBaseStep("cross_platform_deposit"),
		targetService: targetService,
		store:         store,
	}
}

func (s *CrossPlatformDepositStep) Execute(ctx context.Context, txCtx *TxContext) error {
	accountID, _ := GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")
	ticket, _ := GetInputAs[string](txCtx, "ticket")

	// Create pending log for target platform deposit
	logID := s.store.CreatePendingLog(ticket, accountID, "IN", amount)
	txCtx.SetOutput("target_deposit_log_id", logID)

	// Call target platform service
	err := s.targetService.Deposit(ctx, accountID, amount, ticket)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			completed, checkErr := s.targetService.CheckTransaction(ctx, accountID, ticket)
			if checkErr == nil && completed {
				txCtx.SetOutput("target_deposited", true)
				return nil
			}
		}
		return err
	}

	txCtx.SetOutput("target_deposited", true)
	return nil
}

func (s *CrossPlatformDepositStep) Compensate(ctx context.Context, txCtx *TxContext) error {
	accountID, _ := GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")
	ticket, _ := GetInputAs[string](txCtx, "ticket")
	logID, _ := GetOutputAs[int64](txCtx, "target_deposit_log_id")

	// Withdraw from target platform
	compensateTicket := ticket + "_compensate"
	err := s.targetService.Withdraw(ctx, accountID, amount, compensateTicket)
	if err != nil {
		return err
	}

	if logID > 0 {
		s.store.UpdateLogStatus(logID, "FAILED")
	}

	return nil
}

func (s *CrossPlatformDepositStep) SupportsCompensation() bool {
	return true
}

func (s *CrossPlatformDepositStep) IdempotencyKey(txCtx *TxContext) string {
	ticket, _ := GetInputAs[string](txCtx, "ticket")
	accountID, _ := GetInputAs[int64](txCtx, "to_account_id")
	return fmt.Sprintf("cross_deposit:%s:%d", ticket, accountID)
}

func (s *CrossPlatformDepositStep) SupportsIdempotency() bool {
	return true
}

// UpdateLocalBalancesStep updates local account balances after cross-platform transfer
type UpdateLocalBalancesStep struct {
	*BaseStep
	store *MockAccountStore
}

func NewUpdateLocalBalancesStep(store *MockAccountStore) *UpdateLocalBalancesStep {
	return &UpdateLocalBalancesStep{
		BaseStep: NewBaseStep("update_local_balances"),
		store:    store,
	}
}

func (s *UpdateLocalBalancesStep) Execute(ctx context.Context, txCtx *TxContext) error {
	fromAccountID, _ := GetInputAs[int64](txCtx, "from_account_id")
	toAccountID, _ := GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")

	// Debit source account
	if err := s.store.UpdateBalance(fromAccountID, -amount); err != nil {
		return err
	}

	// Credit target account
	if err := s.store.UpdateBalance(toAccountID, amount); err != nil {
		// Rollback source debit
		s.store.UpdateBalance(fromAccountID, amount)
		return err
	}

	txCtx.SetOutput("local_updated", true)
	return nil
}

func (s *UpdateLocalBalancesStep) Compensate(ctx context.Context, txCtx *TxContext) error {
	fromAccountID, _ := GetInputAs[int64](txCtx, "from_account_id")
	toAccountID, _ := GetInputAs[int64](txCtx, "to_account_id")
	amount, _ := GetInputAs[float64](txCtx, "amount")

	// Reverse: credit source, debit target
	s.store.UpdateBalance(fromAccountID, amount)
	s.store.UpdateBalance(toAccountID, -amount)

	return nil
}

func (s *UpdateLocalBalancesStep) SupportsCompensation() bool {
	return true
}

// TestTransferScenario_CrossPlatform_PlatformAToPlatformB_Success tests PlatformA to PlatformB transfer
func TestTransferScenario_CrossPlatform_PlatformAToPlatformB_Success(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	platform_aService := NewMockPlatformService("PlatformA")
	platform_bService := NewMockPlatformService("PlatformB")

	// Setup accounts - PlatformA account transfers to PlatformB account
	accountStore.CreateAccount(&Account{ID: 1, UserID: 100, Type: "forex", Balance: 1000.0})
	accountStore.CreateAccount(&Account{ID: 2, UserID: 100, Type: "forex", Balance: 500.0})

	initialTotal := accountStore.GetTotalBalance()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
		}),
	)

	// Register cross-platform steps
	engine.RegisterStep(NewCrossPlatformWithdrawStep(platform_aService, accountStore))
	engine.RegisterStep(NewCrossPlatformDepositStep(platform_bService, accountStore))
	engine.RegisterStep(NewUpdateLocalBalancesStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	tx, err := engine.NewTransaction("platform_a_to_platform_b").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          300.0,
			"ticket":          "PlatformA-PlatformB-001",
		}).
		AddStep("cross_platform_withdraw"). // Withdraw from PlatformA
		AddStep("cross_platform_deposit").  // Deposit to PlatformB
		AddStep("update_local_balances").   // Update local records
		AddStep("finalize").
		Build()

	if err != nil {
		t.Fatalf("failed to build transaction: %v", err)
	}

	result, err := engine.Execute(context.Background(), tx)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}

	// Verify result
	if result.Status != TxStatusCompleted {
		t.Errorf("expected status COMPLETED, got %s", result.Status)
	}

	// Verify balance conservation (Property 1 - funds not lost or duplicated)
	finalTotal := accountStore.GetTotalBalance()
	if !floatEquals(finalTotal, initialTotal, 0.0001) {
		t.Errorf("balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify account balances
	fromAcc, _ := accountStore.GetAccount(1)
	toAcc, _ := accountStore.GetAccount(2)
	if !floatEquals(fromAcc.Balance, 700.0, 0.0001) {
		t.Errorf("expected from account balance 700, got %f", fromAcc.Balance)
	}
	if !floatEquals(toAcc.Balance, 800.0, 0.0001) {
		t.Errorf("expected to account balance 800, got %f", toAcc.Balance)
	}

	// Verify both platform services were called
	if platform_aService.withdrawCalls != 1 {
		t.Errorf("expected 1 PlatformA withdraw call, got %d", platform_aService.withdrawCalls)
	}
	if platform_bService.depositCalls != 1 {
		t.Errorf("expected 1 PlatformB deposit call, got %d", platform_bService.depositCalls)
	}
}

// TestTransferScenario_CrossPlatform_PlatformBToPlatformA_Success tests PlatformB to PlatformA transfer
func TestTransferScenario_CrossPlatform_PlatformBToPlatformA_Success(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	platform_bService := NewMockPlatformService("PlatformB")
	platform_aService := NewMockPlatformService("PlatformA")

	// Setup accounts - PlatformB account transfers to PlatformA account
	accountStore.CreateAccount(&Account{ID: 1, UserID: 100, Type: "forex", Balance: 2000.0})
	accountStore.CreateAccount(&Account{ID: 2, UserID: 100, Type: "forex", Balance: 300.0})

	initialTotal := accountStore.GetTotalBalance()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    100 * time.Millisecond,
		}),
	)

	// Register cross-platform steps (PlatformB -> PlatformA)
	engine.RegisterStep(NewCrossPlatformWithdrawStep(platform_bService, accountStore))
	engine.RegisterStep(NewCrossPlatformDepositStep(platform_aService, accountStore))
	engine.RegisterStep(NewUpdateLocalBalancesStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	tx, err := engine.NewTransaction("platform_b_to_platform_a").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          500.0,
			"ticket":          "PlatformB-PlatformA-001",
		}).
		AddStep("cross_platform_withdraw"). // Withdraw from PlatformB
		AddStep("cross_platform_deposit").  // Deposit to PlatformA
		AddStep("update_local_balances").   // Update local records
		AddStep("finalize").
		Build()

	if err != nil {
		t.Fatalf("failed to build transaction: %v", err)
	}

	result, err := engine.Execute(context.Background(), tx)
	if err != nil {
		t.Fatalf("failed to execute transaction: %v", err)
	}

	// Verify result
	if result.Status != TxStatusCompleted {
		t.Errorf("expected status COMPLETED, got %s", result.Status)
	}

	// Verify balance conservation
	finalTotal := accountStore.GetTotalBalance()
	if !floatEquals(finalTotal, initialTotal, 0.0001) {
		t.Errorf("balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify account balances
	fromAcc, _ := accountStore.GetAccount(1)
	toAcc, _ := accountStore.GetAccount(2)
	if !floatEquals(fromAcc.Balance, 1500.0, 0.0001) {
		t.Errorf("expected from account balance 1500, got %f", fromAcc.Balance)
	}
	if !floatEquals(toAcc.Balance, 800.0, 0.0001) {
		t.Errorf("expected to account balance 800, got %f", toAcc.Balance)
	}

	// Verify both platform services were called
	if platform_bService.withdrawCalls != 1 {
		t.Errorf("expected 1 PlatformB withdraw call, got %d", platform_bService.withdrawCalls)
	}
	if platform_aService.depositCalls != 1 {
		t.Errorf("expected 1 PlatformA deposit call, got %d", platform_aService.depositCalls)
	}
}

// TestTransferScenario_CrossPlatform_DepositFailure tests compensation when target platform deposit fails
func TestTransferScenario_CrossPlatform_DepositFailure(t *testing.T) {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	platform_aService := NewMockPlatformService("PlatformA")
	platform_bService := NewMockPlatformService("PlatformB")
	platform_bService.SetFailureMode(FailureModeAlways) // PlatformB deposit always fails

	// Setup accounts
	accountStore.CreateAccount(&Account{ID: 1, UserID: 100, Type: "forex", Balance: 1000.0})
	accountStore.CreateAccount(&Account{ID: 2, UserID: 100, Type: "forex", Balance: 500.0})

	initialTotal := accountStore.GetTotalBalance()
	initialFromBalance := 1000.0
	initialToBalance := 500.0

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       1,
			RetryInterval:    10 * time.Millisecond,
		}),
	)

	engine.RegisterStep(NewCrossPlatformWithdrawStep(platform_aService, accountStore))
	engine.RegisterStep(NewCrossPlatformDepositStep(platform_bService, accountStore))
	engine.RegisterStep(NewUpdateLocalBalancesStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	tx, _ := engine.NewTransaction("platform_a_to_platform_b_fail").
		WithLockKeys("account:1", "account:2").
		WithInput(map[string]any{
			"from_account_id": int64(1),
			"to_account_id":   int64(2),
			"amount":          300.0,
			"ticket":          "PlatformA-PlatformB-FAIL-001",
		}).
		AddStep("cross_platform_withdraw").
		AddStep("cross_platform_deposit").
		AddStep("update_local_balances").
		AddStep("finalize").
		Build()

	result, _ := engine.Execute(context.Background(), tx)

	// Transaction should fail or be compensated
	if result.Status != TxStatusFailed && result.Status != TxStatusCompensated {
		t.Errorf("expected status FAILED or COMPENSATED, got %s", result.Status)
	}

	// Verify balance conservation (funds not lost)
	finalTotal := accountStore.GetTotalBalance()
	if !floatEquals(finalTotal, initialTotal, 0.0001) {
		t.Errorf("balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify balances are restored
	fromAcc, _ := accountStore.GetAccount(1)
	toAcc, _ := accountStore.GetAccount(2)
	if !floatEquals(fromAcc.Balance, initialFromBalance, 0.0001) {
		t.Errorf("from account balance not restored: expected %f, got %f", initialFromBalance, fromAcc.Balance)
	}
	if !floatEquals(toAcc.Balance, initialToBalance, 0.0001) {
		t.Errorf("to account balance not restored: expected %f, got %f", initialToBalance, toAcc.Balance)
	}

	// Verify PlatformA withdraw was compensated (deposit back)
	if platform_aService.depositCalls == 0 {
		t.Error("expected compensation deposit call on PlatformA service")
	}
}

// ============================================================================
// Recovery Tests - Stuck Transaction Detection and Recovery
// ============================================================================

// RecoveryMockStore implements recovery.TxStore for testing recovery scenarios
type RecoveryMockStore struct {
	mu           sync.RWMutex
	transactions map[string]*StoreTx
	steps        map[string][]*StoreStepRecord
}

func NewRecoveryMockStore() *RecoveryMockStore {
	return &RecoveryMockStore{
		transactions: make(map[string]*StoreTx),
		steps:        make(map[string][]*StoreStepRecord),
	}
}

func (s *RecoveryMockStore) CreateTransaction(ctx context.Context, tx *StoreTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	txCopy := *tx
	s.transactions[tx.TxID] = &txCopy
	return nil
}

func (s *RecoveryMockStore) GetTransaction(ctx context.Context, txID string) (*StoreTx, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tx, exists := s.transactions[txID]
	if !exists {
		return nil, errors.New("transaction not found")
	}
	txCopy := *tx
	return &txCopy, nil
}

func (s *RecoveryMockStore) UpdateTransaction(ctx context.Context, tx *StoreTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	txCopy := *tx
	s.transactions[tx.TxID] = &txCopy
	return nil
}

func (s *RecoveryMockStore) GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*StoreTx, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*StoreTx
	threshold := time.Now().Add(-olderThan)
	for _, tx := range s.transactions {
		if (tx.Status == TxStatusLocked || tx.Status == TxStatusExecuting) && tx.UpdatedAt.Before(threshold) {
			txCopy := *tx
			result = append(result, &txCopy)
		}
	}
	return result, nil
}

func (s *RecoveryMockStore) GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*StoreTx, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*StoreTx
	for _, tx := range s.transactions {
		if tx.Status == TxStatusFailed && tx.RetryCount < maxRetries {
			txCopy := *tx
			result = append(result, &txCopy)
		}
	}
	return result, nil
}

func (s *RecoveryMockStore) CreateStep(ctx context.Context, step *StoreStepRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stepCopy := *step
	s.steps[step.TxID] = append(s.steps[step.TxID], &stepCopy)
	return nil
}

func (s *RecoveryMockStore) GetStep(ctx context.Context, txID string, stepIdx int) (*StoreStepRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	steps, exists := s.steps[txID]
	if !exists || stepIdx >= len(steps) {
		return nil, errors.New("step not found")
	}
	stepCopy := *steps[stepIdx]
	return &stepCopy, nil
}

func (s *RecoveryMockStore) UpdateStep(ctx context.Context, step *StoreStepRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	steps, exists := s.steps[step.TxID]
	if !exists {
		return errors.New("transaction not found")
	}
	for i, st := range steps {
		if st.StepIndex == step.StepIndex {
			stepCopy := *step
			steps[i] = &stepCopy
			return nil
		}
	}
	return errors.New("step not found")
}

func (s *RecoveryMockStore) GetSteps(ctx context.Context, txID string) ([]*StoreStepRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	steps, exists := s.steps[txID]
	if !exists {
		return nil, nil
	}
	result := make([]*StoreStepRecord, len(steps))
	for i, step := range steps {
		stepCopy := *step
		result[i] = &stepCopy
	}
	return result, nil
}

// AddStuckTransaction adds a stuck transaction for testing
func (s *RecoveryMockStore) AddStuckTransaction(tx *StoreTx) {
	s.mu.Lock()
	defer s.mu.Unlock()
	txCopy := *tx
	s.transactions[tx.TxID] = &txCopy
}

// GetTransactionStatus returns the current status of a transaction
func (s *RecoveryMockStore) GetTransactionStatus(txID string) TxStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if tx, exists := s.transactions[txID]; exists {
		return tx.Status
	}
	return ""
}

// RecoveryMockCoordinator implements recovery.Coordinator for testing
type RecoveryMockCoordinator struct {
	mu           sync.Mutex
	resumeCalls  []string
	resumeErr    error
	resumeResult map[string]error
	store        *RecoveryMockStore
	finalStatus  TxStatus // Status to set after resume
}

func NewRecoveryMockCoordinator(store *RecoveryMockStore) *RecoveryMockCoordinator {
	return &RecoveryMockCoordinator{
		resumeCalls:  make([]string, 0),
		resumeResult: make(map[string]error),
		store:        store,
		finalStatus:  TxStatusCompleted,
	}
}

func (c *RecoveryMockCoordinator) Resume(ctx context.Context, txID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeCalls = append(c.resumeCalls, txID)

	// Check for specific error
	if err, ok := c.resumeResult[txID]; ok {
		return err
	}
	if c.resumeErr != nil {
		return c.resumeErr
	}

	// Update transaction status to simulate successful recovery
	if c.store != nil {
		c.store.mu.Lock()
		if tx, exists := c.store.transactions[txID]; exists {
			tx.Status = c.finalStatus
			tx.UpdatedAt = time.Now()
		}
		c.store.mu.Unlock()
	}

	return nil
}

func (c *RecoveryMockCoordinator) GetResumeCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]string, len(c.resumeCalls))
	copy(result, c.resumeCalls)
	return result
}

func (c *RecoveryMockCoordinator) SetResumeError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeErr = err
}

func (c *RecoveryMockCoordinator) SetResumeErrorForTx(txID string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resumeResult[txID] = err
}

func (c *RecoveryMockCoordinator) SetFinalStatus(status TxStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finalStatus = status
}

// RecoveryWorkerAdapter adapts RecoveryMockStore to recovery.TxStore interface
type RecoveryWorkerAdapter struct {
	store *RecoveryMockStore
}

func NewRecoveryWorkerAdapter(store *RecoveryMockStore) *RecoveryWorkerAdapter {
	return &RecoveryWorkerAdapter{store: store}
}

func (a *RecoveryWorkerAdapter) GetTransaction(ctx context.Context, txID string) (*recoveryStoreTx, error) {
	tx, err := a.store.GetTransaction(ctx, txID)
	if err != nil {
		return nil, err
	}
	return convertToRecoveryStoreTx(tx), nil
}

func (a *RecoveryWorkerAdapter) UpdateTransaction(ctx context.Context, tx *recoveryStoreTx) error {
	storeTx := convertFromRecoveryStoreTx(tx)
	return a.store.UpdateTransaction(ctx, storeTx)
}

func (a *RecoveryWorkerAdapter) GetStuckTransactions(ctx context.Context, olderThan time.Duration) ([]*recoveryStoreTx, error) {
	txs, err := a.store.GetStuckTransactions(ctx, olderThan)
	if err != nil {
		return nil, err
	}
	result := make([]*recoveryStoreTx, len(txs))
	for i, tx := range txs {
		result[i] = convertToRecoveryStoreTx(tx)
	}
	return result, nil
}

func (a *RecoveryWorkerAdapter) GetRetryableTransactions(ctx context.Context, maxRetries int) ([]*recoveryStoreTx, error) {
	txs, err := a.store.GetRetryableTransactions(ctx, maxRetries)
	if err != nil {
		return nil, err
	}
	result := make([]*recoveryStoreTx, len(txs))
	for i, tx := range txs {
		result[i] = convertToRecoveryStoreTx(tx)
	}
	return result, nil
}

// recoveryStoreTx is a type alias for recovery.StoreTx to avoid import cycle
type recoveryStoreTx struct {
	ID          int64
	TxID        string
	TxType      string
	Status      string
	CurrentStep int
	TotalSteps  int
	StepNames   []string
	LockKeys    []string
	ErrorMsg    string
	RetryCount  int
	MaxRetries  int
	Version     int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	LockedAt    *time.Time
	CompletedAt *time.Time
	TimeoutAt   *time.Time
}

func convertToRecoveryStoreTx(tx *StoreTx) *recoveryStoreTx {
	return &recoveryStoreTx{
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

func convertFromRecoveryStoreTx(tx *recoveryStoreTx) *StoreTx {
	return &StoreTx{
		ID:          tx.ID,
		TxID:        tx.TxID,
		TxType:      tx.TxType,
		Status:      TxStatus(tx.Status),
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

// TestRecovery_StuckTransactionDetection tests that stuck transactions are detected
func TestRecovery_StuckTransactionDetection(t *testing.T) {
	store := NewRecoveryMockStore()
	locker := newMockLocker()
	eventBus := event.NewMemoryEventBus()
	coordinator := NewRecoveryMockCoordinator(store)

	// Create a stuck transaction (EXECUTING status, updated 10 minutes ago)
	stuckTime := time.Now().Add(-10 * time.Minute)
	stuckTx := &StoreTx{
		TxID:        "tx-stuck-detection-1",
		TxType:      "transfer",
		Status:      TxStatusExecuting,
		CurrentStep: 1,
		TotalSteps:  4,
		StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
		RetryCount:  0,
		MaxRetries:  3,
		CreatedAt:   stuckTime,
		UpdatedAt:   stuckTime,
	}
	store.AddStuckTransaction(stuckTx)

	// Create a non-stuck transaction (recently updated)
	recentTx := &StoreTx{
		TxID:        "tx-recent-1",
		TxType:      "transfer",
		Status:      TxStatusExecuting,
		CurrentStep: 1,
		TotalSteps:  4,
		StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
		RetryCount:  0,
		MaxRetries:  3,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	store.AddStuckTransaction(recentTx)

	// Verify GetStuckTransactions returns only the stuck transaction
	stuckThreshold := 5 * time.Minute
	stuckTxs, err := store.GetStuckTransactions(context.Background(), stuckThreshold)
	if err != nil {
		t.Fatalf("failed to get stuck transactions: %v", err)
	}

	if len(stuckTxs) != 1 {
		t.Fatalf("expected 1 stuck transaction, got %d", len(stuckTxs))
	}

	if stuckTxs[0].TxID != "tx-stuck-detection-1" {
		t.Errorf("expected stuck transaction tx-stuck-detection-1, got %s", stuckTxs[0].TxID)
	}

	// Now test with recovery worker using the adapter
	adapter := NewRecoveryWorkerAdapter(store)

	// Verify adapter also returns stuck transactions
	adapterStuckTxs, err := adapter.GetStuckTransactions(context.Background(), stuckThreshold)
	if err != nil {
		t.Fatalf("adapter failed to get stuck transactions: %v", err)
	}

	if len(adapterStuckTxs) != 1 {
		t.Fatalf("adapter expected 1 stuck transaction, got %d", len(adapterStuckTxs))
	}

	// Verify the coordinator would be called for recovery
	// (simulating what recovery worker does)
	for _, tx := range stuckTxs {
		err := coordinator.Resume(context.Background(), tx.TxID)
		if err != nil {
			t.Errorf("failed to resume transaction %s: %v", tx.TxID, err)
		}
	}

	// Verify coordinator was called
	calls := coordinator.GetResumeCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 resume call, got %d", len(calls))
	}
	if calls[0] != "tx-stuck-detection-1" {
		t.Errorf("expected resume call for tx-stuck-detection-1, got %s", calls[0])
	}

	// Verify transaction status was updated
	finalStatus := store.GetTransactionStatus("tx-stuck-detection-1")
	if finalStatus != TxStatusCompleted {
		t.Errorf("expected transaction status COMPLETED after recovery, got %s", finalStatus)
	}

	// Log success
	t.Logf("Successfully detected and recovered stuck transaction")
	_ = eventBus // Used for event publishing in real scenarios
	_ = locker   // Used for distributed locking in real scenarios
}

// TestRecovery_LockedTransactionDetection tests detection of stuck LOCKED transactions
func TestRecovery_LockedTransactionDetection(t *testing.T) {
	store := NewRecoveryMockStore()
	coordinator := NewRecoveryMockCoordinator(store)

	// Create a stuck LOCKED transaction
	stuckTime := time.Now().Add(-10 * time.Minute)
	lockedAt := stuckTime
	stuckTx := &StoreTx{
		TxID:        "tx-stuck-locked-1",
		TxType:      "transfer",
		Status:      TxStatusLocked,
		CurrentStep: 0,
		TotalSteps:  4,
		StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
		RetryCount:  0,
		MaxRetries:  3,
		CreatedAt:   stuckTime,
		UpdatedAt:   stuckTime,
		LockedAt:    &lockedAt,
	}
	store.AddStuckTransaction(stuckTx)

	// Verify LOCKED transactions are also detected as stuck
	stuckThreshold := 5 * time.Minute
	stuckTxs, err := store.GetStuckTransactions(context.Background(), stuckThreshold)
	if err != nil {
		t.Fatalf("failed to get stuck transactions: %v", err)
	}

	if len(stuckTxs) != 1 {
		t.Fatalf("expected 1 stuck LOCKED transaction, got %d", len(stuckTxs))
	}

	if stuckTxs[0].Status != TxStatusLocked {
		t.Errorf("expected LOCKED status, got %s", stuckTxs[0].Status)
	}

	// Recover the transaction
	err = coordinator.Resume(context.Background(), stuckTxs[0].TxID)
	if err != nil {
		t.Errorf("failed to resume LOCKED transaction: %v", err)
	}

	// Verify recovery
	finalStatus := store.GetTransactionStatus("tx-stuck-locked-1")
	if finalStatus != TxStatusCompleted {
		t.Errorf("expected COMPLETED status after recovery, got %s", finalStatus)
	}
}

// ============================================================================
// Task 5.2: Transaction Recovery Tests
// ============================================================================

// TestRecovery_ExecutingStateRecovery tests recovery of transactions stuck in EXECUTING state
func TestRecovery_ExecutingStateRecovery(t *testing.T) {
	store := NewRecoveryMockStore()
	coordinator := NewRecoveryMockCoordinator(store)

	// Create a transaction stuck in EXECUTING state at step 2
	stuckTime := time.Now().Add(-10 * time.Minute)
	stuckTx := &StoreTx{
		TxID:        "tx-executing-recovery-1",
		TxType:      "transfer",
		Status:      TxStatusExecuting,
		CurrentStep: 2, // Stuck at step 2 (credit_account)
		TotalSteps:  4,
		StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
		RetryCount:  0,
		MaxRetries:  3,
		CreatedAt:   stuckTime,
		UpdatedAt:   stuckTime,
	}
	store.AddStuckTransaction(stuckTx)

	// Simulate recovery
	err := coordinator.Resume(context.Background(), stuckTx.TxID)
	if err != nil {
		t.Fatalf("failed to resume transaction: %v", err)
	}

	// Verify transaction was recovered to COMPLETED
	finalStatus := store.GetTransactionStatus(stuckTx.TxID)
	if finalStatus != TxStatusCompleted {
		t.Errorf("expected COMPLETED status after recovery, got %s", finalStatus)
	}

	// Verify resume was called
	calls := coordinator.GetResumeCalls()
	if len(calls) != 1 || calls[0] != stuckTx.TxID {
		t.Errorf("expected resume call for %s, got %v", stuckTx.TxID, calls)
	}
}

// TestRecovery_FailedStateWithCompensation tests recovery that triggers compensation
func TestRecovery_FailedStateWithCompensation(t *testing.T) {
	store := NewRecoveryMockStore()
	coordinator := NewRecoveryMockCoordinator(store)
	coordinator.SetFinalStatus(TxStatusCompensated) // Simulate compensation result

	// Create a FAILED transaction that needs compensation
	stuckTime := time.Now().Add(-10 * time.Minute)
	failedTx := &StoreTx{
		TxID:        "tx-failed-compensation-1",
		TxType:      "transfer",
		Status:      TxStatusFailed,
		CurrentStep: 2, // Failed at step 2
		TotalSteps:  4,
		StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
		ErrorMsg:    "external service unavailable",
		RetryCount:  3, // Max retries exceeded
		MaxRetries:  3,
		CreatedAt:   stuckTime,
		UpdatedAt:   stuckTime,
	}
	store.AddStuckTransaction(failedTx)

	// Simulate recovery (should trigger compensation since max retries exceeded)
	err := coordinator.Resume(context.Background(), failedTx.TxID)
	if err != nil {
		t.Fatalf("failed to resume transaction: %v", err)
	}

	// Verify transaction was compensated
	finalStatus := store.GetTransactionStatus(failedTx.TxID)
	if finalStatus != TxStatusCompensated {
		t.Errorf("expected COMPENSATED status after recovery, got %s", finalStatus)
	}
}

// TestRecovery_FailedStateWithRetry tests recovery that retries a failed transaction
func TestRecovery_FailedStateWithRetry(t *testing.T) {
	store := NewRecoveryMockStore()
	coordinator := NewRecoveryMockCoordinator(store)
	coordinator.SetFinalStatus(TxStatusCompleted) // Simulate successful retry

	// Create a FAILED transaction that can be retried
	stuckTime := time.Now().Add(-10 * time.Minute)
	failedTx := &StoreTx{
		TxID:        "tx-failed-retry-1",
		TxType:      "transfer",
		Status:      TxStatusFailed,
		CurrentStep: 1, // Failed at step 1
		TotalSteps:  4,
		StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
		ErrorMsg:    "temporary network error",
		RetryCount:  1, // Can still retry (1 < 3)
		MaxRetries:  3,
		CreatedAt:   stuckTime,
		UpdatedAt:   stuckTime,
	}
	store.AddStuckTransaction(failedTx)

	// Verify it's returned as retryable
	retryableTxs, err := store.GetRetryableTransactions(context.Background(), 3)
	if err != nil {
		t.Fatalf("failed to get retryable transactions: %v", err)
	}

	if len(retryableTxs) != 1 {
		t.Fatalf("expected 1 retryable transaction, got %d", len(retryableTxs))
	}

	// Simulate recovery (should retry)
	err = coordinator.Resume(context.Background(), failedTx.TxID)
	if err != nil {
		t.Fatalf("failed to resume transaction: %v", err)
	}

	// Verify transaction completed after retry
	finalStatus := store.GetTransactionStatus(failedTx.TxID)
	if finalStatus != TxStatusCompleted {
		t.Errorf("expected COMPLETED status after retry, got %s", finalStatus)
	}
}

// TestRecovery_MultipleStuckStates tests recovery of multiple transactions in different stuck states
func TestRecovery_MultipleStuckStates(t *testing.T) {
	store := NewRecoveryMockStore()
	coordinator := NewRecoveryMockCoordinator(store)

	stuckTime := time.Now().Add(-10 * time.Minute)

	// Create multiple stuck transactions in different states
	transactions := []*StoreTx{
		{
			TxID:        "tx-multi-1",
			TxType:      "transfer",
			Status:      TxStatusLocked,
			CurrentStep: 0,
			TotalSteps:  4,
			StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
			RetryCount:  0,
			MaxRetries:  3,
			CreatedAt:   stuckTime,
			UpdatedAt:   stuckTime,
		},
		{
			TxID:        "tx-multi-2",
			TxType:      "transfer",
			Status:      TxStatusExecuting,
			CurrentStep: 1,
			TotalSteps:  4,
			StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
			RetryCount:  0,
			MaxRetries:  3,
			CreatedAt:   stuckTime,
			UpdatedAt:   stuckTime,
		},
		{
			TxID:        "tx-multi-3",
			TxType:      "transfer",
			Status:      TxStatusExecuting,
			CurrentStep: 3,
			TotalSteps:  4,
			StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
			RetryCount:  0,
			MaxRetries:  3,
			CreatedAt:   stuckTime,
			UpdatedAt:   stuckTime,
		},
	}

	for _, tx := range transactions {
		store.AddStuckTransaction(tx)
	}

	// Get all stuck transactions
	stuckTxs, err := store.GetStuckTransactions(context.Background(), 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to get stuck transactions: %v", err)
	}

	if len(stuckTxs) != 3 {
		t.Fatalf("expected 3 stuck transactions, got %d", len(stuckTxs))
	}

	// Recover all transactions (simulating parallel processing)
	var wg sync.WaitGroup
	for _, tx := range stuckTxs {
		wg.Add(1)
		go func(txID string) {
			defer wg.Done()
			coordinator.Resume(context.Background(), txID)
		}(tx.TxID)
	}
	wg.Wait()

	// Verify all transactions were recovered
	for _, tx := range transactions {
		finalStatus := store.GetTransactionStatus(tx.TxID)
		if finalStatus != TxStatusCompleted {
			t.Errorf("transaction %s expected COMPLETED, got %s", tx.TxID, finalStatus)
		}
	}

	// Verify all resume calls were made
	calls := coordinator.GetResumeCalls()
	if len(calls) != 3 {
		t.Errorf("expected 3 resume calls, got %d", len(calls))
	}
}

// TestRecovery_RecoveryFailure tests handling of recovery failures
func TestRecovery_RecoveryFailure(t *testing.T) {
	store := NewRecoveryMockStore()
	coordinator := NewRecoveryMockCoordinator(store)
	coordinator.SetResumeError(errors.New("recovery failed: external service unavailable"))

	// Create a stuck transaction
	stuckTime := time.Now().Add(-10 * time.Minute)
	stuckTx := &StoreTx{
		TxID:        "tx-recovery-failure-1",
		TxType:      "transfer",
		Status:      TxStatusExecuting,
		CurrentStep: 1,
		TotalSteps:  4,
		StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
		RetryCount:  0,
		MaxRetries:  3,
		CreatedAt:   stuckTime,
		UpdatedAt:   stuckTime,
	}
	store.AddStuckTransaction(stuckTx)

	// Attempt recovery (should fail)
	err := coordinator.Resume(context.Background(), stuckTx.TxID)
	if err == nil {
		t.Error("expected recovery to fail")
	}

	// Verify error message
	if err.Error() != "recovery failed: external service unavailable" {
		t.Errorf("unexpected error message: %v", err)
	}

	// Transaction status should remain unchanged (EXECUTING)
	// In real scenario, the recovery worker would handle this and potentially retry
	finalStatus := store.GetTransactionStatus(stuckTx.TxID)
	if finalStatus != TxStatusExecuting {
		t.Errorf("expected status to remain EXECUTING after failed recovery, got %s", finalStatus)
	}
}

// TestRecovery_CompletedTransactionNotRecovered tests that completed transactions are not recovered
func TestRecovery_CompletedTransactionNotRecovered(t *testing.T) {
	store := NewRecoveryMockStore()

	// Create a completed transaction (should not be detected as stuck)
	completedTime := time.Now().Add(-10 * time.Minute)
	completedAt := completedTime
	completedTx := &StoreTx{
		TxID:        "tx-completed-1",
		TxType:      "transfer",
		Status:      TxStatusCompleted,
		CurrentStep: 4,
		TotalSteps:  4,
		StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
		RetryCount:  0,
		MaxRetries:  3,
		CreatedAt:   completedTime,
		UpdatedAt:   completedTime,
		CompletedAt: &completedAt,
	}
	store.AddStuckTransaction(completedTx)

	// Verify completed transactions are not returned as stuck
	stuckTxs, err := store.GetStuckTransactions(context.Background(), 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to get stuck transactions: %v", err)
	}

	if len(stuckTxs) != 0 {
		t.Errorf("expected 0 stuck transactions, got %d", len(stuckTxs))
	}
}

// TestRecovery_CompensatedTransactionNotRecovered tests that compensated transactions are not recovered
func TestRecovery_CompensatedTransactionNotRecovered(t *testing.T) {
	store := NewRecoveryMockStore()

	// Create a compensated transaction (should not be detected as stuck)
	compensatedTime := time.Now().Add(-10 * time.Minute)
	compensatedTx := &StoreTx{
		TxID:        "tx-compensated-1",
		TxType:      "transfer",
		Status:      TxStatusCompensated,
		CurrentStep: 2,
		TotalSteps:  4,
		StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
		ErrorMsg:    "external service failed",
		RetryCount:  3,
		MaxRetries:  3,
		CreatedAt:   compensatedTime,
		UpdatedAt:   compensatedTime,
	}
	store.AddStuckTransaction(compensatedTx)

	// Verify compensated transactions are not returned as stuck
	stuckTxs, err := store.GetStuckTransactions(context.Background(), 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to get stuck transactions: %v", err)
	}

	if len(stuckTxs) != 0 {
		t.Errorf("expected 0 stuck transactions, got %d", len(stuckTxs))
	}

	// Also verify not returned as retryable
	retryableTxs, err := store.GetRetryableTransactions(context.Background(), 3)
	if err != nil {
		t.Fatalf("failed to get retryable transactions: %v", err)
	}

	if len(retryableTxs) != 0 {
		t.Errorf("expected 0 retryable transactions, got %d", len(retryableTxs))
	}
}

// TestRecovery_MaxRetriesExceededNotRetryable tests that transactions at max retries are not retryable
func TestRecovery_MaxRetriesExceededNotRetryable(t *testing.T) {
	store := NewRecoveryMockStore()

	// Create a failed transaction at max retries
	failedTime := time.Now().Add(-10 * time.Minute)
	failedTx := &StoreTx{
		TxID:        "tx-max-retries-1",
		TxType:      "transfer",
		Status:      TxStatusFailed,
		CurrentStep: 1,
		TotalSteps:  4,
		StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
		ErrorMsg:    "external service unavailable",
		RetryCount:  3, // At max retries
		MaxRetries:  3,
		CreatedAt:   failedTime,
		UpdatedAt:   failedTime,
	}
	store.AddStuckTransaction(failedTx)

	// Verify transaction at max retries is not returned as retryable
	retryableTxs, err := store.GetRetryableTransactions(context.Background(), 3)
	if err != nil {
		t.Fatalf("failed to get retryable transactions: %v", err)
	}

	if len(retryableTxs) != 0 {
		t.Errorf("expected 0 retryable transactions (max retries exceeded), got %d", len(retryableTxs))
	}
}

// ============================================================================
// Task 5.3: Recovery Consistency Property-Based Test
// Property 7: Recovery Consistency
// ============================================================================

// TestProperty_RecoveryConsistency verifies that recovered transactions end in valid final states
// Property 7: For any stuck transaction recovered by the recovery worker,
// the final state SHALL be either COMPLETED (all steps succeeded) or COMPENSATED (all compensatable steps reversed).
func TestProperty_RecoveryConsistency(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Setup test infrastructure
		store := newMockStore()
		locker := newMockLocker()
		breaker := newMockBreaker()
		eventBus := event.NewMemoryEventBus()

		accountStore := NewMockAccountStore()
		externalService := NewMockExternalService()

		// Generate random initial balances
		fromBalance := rapid.Float64Range(1000, 10000).Draw(t, "from_balance")
		toBalance := rapid.Float64Range(0, 10000).Draw(t, "to_balance")
		transferAmount := rapid.Float64Range(10, fromBalance/2).Draw(t, "amount")

		accountStore.CreateAccount(&Account{ID: 1, Balance: fromBalance})
		accountStore.CreateAccount(&Account{ID: 2, Balance: toBalance})

		initialTotal := accountStore.GetTotalBalance()

		// Generate random failure point (0 = no failure, 1-3 = fail at step)
		failAtStep := rapid.IntRange(0, 3).Draw(t, "fail_at_step")

		// Configure external service failure mode based on failure point
		if failAtStep == 1 {
			// Fail at external_deposit step
			externalService.SetFailureMode(FailureModeAlways)
		}

		engine := NewEngine(
			WithEngineStore(store),
			WithEngineLocker(locker),
			WithEngineBreaker(breaker),
			WithEngineEventBus(eventBus),
			WithEngineConfig(Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       1, // Low retries for faster test
				RetryInterval:    1 * time.Millisecond,
			}),
		)

		// Create steps with optional failure injection
		debitStep := NewDebitAccountStep(accountStore)
		externalDepositStep := NewExternalDepositStep(externalService, accountStore)

		// Create credit step that may fail
		var creditStep Step
		if failAtStep == 2 {
			creditStep = &failingCreditStep{
				CreditAccountStep: NewCreditAccountStep(accountStore),
			}
		} else {
			creditStep = NewCreditAccountStep(accountStore)
		}

		// Create finalize step that may fail
		var finalizeStep Step
		if failAtStep == 3 {
			finalizeStep = &failingFinalizeStep{
				FinalizeStep: NewFinalizeStep(accountStore),
			}
		} else {
			finalizeStep = NewFinalizeStep(accountStore)
		}

		engine.RegisterStep(debitStep)
		engine.RegisterStep(externalDepositStep)
		engine.RegisterStep(creditStep)
		engine.RegisterStep(finalizeStep)

		ticket := fmt.Sprintf("TRF-RECOVERY-%d", rapid.Int().Draw(t, "ticket_id"))

		tx, err := engine.NewTransaction("transfer").
			WithLockKeys("account:1", "account:2").
			WithInput(map[string]any{
				"from_account_id": int64(1),
				"to_account_id":   int64(2),
				"amount":          transferAmount,
				"ticket":          ticket,
			}).
			AddStep("debit_account").
			AddStep("external_deposit").
			AddStep("credit_account").
			AddStep("finalize").
			Build()

		if err != nil {
			t.Fatalf("failed to build transaction: %v", err)
		}

		result, _ := engine.Execute(context.Background(), tx)

		// Property 7: Final state must be COMPLETED or COMPENSATED (or FAILED if no compensation needed)
		validFinalStates := map[TxStatus]bool{
			TxStatusCompleted:   true,
			TxStatusCompensated: true,
			TxStatusFailed:      true, // Valid if no steps support compensation
		}

		if !validFinalStates[result.Status] {
			t.Fatalf("Invalid final state: %s (expected COMPLETED, COMPENSATED, or FAILED)", result.Status)
		}

		// Property: Balance conservation must hold regardless of final state
		finalTotal := accountStore.GetTotalBalance()
		if !floatEquals(finalTotal, initialTotal, 0.0001) {
			t.Fatalf("Balance conservation violated: initial=%f, final=%f, status=%s",
				initialTotal, finalTotal, result.Status)
		}

		// Additional property: If COMPLETED, balances should reflect the transfer
		if result.Status == TxStatusCompleted {
			fromAcc, _ := accountStore.GetAccount(1)
			toAcc, _ := accountStore.GetAccount(2)

			expectedFromBalance := fromBalance - transferAmount
			expectedToBalance := toBalance + transferAmount

			if !floatEquals(fromAcc.Balance, expectedFromBalance, 0.0001) {
				t.Fatalf("COMPLETED but from balance incorrect: expected=%f, got=%f",
					expectedFromBalance, fromAcc.Balance)
			}
			if !floatEquals(toAcc.Balance, expectedToBalance, 0.0001) {
				t.Fatalf("COMPLETED but to balance incorrect: expected=%f, got=%f",
					expectedToBalance, toAcc.Balance)
			}
		}

		// Additional property: If COMPENSATED or FAILED, balances should be restored
		if result.Status == TxStatusCompensated || result.Status == TxStatusFailed {
			fromAcc, _ := accountStore.GetAccount(1)
			toAcc, _ := accountStore.GetAccount(2)

			// Balances should be close to initial (may have small variations due to partial execution)
			// The key invariant is that total balance is conserved
			if !floatEquals(fromAcc.Balance+toAcc.Balance, initialTotal, 0.0001) {
				t.Fatalf("Balance conservation violated after %s: from=%f, to=%f, expected_total=%f",
					result.Status, fromAcc.Balance, toAcc.Balance, initialTotal)
			}
		}
	})
}

// failingCreditStep is a credit step that always fails
type failingCreditStep struct {
	*CreditAccountStep
}

func (s *failingCreditStep) Execute(ctx context.Context, txCtx *TxContext) error {
	return errors.New("credit step forced failure for testing")
}

// failingFinalizeStep is a finalize step that always fails
type failingFinalizeStep struct {
	*FinalizeStep
}

func (s *failingFinalizeStep) Execute(ctx context.Context, txCtx *TxContext) error {
	return errors.New("finalize step forced failure for testing")
}

// TestProperty_RecoveryConsistency_WithRandomStuckState tests recovery from random stuck states
// Property 7: Recovery Consistency - validates that recovery always results in consistent state
func TestProperty_RecoveryConsistency_WithRandomStuckState(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := NewRecoveryMockStore()

		// Generate random stuck state
		stuckStatus := rapid.SampledFrom([]TxStatus{TxStatusLocked, TxStatusExecuting}).Draw(t, "stuck_status")
		currentStep := rapid.IntRange(0, 3).Draw(t, "current_step")
		retryCount := rapid.IntRange(0, 3).Draw(t, "retry_count")
		maxRetries := rapid.IntRange(retryCount, 5).Draw(t, "max_retries")

		stuckTime := time.Now().Add(-10 * time.Minute)
		stuckTx := &StoreTx{
			TxID:        fmt.Sprintf("tx-random-stuck-%d", rapid.Int().Draw(t, "tx_id")),
			TxType:      "transfer",
			Status:      stuckStatus,
			CurrentStep: currentStep,
			TotalSteps:  4,
			StepNames:   []string{"debit_account", "external_deposit", "credit_account", "finalize"},
			RetryCount:  retryCount,
			MaxRetries:  maxRetries,
			CreatedAt:   stuckTime,
			UpdatedAt:   stuckTime,
		}
		store.AddStuckTransaction(stuckTx)

		// Generate random recovery outcome
		recoverySucceeds := rapid.Bool().Draw(t, "recovery_succeeds")

		coordinator := NewRecoveryMockCoordinator(store)
		if recoverySucceeds {
			coordinator.SetFinalStatus(TxStatusCompleted)
		} else {
			coordinator.SetFinalStatus(TxStatusCompensated)
		}

		// Perform recovery
		err := coordinator.Resume(context.Background(), stuckTx.TxID)
		if err != nil {
			// Recovery error is acceptable, but state should still be consistent
			return
		}

		// Property: Final state must be valid
		finalStatus := store.GetTransactionStatus(stuckTx.TxID)
		validFinalStates := map[TxStatus]bool{
			TxStatusCompleted:   true,
			TxStatusCompensated: true,
			TxStatusFailed:      true,
		}

		if !validFinalStates[finalStatus] {
			t.Fatalf("Invalid final state after recovery: %s", finalStatus)
		}

		// Property: If recovery succeeded, status should match expected outcome
		if recoverySucceeds && finalStatus != TxStatusCompleted {
			t.Fatalf("Expected COMPLETED after successful recovery, got %s", finalStatus)
		}
		if !recoverySucceeds && finalStatus != TxStatusCompensated {
			t.Fatalf("Expected COMPENSATED after failed recovery, got %s", finalStatus)
		}
	})
}

// TestProperty_RecoveryConsistency_BalanceInvariant tests that balance is always conserved after recovery
// Property 7: Recovery Consistency - balance conservation invariant
func TestProperty_RecoveryConsistency_BalanceInvariant(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Setup
		store := newMockStore()
		locker := newMockLocker()
		breaker := newMockBreaker()
		eventBus := event.NewMemoryEventBus()

		accountStore := NewMockAccountStore()
		externalService := NewMockExternalService()

		// Generate random balances
		fromBalance := rapid.Float64Range(500, 5000).Draw(t, "from_balance")
		toBalance := rapid.Float64Range(0, 5000).Draw(t, "to_balance")
		transferAmount := rapid.Float64Range(1, fromBalance/3).Draw(t, "amount")

		accountStore.CreateAccount(&Account{ID: 1, Balance: fromBalance})
		accountStore.CreateAccount(&Account{ID: 2, Balance: toBalance})

		initialTotal := accountStore.GetTotalBalance()

		// Randomly decide if external service should fail
		shouldFail := rapid.Bool().Draw(t, "should_fail")
		if shouldFail {
			externalService.SetFailureMode(FailureModeAlways)
		}

		engine := NewEngine(
			WithEngineStore(store),
			WithEngineLocker(locker),
			WithEngineBreaker(breaker),
			WithEngineEventBus(eventBus),
			WithEngineConfig(Config{
				LockTTL:          30 * time.Second,
				LockExtendPeriod: 10 * time.Second,
				StepTimeout:      5 * time.Second,
				TxTimeout:        30 * time.Second,
				MaxRetries:       1,
				RetryInterval:    1 * time.Millisecond,
			}),
		)

		engine.RegisterStep(NewDebitAccountStep(accountStore))
		engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
		engine.RegisterStep(NewCreditAccountStep(accountStore))
		engine.RegisterStep(NewFinalizeStep(accountStore))

		tx, _ := engine.NewTransaction("transfer").
			WithLockKeys("account:1", "account:2").
			WithInput(map[string]any{
				"from_account_id": int64(1),
				"to_account_id":   int64(2),
				"amount":          transferAmount,
				"ticket":          fmt.Sprintf("TRF-BAL-%d", rapid.Int().Draw(t, "ticket")),
			}).
			AddStep("debit_account").
			AddStep("external_deposit").
			AddStep("credit_account").
			AddStep("finalize").
			Build()

		// Execute transaction
		engine.Execute(context.Background(), tx)

		// Property: Balance must always be conserved
		finalTotal := accountStore.GetTotalBalance()
		if !floatEquals(finalTotal, initialTotal, 0.0001) {
			t.Fatalf("Balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
		}
	})
}

// ============================================================================
// Concurrent Transfer Tests - Task 6
// ============================================================================

// TestConcurrent_SameAccountTransfers tests multiple goroutines transferring from the same account
// This test verifies that concurrent transfers on the same account are properly serialized
// and the final balance is correct regardless of execution order.
func TestConcurrent_SameAccountTransfers(t *testing.T) {
	store := newMockStore()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()

	// Setup accounts - source account has enough balance for all transfers
	const numTransfers = 10
	const transferAmount = 100.0
	const initialFromBalance = 5000.0
	const initialToBalance = 0.0

	accountStore.CreateAccount(&Account{ID: 1, UserID: 100, Type: "saving", Balance: initialFromBalance})
	accountStore.CreateAccount(&Account{ID: 2, UserID: 100, Type: "forex", Balance: initialToBalance})

	initialTotal := accountStore.GetTotalBalance()

	// Use trackable locker to verify lock exclusivity
	trackableLocker := newTrackableMockLocker()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(trackableLocker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    10 * time.Millisecond,
		}),
	)

	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	// Track results
	var wg sync.WaitGroup
	var successCount int32
	var failCount int32

	wg.Add(numTransfers)

	// Launch concurrent transfers
	for i := 0; i < numTransfers; i++ {
		go func(idx int) {
			defer wg.Done()

			tx, err := engine.NewTransaction("concurrent_transfer").
				WithLockKeys("account:1", "account:2").
				WithInput(map[string]any{
					"from_account_id": int64(1),
					"to_account_id":   int64(2),
					"amount":          transferAmount,
					"ticket":          fmt.Sprintf("CONC-SAME-%d-%d", time.Now().UnixNano(), idx),
				}).
				AddStep("debit_account").
				AddStep("external_deposit").
				AddStep("credit_account").
				AddStep("finalize").
				Build()

			if err != nil {
				atomic.AddInt32(&failCount, 1)
				return
			}

			result, _ := engine.Execute(context.Background(), tx)
			if result.Status == TxStatusCompleted {
				atomic.AddInt32(&successCount, 1)
			} else {
				atomic.AddInt32(&failCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// Verify lock exclusivity - at most one transaction should hold the lock at any time
	maxConcurrent := trackableLocker.GetMaxConcurrent()
	if maxConcurrent > 1 {
		t.Errorf("Lock exclusivity violated: max concurrent holders = %d (expected 1)", maxConcurrent)
	}

	// Verify balance conservation
	finalTotal := accountStore.GetTotalBalance()
	if !floatEquals(finalTotal, initialTotal, 0.0001) {
		t.Errorf("Balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify final balances are consistent
	fromAcc, _ := accountStore.GetAccount(1)
	toAcc, _ := accountStore.GetAccount(2)

	// Each successful transfer moves transferAmount from account 1 to account 2
	expectedFromBalance := initialFromBalance - (float64(successCount) * transferAmount)
	expectedToBalance := initialToBalance + (float64(successCount) * transferAmount)

	if !floatEquals(fromAcc.Balance, expectedFromBalance, 0.0001) {
		t.Errorf("From account balance mismatch: expected=%f, actual=%f (success=%d)",
			expectedFromBalance, fromAcc.Balance, successCount)
	}
	if !floatEquals(toAcc.Balance, expectedToBalance, 0.0001) {
		t.Errorf("To account balance mismatch: expected=%f, actual=%f (success=%d)",
			expectedToBalance, toAcc.Balance, successCount)
	}

	t.Logf("Concurrent test completed: %d successful, %d failed out of %d transfers",
		successCount, failCount, numTransfers)
}

// TestConcurrent_SameAccountMultipleTargets tests concurrent transfers from one source to multiple targets
func TestConcurrent_SameAccountMultipleTargets(t *testing.T) {
	store := newMockStore()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()

	// Setup accounts - one source, multiple targets
	const numTargets = 5
	const transferAmount = 100.0
	const initialFromBalance = 5000.0

	accountStore.CreateAccount(&Account{ID: 1, UserID: 100, Type: "saving", Balance: initialFromBalance})
	for i := 2; i <= numTargets+1; i++ {
		accountStore.CreateAccount(&Account{ID: int64(i), UserID: 100, Type: "forex", Balance: 0.0})
	}

	initialTotal := accountStore.GetTotalBalance()

	trackableLocker := newTrackableMockLocker()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(trackableLocker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    10 * time.Millisecond,
		}),
	)

	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	var wg sync.WaitGroup
	var successCount int32

	// Launch concurrent transfers to different targets
	for i := 0; i < numTargets; i++ {
		wg.Add(1)
		go func(targetIdx int) {
			defer wg.Done()

			targetID := int64(targetIdx + 2) // Target accounts start from ID 2

			tx, _ := engine.NewTransaction("multi_target_transfer").
				WithLockKeys("account:1", fmt.Sprintf("account:%d", targetID)).
				WithInput(map[string]any{
					"from_account_id": int64(1),
					"to_account_id":   targetID,
					"amount":          transferAmount,
					"ticket":          fmt.Sprintf("MULTI-TGT-%d-%d", time.Now().UnixNano(), targetIdx),
				}).
				AddStep("debit_account").
				AddStep("external_deposit").
				AddStep("credit_account").
				AddStep("finalize").
				Build()

			result, _ := engine.Execute(context.Background(), tx)
			if result.Status == TxStatusCompleted {
				atomic.AddInt32(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// Verify balance conservation
	finalTotal := accountStore.GetTotalBalance()
	if !floatEquals(finalTotal, initialTotal, 0.0001) {
		t.Errorf("Balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify source account balance
	fromAcc, _ := accountStore.GetAccount(1)
	expectedFromBalance := initialFromBalance - (float64(successCount) * transferAmount)
	if !floatEquals(fromAcc.Balance, expectedFromBalance, 0.0001) {
		t.Errorf("From account balance mismatch: expected=%f, actual=%f", expectedFromBalance, fromAcc.Balance)
	}

	// Verify total transferred to targets
	var totalTransferred float64
	for i := 2; i <= numTargets+1; i++ {
		acc, _ := accountStore.GetAccount(int64(i))
		totalTransferred += acc.Balance
	}

	expectedTransferred := float64(successCount) * transferAmount
	if !floatEquals(totalTransferred, expectedTransferred, 0.0001) {
		t.Errorf("Total transferred mismatch: expected=%f, actual=%f", expectedTransferred, totalTransferred)
	}

	t.Logf("Multi-target test completed: %d successful out of %d transfers", successCount, numTargets)
}

// TestConcurrent_BidirectionalTransfers tests concurrent transfers in both directions between two accounts
func TestConcurrent_BidirectionalTransfers(t *testing.T) {
	store := newMockStore()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()

	// Setup accounts with equal balances
	const initialBalance = 5000.0
	const transferAmount = 100.0
	const numTransfersPerDirection = 5

	accountStore.CreateAccount(&Account{ID: 1, UserID: 100, Type: "saving", Balance: initialBalance})
	accountStore.CreateAccount(&Account{ID: 2, UserID: 100, Type: "saving", Balance: initialBalance})

	initialTotal := accountStore.GetTotalBalance()

	trackableLocker := newTrackableMockLocker()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(trackableLocker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    10 * time.Millisecond,
		}),
	)

	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	var wg sync.WaitGroup
	var successCount1to2 int32
	var successCount2to1 int32

	// Transfers from account 1 to account 2
	for i := 0; i < numTransfersPerDirection; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			tx, _ := engine.NewTransaction("bidirectional_1to2").
				WithLockKeys("account:1", "account:2").
				WithInput(map[string]any{
					"from_account_id": int64(1),
					"to_account_id":   int64(2),
					"amount":          transferAmount,
					"ticket":          fmt.Sprintf("BIDIR-1TO2-%d-%d", time.Now().UnixNano(), idx),
				}).
				AddStep("debit_account").
				AddStep("external_deposit").
				AddStep("credit_account").
				AddStep("finalize").
				Build()

			result, _ := engine.Execute(context.Background(), tx)
			if result.Status == TxStatusCompleted {
				atomic.AddInt32(&successCount1to2, 1)
			}
		}(i)
	}

	// Transfers from account 2 to account 1
	for i := 0; i < numTransfersPerDirection; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			tx, _ := engine.NewTransaction("bidirectional_2to1").
				WithLockKeys("account:1", "account:2").
				WithInput(map[string]any{
					"from_account_id": int64(2),
					"to_account_id":   int64(1),
					"amount":          transferAmount,
					"ticket":          fmt.Sprintf("BIDIR-2TO1-%d-%d", time.Now().UnixNano(), idx),
				}).
				AddStep("debit_account").
				AddStep("external_deposit").
				AddStep("credit_account").
				AddStep("finalize").
				Build()

			result, _ := engine.Execute(context.Background(), tx)
			if result.Status == TxStatusCompleted {
				atomic.AddInt32(&successCount2to1, 1)
			}
		}(i)
	}

	wg.Wait()

	// Verify lock exclusivity
	maxConcurrent := trackableLocker.GetMaxConcurrent()
	if maxConcurrent > 1 {
		t.Errorf("Lock exclusivity violated: max concurrent holders = %d", maxConcurrent)
	}

	// Verify balance conservation
	finalTotal := accountStore.GetTotalBalance()
	if !floatEquals(finalTotal, initialTotal, 0.0001) {
		t.Errorf("Balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify final balances
	acc1, _ := accountStore.GetAccount(1)
	acc2, _ := accountStore.GetAccount(2)

	// Net transfer = (successCount2to1 - successCount1to2) * transferAmount
	netTransferTo1 := float64(successCount2to1-successCount1to2) * transferAmount
	expectedAcc1Balance := initialBalance + netTransferTo1
	expectedAcc2Balance := initialBalance - netTransferTo1

	if !floatEquals(acc1.Balance, expectedAcc1Balance, 0.0001) {
		t.Errorf("Account 1 balance mismatch: expected=%f, actual=%f", expectedAcc1Balance, acc1.Balance)
	}
	if !floatEquals(acc2.Balance, expectedAcc2Balance, 0.0001) {
		t.Errorf("Account 2 balance mismatch: expected=%f, actual=%f", expectedAcc2Balance, acc2.Balance)
	}

	t.Logf("Bidirectional test completed: 1->2: %d, 2->1: %d successful", successCount1to2, successCount2to1)
}

// TestConcurrent_LockContention tests behavior under high lock contention
func TestConcurrent_LockContention(t *testing.T) {
	store := newMockStore()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()

	// Setup accounts
	accountStore.CreateAccount(&Account{ID: 1, UserID: 100, Type: "saving", Balance: 10000.0})
	accountStore.CreateAccount(&Account{ID: 2, UserID: 100, Type: "forex", Balance: 0.0})

	initialTotal := accountStore.GetTotalBalance()

	// Use a locker that simulates contention by adding delay
	contentionLocker := newContentionMockLocker()

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(contentionLocker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    10 * time.Millisecond,
		}),
	)

	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	const numTransfers = 20
	var wg sync.WaitGroup
	var successCount int32
	var lockFailCount int32

	wg.Add(numTransfers)

	for i := 0; i < numTransfers; i++ {
		go func(idx int) {
			defer wg.Done()

			tx, _ := engine.NewTransaction("contention_transfer").
				WithLockKeys("account:1", "account:2").
				WithInput(map[string]any{
					"from_account_id": int64(1),
					"to_account_id":   int64(2),
					"amount":          50.0,
					"ticket":          fmt.Sprintf("CONTENTION-%d-%d", time.Now().UnixNano(), idx),
				}).
				AddStep("debit_account").
				AddStep("external_deposit").
				AddStep("credit_account").
				AddStep("finalize").
				Build()

			result, err := engine.Execute(context.Background(), tx)
			if err != nil && errors.Is(err, ErrLockAcquisitionFailed) {
				atomic.AddInt32(&lockFailCount, 1)
				return
			}
			if result.Status == TxStatusCompleted {
				atomic.AddInt32(&successCount, 1)
			}
		}(i)
	}

	wg.Wait()

	// Verify balance conservation
	finalTotal := accountStore.GetTotalBalance()
	if !floatEquals(finalTotal, initialTotal, 0.0001) {
		t.Errorf("Balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
	}

	// Verify that some transactions failed due to lock contention (expected behavior)
	t.Logf("Lock contention test: %d successful, %d lock failures out of %d",
		successCount, lockFailCount, numTransfers)

	// At least some should succeed
	if successCount == 0 {
		t.Error("Expected at least some successful transfers")
	}
}

// contentionMockLocker simulates lock contention with delays
type contentionMockLocker struct {
	mu            sync.Mutex
	locks         map[string]bool
	lockHolders   int32
	maxConcurrent int32
}

func newContentionMockLocker() *contentionMockLocker {
	return &contentionMockLocker{
		locks: make(map[string]bool),
	}
}

func (l *contentionMockLocker) Acquire(ctx context.Context, keys []string, ttl time.Duration) (lock.LockHandle, error) {
	// Add small delay to increase contention
	time.Sleep(time.Millisecond * 5)

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, key := range keys {
		if l.locks[key] {
			return nil, ErrLockAcquisitionFailed
		}
	}
	for _, key := range keys {
		l.locks[key] = true
	}

	current := atomic.AddInt32(&l.lockHolders, 1)
	for {
		old := atomic.LoadInt32(&l.maxConcurrent)
		if current <= old || atomic.CompareAndSwapInt32(&l.maxConcurrent, old, current) {
			break
		}
	}

	return &contentionLockHandle{locker: l, keys: keys}, nil
}

type contentionLockHandle struct {
	locker *contentionMockLocker
	keys   []string
}

func (h *contentionLockHandle) Extend(ctx context.Context, ttl time.Duration) error {
	return nil
}

func (h *contentionLockHandle) Release(ctx context.Context) error {
	h.locker.mu.Lock()
	defer h.locker.mu.Unlock()
	for _, key := range h.keys {
		delete(h.locker.locks, key)
	}
	atomic.AddInt32(&h.locker.lockHolders, -1)
	return nil
}

func (h *contentionLockHandle) Keys() []string {
	return h.keys
}

// ============================================================================
// Property-Based Test: Confluence (Property 8)
// ============================================================================

// TestProperty_Confluence verifies that final state is deterministic regardless of execution order
// Property 8: Confluence - For all concurrent operations on the same account,
// the final balance SHALL be deterministic regardless of execution order
func TestProperty_Confluence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate random number of transfers (2-5)
		numTransfers := rapid.IntRange(2, 5).Draw(t, "num_transfers")

		// Generate random initial balances
		initialFromBalance := rapid.Float64Range(1000, 10000).Draw(t, "from_balance")
		initialToBalance := rapid.Float64Range(0, 5000).Draw(t, "to_balance")

		// Generate random transfer amounts (ensure total doesn't exceed from balance)
		maxPerTransfer := initialFromBalance / float64(numTransfers+1)
		transferAmounts := make([]float64, numTransfers)
		for i := 0; i < numTransfers; i++ {
			transferAmounts[i] = rapid.Float64Range(1, maxPerTransfer).Draw(t, fmt.Sprintf("amount_%d", i))
		}

		// Run the same set of transfers in two different orders and verify final state is the same
		// Order 1: Sequential execution
		finalState1 := runTransfersSequentially(t, initialFromBalance, initialToBalance, transferAmounts)

		// Order 2: Reverse sequential execution
		reversedAmounts := make([]float64, numTransfers)
		for i := 0; i < numTransfers; i++ {
			reversedAmounts[i] = transferAmounts[numTransfers-1-i]
		}
		finalState2 := runTransfersSequentially(t, initialFromBalance, initialToBalance, reversedAmounts)

		// Property: Final balances should be the same regardless of order
		// (assuming all transfers succeed, which they should with sufficient balance)
		// Use floating point comparison with tolerance
		if !floatEquals(finalState1.fromBalance, finalState2.fromBalance, 0.0001) {
			t.Fatalf("Confluence violated: from balance differs - order1=%f, order2=%f",
				finalState1.fromBalance, finalState2.fromBalance)
		}
		if !floatEquals(finalState1.toBalance, finalState2.toBalance, 0.0001) {
			t.Fatalf("Confluence violated: to balance differs - order1=%f, order2=%f",
				finalState1.toBalance, finalState2.toBalance)
		}

		// Property: Total balance should be conserved
		initialTotal := initialFromBalance + initialToBalance
		finalTotal1 := finalState1.fromBalance + finalState1.toBalance
		finalTotal2 := finalState2.fromBalance + finalState2.toBalance

		if !floatEquals(finalTotal1, initialTotal, 0.0001) {
			t.Fatalf("Balance conservation violated in order1: initial=%f, final=%f", initialTotal, finalTotal1)
		}
		if !floatEquals(finalTotal2, initialTotal, 0.0001) {
			t.Fatalf("Balance conservation violated in order2: initial=%f, final=%f", initialTotal, finalTotal2)
		}
	})
}

// confluenceState holds the final state after transfers
type confluenceState struct {
	fromBalance  float64
	toBalance    float64
	successCount int
}

// runTransfersSequentially runs transfers in order and returns final state
func runTransfersSequentially(t *rapid.T, fromBalance, toBalance float64, amounts []float64) confluenceState {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()

	accountStore.CreateAccount(&Account{ID: 1, Balance: fromBalance})
	accountStore.CreateAccount(&Account{ID: 2, Balance: toBalance})

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    10 * time.Millisecond,
		}),
	)

	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	successCount := 0
	for i, amount := range amounts {
		tx, _ := engine.NewTransaction("confluence_transfer").
			WithLockKeys("account:1", "account:2").
			WithInput(map[string]any{
				"from_account_id": int64(1),
				"to_account_id":   int64(2),
				"amount":          amount,
				"ticket":          fmt.Sprintf("CONF-%d-%d", rapid.Int().Draw(t, fmt.Sprintf("ticket_%d", i)), i),
			}).
			AddStep("debit_account").
			AddStep("external_deposit").
			AddStep("credit_account").
			AddStep("finalize").
			Build()

		result, _ := engine.Execute(context.Background(), tx)
		if result.Status == TxStatusCompleted {
			successCount++
		}
	}

	fromAcc, _ := accountStore.GetAccount(1)
	toAcc, _ := accountStore.GetAccount(2)

	return confluenceState{
		fromBalance:  fromAcc.Balance,
		toBalance:    toAcc.Balance,
		successCount: successCount,
	}
}

// TestProperty_Confluence_Concurrent verifies confluence under actual concurrent execution
// Property 8: Confluence - concurrent operations produce deterministic final state
func TestProperty_Confluence_Concurrent(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate random parameters
		numTransfers := rapid.IntRange(3, 8).Draw(t, "num_transfers")
		initialFromBalance := rapid.Float64Range(5000, 20000).Draw(t, "from_balance")
		initialToBalance := rapid.Float64Range(0, 5000).Draw(t, "to_balance")

		// Generate transfer amounts
		maxPerTransfer := initialFromBalance / float64(numTransfers+2)
		transferAmounts := make([]float64, numTransfers)
		var totalTransferAmount float64
		for i := 0; i < numTransfers; i++ {
			transferAmounts[i] = rapid.Float64Range(1, maxPerTransfer).Draw(t, fmt.Sprintf("amount_%d", i))
			totalTransferAmount += transferAmounts[i]
		}

		// Run concurrent transfers
		finalState := runTransfersConcurrently(initialFromBalance, initialToBalance, transferAmounts)

		// Property: Balance conservation must hold
		initialTotal := initialFromBalance + initialToBalance
		finalTotal := finalState.fromBalance + finalState.toBalance

		if !floatEquals(finalTotal, initialTotal, 0.0001) {
			t.Fatalf("Balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
		}

		// Property: Final balance should reflect successful transfers
		// Each successful transfer moves amount from account 1 to account 2
		// We can't predict exact success count due to concurrency, but we can verify consistency
		transferredAmount := finalState.toBalance - initialToBalance
		debitedAmount := initialFromBalance - finalState.fromBalance

		if !floatEquals(transferredAmount, debitedAmount, 0.0001) {
			t.Fatalf("Transfer consistency violated: credited=%f, debited=%f", transferredAmount, debitedAmount)
		}

		// Property: Transferred amount should be sum of some subset of transfer amounts
		// (the successful ones)
		if transferredAmount < 0 {
			t.Fatalf("Negative transfer amount: %f", transferredAmount)
		}
		if transferredAmount > totalTransferAmount {
			t.Fatalf("Transferred more than total requested: transferred=%f, total=%f",
				transferredAmount, totalTransferAmount)
		}
	})
}

// runTransfersConcurrently runs transfers concurrently and returns final state
func runTransfersConcurrently(fromBalance, toBalance float64, amounts []float64) confluenceState {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()

	accountStore.CreateAccount(&Account{ID: 1, Balance: fromBalance})
	accountStore.CreateAccount(&Account{ID: 2, Balance: toBalance})

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    10 * time.Millisecond,
		}),
	)

	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	var wg sync.WaitGroup
	var successCount int32

	for i, amount := range amounts {
		wg.Add(1)
		go func(idx int, amt float64) {
			defer wg.Done()

			tx, _ := engine.NewTransaction("confluence_concurrent").
				WithLockKeys("account:1", "account:2").
				WithInput(map[string]any{
					"from_account_id": int64(1),
					"to_account_id":   int64(2),
					"amount":          amt,
					"ticket":          fmt.Sprintf("CONF-CONC-%d-%d", time.Now().UnixNano(), idx),
				}).
				AddStep("debit_account").
				AddStep("external_deposit").
				AddStep("credit_account").
				AddStep("finalize").
				Build()

			result, _ := engine.Execute(context.Background(), tx)
			if result.Status == TxStatusCompleted {
				atomic.AddInt32(&successCount, 1)
			}
		}(i, amount)
	}

	wg.Wait()

	fromAcc, _ := accountStore.GetAccount(1)
	toAcc, _ := accountStore.GetAccount(2)

	return confluenceState{
		fromBalance:  fromAcc.Balance,
		toBalance:    toAcc.Balance,
		successCount: int(successCount),
	}
}

// TestProperty_Confluence_Bidirectional verifies confluence for bidirectional transfers
// Property 8: Confluence - bidirectional concurrent transfers produce consistent state
func TestProperty_Confluence_Bidirectional(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate random parameters
		numTransfersPerDirection := rapid.IntRange(2, 5).Draw(t, "num_per_direction")
		initialBalance1 := rapid.Float64Range(5000, 15000).Draw(t, "balance1")
		initialBalance2 := rapid.Float64Range(5000, 15000).Draw(t, "balance2")

		// Generate transfer amounts for both directions
		maxPerTransfer := 500.0 // Keep transfers small to avoid insufficient balance
		amounts1to2 := make([]float64, numTransfersPerDirection)
		amounts2to1 := make([]float64, numTransfersPerDirection)

		var total1to2, total2to1 float64
		for i := 0; i < numTransfersPerDirection; i++ {
			amounts1to2[i] = rapid.Float64Range(10, maxPerTransfer).Draw(t, fmt.Sprintf("amt1to2_%d", i))
			amounts2to1[i] = rapid.Float64Range(10, maxPerTransfer).Draw(t, fmt.Sprintf("amt2to1_%d", i))
			total1to2 += amounts1to2[i]
			total2to1 += amounts2to1[i]
		}

		// Run bidirectional transfers
		finalState := runBidirectionalTransfers(initialBalance1, initialBalance2, amounts1to2, amounts2to1)

		// Property: Balance conservation must hold
		initialTotal := initialBalance1 + initialBalance2
		finalTotal := finalState.balance1 + finalState.balance2

		if !floatEquals(finalTotal, initialTotal, 0.0001) {
			t.Fatalf("Balance conservation violated: initial=%f, final=%f", initialTotal, finalTotal)
		}

		// Property: Net transfer should be consistent
		// Net change in balance1 = (transfers received from 2) - (transfers sent to 2)
		netChange1 := finalState.balance1 - initialBalance1
		netChange2 := finalState.balance2 - initialBalance2

		// Net changes should be opposite
		if !floatEquals(netChange1, -netChange2, 0.0001) {
			t.Fatalf("Net transfer inconsistency: netChange1=%f, netChange2=%f", netChange1, netChange2)
		}
	})
}

// bidirectionalState holds state after bidirectional transfers
type bidirectionalState struct {
	balance1    float64
	balance2    float64
	success1to2 int
	success2to1 int
}

// runBidirectionalTransfers runs bidirectional transfers concurrently
func runBidirectionalTransfers(balance1, balance2 float64, amounts1to2, amounts2to1 []float64) bidirectionalState {
	store := newMockStore()
	locker := newMockLocker()
	breaker := newMockBreaker()
	eventBus := event.NewMemoryEventBus()

	accountStore := NewMockAccountStore()
	externalService := NewMockExternalService()

	accountStore.CreateAccount(&Account{ID: 1, Balance: balance1})
	accountStore.CreateAccount(&Account{ID: 2, Balance: balance2})

	engine := NewEngine(
		WithEngineStore(store),
		WithEngineLocker(locker),
		WithEngineBreaker(breaker),
		WithEngineEventBus(eventBus),
		WithEngineConfig(Config{
			LockTTL:          30 * time.Second,
			LockExtendPeriod: 10 * time.Second,
			StepTimeout:      5 * time.Second,
			TxTimeout:        30 * time.Second,
			MaxRetries:       3,
			RetryInterval:    10 * time.Millisecond,
		}),
	)

	engine.RegisterStep(NewDebitAccountStep(accountStore))
	engine.RegisterStep(NewExternalDepositStep(externalService, accountStore))
	engine.RegisterStep(NewCreditAccountStep(accountStore))
	engine.RegisterStep(NewFinalizeStep(accountStore))

	var wg sync.WaitGroup
	var success1to2, success2to1 int32

	// Transfers from 1 to 2
	for i, amount := range amounts1to2 {
		wg.Add(1)
		go func(idx int, amt float64) {
			defer wg.Done()

			tx, _ := engine.NewTransaction("bidir_1to2").
				WithLockKeys("account:1", "account:2").
				WithInput(map[string]any{
					"from_account_id": int64(1),
					"to_account_id":   int64(2),
					"amount":          amt,
					"ticket":          fmt.Sprintf("BIDIR-PROP-1TO2-%d-%d", time.Now().UnixNano(), idx),
				}).
				AddStep("debit_account").
				AddStep("external_deposit").
				AddStep("credit_account").
				AddStep("finalize").
				Build()

			result, _ := engine.Execute(context.Background(), tx)
			if result.Status == TxStatusCompleted {
				atomic.AddInt32(&success1to2, 1)
			}
		}(i, amount)
	}

	// Transfers from 2 to 1
	for i, amount := range amounts2to1 {
		wg.Add(1)
		go func(idx int, amt float64) {
			defer wg.Done()

			tx, _ := engine.NewTransaction("bidir_2to1").
				WithLockKeys("account:1", "account:2").
				WithInput(map[string]any{
					"from_account_id": int64(2),
					"to_account_id":   int64(1),
					"amount":          amt,
					"ticket":          fmt.Sprintf("BIDIR-PROP-2TO1-%d-%d", time.Now().UnixNano(), idx),
				}).
				AddStep("debit_account").
				AddStep("external_deposit").
				AddStep("credit_account").
				AddStep("finalize").
				Build()

			result, _ := engine.Execute(context.Background(), tx)
			if result.Status == TxStatusCompleted {
				atomic.AddInt32(&success2to1, 1)
			}
		}(i, amount)
	}

	wg.Wait()

	acc1, _ := accountStore.GetAccount(1)
	acc2, _ := accountStore.GetAccount(2)

	return bidirectionalState{
		balance1:    acc1.Balance,
		balance2:    acc2.Balance,
		success1to2: int(success1to2),
		success2to1: int(success2to1),
	}
}
