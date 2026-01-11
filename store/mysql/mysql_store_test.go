// Package mysql provides tests for the MySQL implementation of the store.Store interface.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"pgregory.net/rapid"

	"rte"
	"rte/store"
)

// ============================================================================
// Test Helpers
// ============================================================================

func newTestStore(t *testing.T) (*MySQLStore, sqlmock.Sqlmock, func()) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	s := New(db)
	return s, mock, func() { db.Close() }
}

func createTestTransaction(txID, txType string) *store.Transaction {
	return store.NewTransaction(txID, txType, []string{"step1", "step2"})
}

func createTestStep(txID string, stepIndex int, stepName string) *store.StepRecord {
	return store.NewStepRecord(txID, stepIndex, stepName)
}

// ============================================================================
// Transaction CRUD Tests
// ============================================================================

func TestMySQLStore_CreateTransaction(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	tx := createTestTransaction("tx-123", "test_type")
	tx.LockKeys = []string{"key1", "key2"}

	mock.ExpectExec("INSERT INTO rte_transactions").
		WithArgs(
			tx.TxID, tx.TxType, tx.Status, tx.CurrentStep, tx.TotalSteps,
			sqlmock.AnyArg(), // step_names JSON
			sqlmock.AnyArg(), // lock_keys JSON
			sqlmock.AnyArg(), // context JSON
			tx.ErrorMsg, tx.RetryCount, tx.MaxRetries, tx.Version,
			sqlmock.AnyArg(), sqlmock.AnyArg(), // created_at, updated_at
			tx.LockedAt, tx.CompletedAt, tx.TimeoutAt,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := s.CreateTransaction(context.Background(), tx)
	if err != nil {
		t.Errorf("CreateTransaction failed: %v", err)
	}

	if tx.ID != 1 {
		t.Errorf("expected ID 1, got %d", tx.ID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestMySQLStore_CreateTransaction_DuplicateKey(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	tx := createTestTransaction("tx-123", "test_type")

	mock.ExpectExec("INSERT INTO rte_transactions").
		WillReturnError(errors.New("Duplicate entry 'tx-123' for key 'tx_id'"))

	err := s.CreateTransaction(context.Background(), tx)
	if !errors.Is(err, rte.ErrTransactionAlreadyExists) {
		t.Errorf("expected ErrTransactionAlreadyExists, got %v", err)
	}
}

func TestMySQLStore_GetTransaction(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tx_id", "tx_type", "status", "current_step", "total_steps", "step_names",
		"lock_keys", "context", "error_msg", "retry_count", "max_retries", "version",
		"created_at", "updated_at", "locked_at", "completed_at", "timeout_at",
	}).AddRow(
		1, "tx-123", "test_type", rte.TxStatusCreated, 0, 2, `["step1","step2"]`,
		`["key1"]`, `{"tx_id":"tx-123","tx_type":"test_type"}`, "", 0, 3, 0,
		now, now, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT .+ FROM rte_transactions WHERE tx_id = ?").
		WithArgs("tx-123").
		WillReturnRows(rows)

	tx, err := s.GetTransaction(context.Background(), "tx-123")
	if err != nil {
		t.Errorf("GetTransaction failed: %v", err)
	}

	if tx.TxID != "tx-123" {
		t.Errorf("expected TxID 'tx-123', got '%s'", tx.TxID)
	}
	if tx.TxType != "test_type" {
		t.Errorf("expected TxType 'test_type', got '%s'", tx.TxType)
	}
	if tx.Status != rte.TxStatusCreated {
		t.Errorf("expected status CREATED, got %s", tx.Status)
	}
}

func TestMySQLStore_GetTransaction_NotFound(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectQuery("SELECT .+ FROM rte_transactions WHERE tx_id = ?").
		WithArgs("tx-not-found").
		WillReturnError(sql.ErrNoRows)

	_, err := s.GetTransaction(context.Background(), "tx-not-found")
	if !errors.Is(err, rte.ErrTransactionNotFound) {
		t.Errorf("expected ErrTransactionNotFound, got %v", err)
	}
}

func TestMySQLStore_UpdateTransaction(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	tx := createTestTransaction("tx-123", "test_type")
	tx.Status = rte.TxStatusExecuting
	tx.Version = 1 // Caller is expected to have already incremented the version

	mock.ExpectExec("UPDATE rte_transactions SET").
		WithArgs(
			tx.Status, tx.CurrentStep, sqlmock.AnyArg(), tx.ErrorMsg,
			tx.RetryCount, tx.Version, sqlmock.AnyArg(), // version and updated_at
			tx.LockedAt, tx.CompletedAt, tx.TimeoutAt,
			tx.TxID, tx.Version-1, // WHERE clause uses version-1
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := s.UpdateTransaction(context.Background(), tx)
	if err != nil {
		t.Errorf("UpdateTransaction failed: %v", err)
	}

	// Version should remain the same (caller already incremented it)
	if tx.Version != 1 {
		t.Errorf("expected version to remain 1, got %d", tx.Version)
	}
}

func TestMySQLStore_UpdateTransaction_NotFound(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	tx := createTestTransaction("tx-not-found", "test_type")
	tx.Version = 0

	mock.ExpectExec("UPDATE rte_transactions SET").
		WillReturnResult(sqlmock.NewResult(0, 0))

	// Check if transaction exists
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM rte_transactions WHERE tx_id = ?").
		WithArgs("tx-not-found").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	err := s.UpdateTransaction(context.Background(), tx)
	if !errors.Is(err, rte.ErrTransactionNotFound) {
		t.Errorf("expected ErrTransactionNotFound, got %v", err)
	}
}

// ============================================================================
// Step CRUD Tests
// ============================================================================

func TestMySQLStore_CreateStep(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	step := createTestStep("tx-123", 0, "step1")

	mock.ExpectExec("INSERT INTO rte_steps").
		WithArgs(
			step.TxID, step.StepIndex, step.StepName, step.Status, step.IdempotencyKey,
			step.Input, step.Output, step.ErrorMsg, step.RetryCount,
			step.StartedAt, step.CompletedAt, sqlmock.AnyArg(), sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := s.CreateStep(context.Background(), step)
	if err != nil {
		t.Errorf("CreateStep failed: %v", err)
	}

	if step.ID != 1 {
		t.Errorf("expected ID 1, got %d", step.ID)
	}
}

func TestMySQLStore_GetStep(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tx_id", "step_index", "step_name", "status", "idempotency_key",
		"input", "output", "error_msg", "retry_count",
		"started_at", "completed_at", "created_at", "updated_at",
	}).AddRow(
		1, "tx-123", 0, "step1", rte.StepStatusPending, "",
		nil, nil, "", 0,
		nil, nil, now, now,
	)

	mock.ExpectQuery("SELECT .+ FROM rte_steps WHERE tx_id = \\? AND step_index = \\?").
		WithArgs("tx-123", 0).
		WillReturnRows(rows)

	step, err := s.GetStep(context.Background(), "tx-123", 0)
	if err != nil {
		t.Errorf("GetStep failed: %v", err)
	}

	if step.StepName != "step1" {
		t.Errorf("expected StepName 'step1', got '%s'", step.StepName)
	}
}

func TestMySQLStore_GetStep_NotFound(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectQuery("SELECT .+ FROM rte_steps WHERE tx_id = \\? AND step_index = \\?").
		WithArgs("tx-123", 99).
		WillReturnError(sql.ErrNoRows)

	_, err := s.GetStep(context.Background(), "tx-123", 99)
	if !errors.Is(err, rte.ErrStepNotFound) {
		t.Errorf("expected ErrStepNotFound, got %v", err)
	}
}

func TestMySQLStore_UpdateStep(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	step := createTestStep("tx-123", 0, "step1")
	step.Status = rte.StepStatusExecuting

	mock.ExpectExec("UPDATE rte_steps SET").
		WithArgs(
			step.Status, step.IdempotencyKey, step.Input, step.Output,
			step.ErrorMsg, step.RetryCount, step.StartedAt, step.CompletedAt, sqlmock.AnyArg(),
			step.TxID, step.StepIndex,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err := s.UpdateStep(context.Background(), step)
	if err != nil {
		t.Errorf("UpdateStep failed: %v", err)
	}
}

func TestMySQLStore_UpdateStep_NotFound(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	step := createTestStep("tx-123", 99, "step_not_found")

	mock.ExpectExec("UPDATE rte_steps SET").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := s.UpdateStep(context.Background(), step)
	if !errors.Is(err, rte.ErrStepNotFound) {
		t.Errorf("expected ErrStepNotFound, got %v", err)
	}
}

func TestMySQLStore_GetSteps(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tx_id", "step_index", "step_name", "status", "idempotency_key",
		"input", "output", "error_msg", "retry_count",
		"started_at", "completed_at", "created_at", "updated_at",
	}).
		AddRow(1, "tx-123", 0, "step1", rte.StepStatusCompleted, "", nil, nil, "", 0, nil, nil, now, now).
		AddRow(2, "tx-123", 1, "step2", rte.StepStatusPending, "", nil, nil, "", 0, nil, nil, now, now)

	mock.ExpectQuery("SELECT .+ FROM rte_steps WHERE tx_id = \\? ORDER BY step_index ASC").
		WithArgs("tx-123").
		WillReturnRows(rows)

	steps, err := s.GetSteps(context.Background(), "tx-123")
	if err != nil {
		t.Errorf("GetSteps failed: %v", err)
	}

	if len(steps) != 2 {
		t.Errorf("expected 2 steps, got %d", len(steps))
	}
}

// ============================================================================
// Idempotency Tests
// ============================================================================

func TestMySQLStore_CheckIdempotency_NotExists(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectQuery("SELECT result FROM rte_idempotency").
		WithArgs("key-123", sqlmock.AnyArg()).
		WillReturnError(sql.ErrNoRows)

	exists, result, err := s.CheckIdempotency(context.Background(), "key-123")
	if err != nil {
		t.Errorf("CheckIdempotency failed: %v", err)
	}
	if exists {
		t.Error("expected exists to be false")
	}
	if result != nil {
		t.Error("expected result to be nil")
	}
}

func TestMySQLStore_CheckIdempotency_Exists(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	expectedResult := []byte(`{"status":"success"}`)
	rows := sqlmock.NewRows([]string{"result"}).AddRow(expectedResult)

	mock.ExpectQuery("SELECT result FROM rte_idempotency").
		WithArgs("key-123", sqlmock.AnyArg()).
		WillReturnRows(rows)

	exists, result, err := s.CheckIdempotency(context.Background(), "key-123")
	if err != nil {
		t.Errorf("CheckIdempotency failed: %v", err)
	}
	if !exists {
		t.Error("expected exists to be true")
	}
	if string(result) != string(expectedResult) {
		t.Errorf("expected result %s, got %s", expectedResult, result)
	}
}

func TestMySQLStore_MarkIdempotency(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	result := []byte(`{"status":"success"}`)

	mock.ExpectExec("INSERT INTO rte_idempotency").
		WithArgs("key-123", result, sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := s.MarkIdempotency(context.Background(), "key-123", result, 24*time.Hour)
	if err != nil {
		t.Errorf("MarkIdempotency failed: %v", err)
	}
}

// ============================================================================
// Recovery Query Tests
// ============================================================================

func TestMySQLStore_GetStuckTransactions(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tx_id", "tx_type", "status", "current_step", "total_steps", "step_names",
		"lock_keys", "context", "error_msg", "retry_count", "max_retries", "version",
		"created_at", "updated_at", "locked_at", "completed_at", "timeout_at",
	}).AddRow(
		1, "tx-stuck", "test_type", rte.TxStatusExecuting, 1, 2, `["step1","step2"]`,
		`[]`, `{"tx_id":"tx-stuck"}`, "", 0, 3, 0,
		now.Add(-10*time.Minute), now.Add(-10*time.Minute), nil, nil, nil,
	)

	mock.ExpectQuery("SELECT .+ FROM rte_transactions WHERE status IN").
		WithArgs(rte.TxStatusLocked, rte.TxStatusExecuting, sqlmock.AnyArg()).
		WillReturnRows(rows)

	txs, err := s.GetStuckTransactions(context.Background(), 5*time.Minute)
	if err != nil {
		t.Errorf("GetStuckTransactions failed: %v", err)
	}

	if len(txs) != 1 {
		t.Errorf("expected 1 stuck transaction, got %d", len(txs))
	}
}

func TestMySQLStore_GetRetryableTransactions(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tx_id", "tx_type", "status", "current_step", "total_steps", "step_names",
		"lock_keys", "context", "error_msg", "retry_count", "max_retries", "version",
		"created_at", "updated_at", "locked_at", "completed_at", "timeout_at",
	}).AddRow(
		1, "tx-failed", "test_type", rte.TxStatusFailed, 1, 2, `["step1","step2"]`,
		`[]`, `{"tx_id":"tx-failed"}`, "some error", 1, 3, 0,
		now, now, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT .+ FROM rte_transactions WHERE status = \\? AND retry_count < \\?").
		WithArgs(rte.TxStatusFailed, 3).
		WillReturnRows(rows)

	txs, err := s.GetRetryableTransactions(context.Background(), 3)
	if err != nil {
		t.Errorf("GetRetryableTransactions failed: %v", err)
	}

	if len(txs) != 1 {
		t.Errorf("expected 1 retryable transaction, got %d", len(txs))
	}
}

// ============================================================================
// Optimistic Lock Tests 
// ============================================================================

func TestMySQLStore_UpdateTransaction_VersionConflict(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	tx := createTestTransaction("tx-123", "test_type")
	tx.Version = 6 // Caller has already incremented version to 6

	// Simulate version conflict - no rows affected because version doesn't match
	// The WHERE clause uses version-1 (5), but the actual version in DB is different
	mock.ExpectExec("UPDATE rte_transactions SET").
		WithArgs(
			tx.Status, tx.CurrentStep, sqlmock.AnyArg(), tx.ErrorMsg,
			tx.RetryCount, tx.Version, sqlmock.AnyArg(), // version and updated_at
			tx.LockedAt, tx.CompletedAt, tx.TimeoutAt,
			tx.TxID, tx.Version-1, // WHERE clause uses version-1
		).
		WillReturnResult(sqlmock.NewResult(0, 0))

	// Transaction exists but version doesn't match
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM rte_transactions WHERE tx_id = ?").
		WithArgs("tx-123").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	err := s.UpdateTransaction(context.Background(), tx)
	if !errors.Is(err, rte.ErrVersionConflict) {
		t.Errorf("expected ErrVersionConflict, got %v", err)
	}
}

// ============================================================================
// Property-Based Tests
// ============================================================================


// For any transaction, if two concurrent updates attempt to modify the same
// transaction with the same version, only one should succeed and the other
// should receive a version conflict error.
// Note: The caller is expected to have already incremented the version before calling UpdateTransaction.
func TestProperty_OptimisticLockPreventsLostUpdates(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate random transaction data
		txID := rapid.StringMatching(`tx-[a-z0-9]{8}`).Draw(t, "txID")
		txType := rapid.SampledFrom([]string{"transfer", "deposit", "withdrawal"}).Draw(t, "txType")
		initialVersion := rapid.IntRange(0, 100).Draw(t, "initialVersion")

		// Create mock store
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create sqlmock: %v", err)
		}
		defer db.Close()
		s := New(db)

		// Create two copies of the same transaction (simulating two concurrent readers)
		// Both callers have read version=initialVersion and incremented to initialVersion+1
		tx1 := createTestTransaction(txID, txType)
		tx1.Version = initialVersion + 1 // Caller has already incremented
		tx1.Status = rte.TxStatusExecuting

		tx2 := createTestTransaction(txID, txType)
		tx2.Version = initialVersion + 1 // Caller has already incremented (same as tx1)
		tx2.Status = rte.TxStatusFailed

		// First update succeeds - WHERE clause uses version-1 (initialVersion)
		mock.ExpectExec("UPDATE rte_transactions SET").
			WithArgs(
				tx1.Status, tx1.CurrentStep, sqlmock.AnyArg(), tx1.ErrorMsg,
				tx1.RetryCount, tx1.Version, sqlmock.AnyArg(), // version and updated_at
				tx1.LockedAt, tx1.CompletedAt, tx1.TimeoutAt,
				tx1.TxID, initialVersion, // WHERE clause uses version-1
			).
			WillReturnResult(sqlmock.NewResult(0, 1))

		// Second update fails (version already incremented by first update)
		// WHERE clause uses version-1 (initialVersion), but DB now has initialVersion+1
		mock.ExpectExec("UPDATE rte_transactions SET").
			WithArgs(
				tx2.Status, tx2.CurrentStep, sqlmock.AnyArg(), tx2.ErrorMsg,
				tx2.RetryCount, tx2.Version, sqlmock.AnyArg(), // version and updated_at
				tx2.LockedAt, tx2.CompletedAt, tx2.TimeoutAt,
				tx2.TxID, initialVersion, // WHERE clause uses version-1
			).
			WillReturnResult(sqlmock.NewResult(0, 0))

		// Transaction exists check for second update
		mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM rte_transactions WHERE tx_id = ?").
			WithArgs(txID).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		// Execute first update - should succeed
		err1 := s.UpdateTransaction(context.Background(), tx1)
		if err1 != nil {
			t.Fatalf("first update should succeed, got error: %v", err1)
		}

		// Version should remain the same (caller already incremented it)
		if tx1.Version != initialVersion+1 {
			t.Fatalf("expected version %d after first update, got %d", initialVersion+1, tx1.Version)
		}

		// Execute second update - should fail with version conflict
		err2 := s.UpdateTransaction(context.Background(), tx2)
		if !errors.Is(err2, rte.ErrVersionConflict) {
			t.Fatalf("second update should fail with ErrVersionConflict, got: %v", err2)
		}

		// Verify second transaction's version was NOT changed
		if tx2.Version != initialVersion+1 {
			t.Fatalf("expected version %d unchanged after failed update, got %d", initialVersion+1, tx2.Version)
		}

		// Verify all expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unfulfilled expectations: %v", err)
		}
	})
}

// Additional property test: Version remains unchanged after successful update
// (caller is expected to have already incremented the version)
func TestProperty_VersionIncrementsOnSuccess(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		txID := rapid.StringMatching(`tx-[a-z0-9]{8}`).Draw(t, "txID")
		txType := rapid.SampledFrom([]string{"transfer", "deposit", "withdrawal"}).Draw(t, "txType")
		initialVersion := rapid.IntRange(1, 1000).Draw(t, "initialVersion") // Start from 1 since caller increments

		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("failed to create sqlmock: %v", err)
		}
		defer db.Close()
		s := New(db)

		tx := createTestTransaction(txID, txType)
		tx.Version = initialVersion // Caller has already incremented

		mock.ExpectExec("UPDATE rte_transactions SET").
			WithArgs(
				tx.Status, tx.CurrentStep, sqlmock.AnyArg(), tx.ErrorMsg,
				tx.RetryCount, tx.Version, sqlmock.AnyArg(), // version and updated_at
				tx.LockedAt, tx.CompletedAt, tx.TimeoutAt,
				tx.TxID, tx.Version-1, // WHERE clause uses version-1
			).
			WillReturnResult(sqlmock.NewResult(0, 1))

		err = s.UpdateTransaction(context.Background(), tx)
		if err != nil {
			t.Fatalf("update should succeed, got error: %v", err)
		}

		
		if tx.Version != initialVersion {
			t.Fatalf("expected version %d, got %d", initialVersion, tx.Version)
		}
	})
}

// ============================================================================
// ListTransactions Tests
// ============================================================================

func TestMySQLStore_ListTransactions(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	now := time.Now()
	filter := store.NewTxFilter().
		WithStatus(rte.TxStatusFailed).
		WithPagination(10, 0)

	// Count query
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM rte_transactions WHERE status IN").
		WithArgs(rte.TxStatusFailed).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	// List query
	rows := sqlmock.NewRows([]string{
		"id", "tx_id", "tx_type", "status", "current_step", "total_steps", "step_names",
		"lock_keys", "context", "error_msg", "retry_count", "max_retries", "version",
		"created_at", "updated_at", "locked_at", "completed_at", "timeout_at",
	}).AddRow(
		1, "tx-123", "test_type", rte.TxStatusFailed, 1, 2, `["step1","step2"]`,
		`[]`, `{"tx_id":"tx-123"}`, "error", 0, 3, 0,
		now, now, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT .+ FROM rte_transactions .+ LIMIT \\? OFFSET \\?").
		WithArgs(rte.TxStatusFailed, 10, 0).
		WillReturnRows(rows)

	txs, total, err := s.ListTransactions(context.Background(), filter)
	if err != nil {
		t.Errorf("ListTransactions failed: %v", err)
	}

	if total != 1 {
		t.Errorf("expected total 1, got %d", total)
	}

	if len(txs) != 1 {
		t.Errorf("expected 1 transaction, got %d", len(txs))
	}
}

func TestMySQLStore_DeleteExpiredIdempotency(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectExec("DELETE FROM rte_idempotency WHERE expires_at < ?").
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 5))

	count, err := s.DeleteExpiredIdempotency(context.Background())
	if err != nil {
		t.Errorf("DeleteExpiredIdempotency failed: %v", err)
	}

	if count != 5 {
		t.Errorf("expected 5 deleted, got %d", count)
	}
}

// ============================================================================
// GetPendingTransactions Tests 
// ============================================================================

func TestMySQLStore_GetPendingTransactions(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tx_id", "tx_type", "status", "current_step", "total_steps", "step_names",
		"lock_keys", "context", "error_msg", "retry_count", "max_retries", "version",
		"created_at", "updated_at", "locked_at", "completed_at", "timeout_at",
	}).AddRow(
		1, "tx-pending", "test_type", rte.TxStatusCreated, 0, 2, `["step1","step2"]`,
		`[]`, `{"tx_id":"tx-pending"}`, "", 0, 3, 0,
		now.Add(-10*time.Minute), now.Add(-10*time.Minute), nil, nil, nil,
	)

	mock.ExpectQuery("SELECT .+ FROM rte_transactions WHERE status = \\? AND updated_at < \\?").
		WithArgs(rte.TxStatusCreated, sqlmock.AnyArg()).
		WillReturnRows(rows)

	txs, err := s.GetPendingTransactions(context.Background(), 5*time.Minute)
	if err != nil {
		t.Errorf("GetPendingTransactions failed: %v", err)
	}

	if len(txs) != 1 {
		t.Errorf("expected 1 pending transaction, got %d", len(txs))
	}

	if txs[0].TxID != "tx-pending" {
		t.Errorf("expected TxID 'tx-pending', got '%s'", txs[0].TxID)
	}

	if txs[0].Status != rte.TxStatusCreated {
		t.Errorf("expected status CREATED, got %s", txs[0].Status)
	}
}

func TestMySQLStore_GetPendingTransactions_Empty(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	rows := sqlmock.NewRows([]string{
		"id", "tx_id", "tx_type", "status", "current_step", "total_steps", "step_names",
		"lock_keys", "context", "error_msg", "retry_count", "max_retries", "version",
		"created_at", "updated_at", "locked_at", "completed_at", "timeout_at",
	})

	mock.ExpectQuery("SELECT .+ FROM rte_transactions WHERE status = \\? AND updated_at < \\?").
		WithArgs(rte.TxStatusCreated, sqlmock.AnyArg()).
		WillReturnRows(rows)

	txs, err := s.GetPendingTransactions(context.Background(), 5*time.Minute)
	if err != nil {
		t.Errorf("GetPendingTransactions failed: %v", err)
	}

	if len(txs) != 0 {
		t.Errorf("expected 0 pending transactions, got %d", len(txs))
	}
}

func TestMySQLStore_GetPendingTransactions_QueryError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectQuery("SELECT .+ FROM rte_transactions WHERE status = \\? AND updated_at < \\?").
		WithArgs(rte.TxStatusCreated, sqlmock.AnyArg()).
		WillReturnError(errors.New("database connection error"))

	_, err := s.GetPendingTransactions(context.Background(), 5*time.Minute)
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

// ============================================================================
// CreateTransaction Error Path Tests 
// ============================================================================

func TestMySQLStore_CreateTransaction_ExecError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	tx := createTestTransaction("tx-123", "test_type")

	mock.ExpectExec("INSERT INTO rte_transactions").
		WillReturnError(errors.New("database connection error"))

	err := s.CreateTransaction(context.Background(), tx)
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

func TestMySQLStore_CreateTransaction_LastInsertIdError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	tx := createTestTransaction("tx-123", "test_type")

	mock.ExpectExec("INSERT INTO rte_transactions").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("last insert id error")))

	err := s.CreateTransaction(context.Background(), tx)
	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ============================================================================
// UpdateTransaction Error Path Tests 
// ============================================================================

func TestMySQLStore_UpdateTransaction_ExecError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	tx := createTestTransaction("tx-123", "test_type")
	tx.Version = 1

	mock.ExpectExec("UPDATE rte_transactions SET").
		WillReturnError(errors.New("database connection error"))

	err := s.UpdateTransaction(context.Background(), tx)
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

func TestMySQLStore_UpdateTransaction_RowsAffectedError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	tx := createTestTransaction("tx-123", "test_type")
	tx.Version = 1

	mock.ExpectExec("UPDATE rte_transactions SET").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	err := s.UpdateTransaction(context.Background(), tx)
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestMySQLStore_UpdateTransaction_TransactionExistsCheckError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	tx := createTestTransaction("tx-123", "test_type")
	tx.Version = 1

	mock.ExpectExec("UPDATE rte_transactions SET").
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM rte_transactions WHERE tx_id = ?").
		WithArgs("tx-123").
		WillReturnError(errors.New("database error"))

	err := s.UpdateTransaction(context.Background(), tx)
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

// ============================================================================
// GetTransaction Error Path Tests 
// ============================================================================

func TestMySQLStore_GetTransaction_QueryError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectQuery("SELECT .+ FROM rte_transactions WHERE tx_id = ?").
		WithArgs("tx-123").
		WillReturnError(errors.New("database connection error"))

	_, err := s.GetTransaction(context.Background(), "tx-123")
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

func TestMySQLStore_GetTransaction_UnmarshalStepNamesError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tx_id", "tx_type", "status", "current_step", "total_steps", "step_names",
		"lock_keys", "context", "error_msg", "retry_count", "max_retries", "version",
		"created_at", "updated_at", "locked_at", "completed_at", "timeout_at",
	}).AddRow(
		1, "tx-123", "test_type", rte.TxStatusCreated, 0, 2, `invalid json`,
		`["key1"]`, `{"tx_id":"tx-123"}`, "", 0, 3, 0,
		now, now, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT .+ FROM rte_transactions WHERE tx_id = ?").
		WithArgs("tx-123").
		WillReturnRows(rows)

	_, err := s.GetTransaction(context.Background(), "tx-123")
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestMySQLStore_GetTransaction_UnmarshalLockKeysError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tx_id", "tx_type", "status", "current_step", "total_steps", "step_names",
		"lock_keys", "context", "error_msg", "retry_count", "max_retries", "version",
		"created_at", "updated_at", "locked_at", "completed_at", "timeout_at",
	}).AddRow(
		1, "tx-123", "test_type", rte.TxStatusCreated, 0, 2, `["step1","step2"]`,
		`invalid json`, `{"tx_id":"tx-123"}`, "", 0, 3, 0,
		now, now, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT .+ FROM rte_transactions WHERE tx_id = ?").
		WithArgs("tx-123").
		WillReturnRows(rows)

	_, err := s.GetTransaction(context.Background(), "tx-123")
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestMySQLStore_GetTransaction_UnmarshalContextError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	now := time.Now()
	rows := sqlmock.NewRows([]string{
		"id", "tx_id", "tx_type", "status", "current_step", "total_steps", "step_names",
		"lock_keys", "context", "error_msg", "retry_count", "max_retries", "version",
		"created_at", "updated_at", "locked_at", "completed_at", "timeout_at",
	}).AddRow(
		1, "tx-123", "test_type", rte.TxStatusCreated, 0, 2, `["step1","step2"]`,
		`["key1"]`, `invalid json`, "", 0, 3, 0,
		now, now, nil, nil, nil,
	)

	mock.ExpectQuery("SELECT .+ FROM rte_transactions WHERE tx_id = ?").
		WithArgs("tx-123").
		WillReturnRows(rows)

	_, err := s.GetTransaction(context.Background(), "tx-123")
	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ============================================================================
// CreateStep Error Path Tests 
// ============================================================================

func TestMySQLStore_CreateStep_DuplicateKey(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	step := createTestStep("tx-123", 0, "step1")

	mock.ExpectExec("INSERT INTO rte_steps").
		WillReturnError(errors.New("Duplicate entry 'tx-123-0' for key 'tx_id_step_index'"))

	err := s.CreateStep(context.Background(), step)
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

func TestMySQLStore_CreateStep_ExecError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	step := createTestStep("tx-123", 0, "step1")

	mock.ExpectExec("INSERT INTO rte_steps").
		WillReturnError(errors.New("database connection error"))

	err := s.CreateStep(context.Background(), step)
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

func TestMySQLStore_CreateStep_LastInsertIdError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	step := createTestStep("tx-123", 0, "step1")

	mock.ExpectExec("INSERT INTO rte_steps").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("last insert id error")))

	err := s.CreateStep(context.Background(), step)
	if err == nil {
		t.Error("expected error, got nil")
	}
}

// ============================================================================
// isDuplicateKeyError Tests 
// ============================================================================

func TestIsDuplicateKeyError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "duplicate entry error",
			err:      errors.New("Duplicate entry 'tx-123' for key 'tx_id'"),
			expected: true,
		},
		{
			name:     "error code 1062",
			err:      errors.New("Error 1062: Duplicate entry"),
			expected: true,
		},
		{
			name:     "other error",
			err:      errors.New("connection refused"),
			expected: false,
		},
		{
			name:     "empty error message",
			err:      errors.New(""),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDuplicateKeyError(tt.err)
			if result != tt.expected {
				t.Errorf("isDuplicateKeyError(%v) = %v, expected %v", tt.err, result, tt.expected)
			}
		})
	}
}

// ============================================================================
// Additional Error Path Tests
// ============================================================================

func TestMySQLStore_GetSteps_QueryError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectQuery("SELECT .+ FROM rte_steps WHERE tx_id = \\? ORDER BY step_index ASC").
		WithArgs("tx-123").
		WillReturnError(errors.New("database connection error"))

	_, err := s.GetSteps(context.Background(), "tx-123")
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

func TestMySQLStore_GetSteps_ScanError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	// Return rows with wrong number of columns to trigger scan error
	rows := sqlmock.NewRows([]string{"id", "tx_id"}).
		AddRow(1, "tx-123")

	mock.ExpectQuery("SELECT .+ FROM rte_steps WHERE tx_id = \\? ORDER BY step_index ASC").
		WithArgs("tx-123").
		WillReturnRows(rows)

	_, err := s.GetSteps(context.Background(), "tx-123")
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestMySQLStore_UpdateStep_ExecError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	step := createTestStep("tx-123", 0, "step1")

	mock.ExpectExec("UPDATE rte_steps SET").
		WillReturnError(errors.New("database connection error"))

	err := s.UpdateStep(context.Background(), step)
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

func TestMySQLStore_UpdateStep_RowsAffectedError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	step := createTestStep("tx-123", 0, "step1")

	mock.ExpectExec("UPDATE rte_steps SET").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	err := s.UpdateStep(context.Background(), step)
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestMySQLStore_GetStep_QueryError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectQuery("SELECT .+ FROM rte_steps WHERE tx_id = \\? AND step_index = \\?").
		WithArgs("tx-123", 0).
		WillReturnError(errors.New("database connection error"))

	_, err := s.GetStep(context.Background(), "tx-123", 0)
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

func TestMySQLStore_CheckIdempotency_QueryError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectQuery("SELECT result FROM rte_idempotency").
		WithArgs("key-123", sqlmock.AnyArg()).
		WillReturnError(errors.New("database connection error"))

	_, _, err := s.CheckIdempotency(context.Background(), "key-123")
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrIdempotencyCheckFailed) {
		t.Errorf("expected ErrIdempotencyCheckFailed, got %v", err)
	}
}

func TestMySQLStore_MarkIdempotency_ExecError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectExec("INSERT INTO rte_idempotency").
		WillReturnError(errors.New("database connection error"))

	err := s.MarkIdempotency(context.Background(), "key-123", []byte(`{}`), time.Hour)
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

func TestMySQLStore_DeleteExpiredIdempotency_ExecError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectExec("DELETE FROM rte_idempotency WHERE expires_at < ?").
		WillReturnError(errors.New("database connection error"))

	_, err := s.DeleteExpiredIdempotency(context.Background())
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

func TestMySQLStore_DeleteExpiredIdempotency_RowsAffectedError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectExec("DELETE FROM rte_idempotency WHERE expires_at < ?").
		WillReturnResult(sqlmock.NewErrorResult(errors.New("rows affected error")))

	_, err := s.DeleteExpiredIdempotency(context.Background())
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestMySQLStore_ListTransactions_CountError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	filter := store.NewTxFilter().WithPagination(10, 0)

	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM rte_transactions").
		WillReturnError(errors.New("database connection error"))

	_, _, err := s.ListTransactions(context.Background(), filter)
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

func TestMySQLStore_GetStuckTransactions_QueryError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectQuery("SELECT .+ FROM rte_transactions WHERE status IN").
		WithArgs(rte.TxStatusLocked, rte.TxStatusExecuting, sqlmock.AnyArg()).
		WillReturnError(errors.New("database connection error"))

	_, err := s.GetStuckTransactions(context.Background(), 5*time.Minute)
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}

func TestMySQLStore_GetRetryableTransactions_QueryError(t *testing.T) {
	s, mock, cleanup := newTestStore(t)
	defer cleanup()

	mock.ExpectQuery("SELECT .+ FROM rte_transactions WHERE status = \\? AND retry_count < \\?").
		WithArgs(rte.TxStatusFailed, 3).
		WillReturnError(errors.New("database connection error"))

	_, err := s.GetRetryableTransactions(context.Background(), 3)
	if err == nil {
		t.Error("expected error, got nil")
	}

	if !errors.Is(err, rte.ErrStoreOperationFailed) {
		t.Errorf("expected ErrStoreOperationFailed, got %v", err)
	}
}
