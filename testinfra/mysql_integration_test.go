// Package testinfra provides integration tests for MySQL store with real database.
// These tests validate transaction CRUD, optimistic locking, and recovery queries.
package testinfra

import (
	"context"
	"sync"
	"testing"
	"time"

	"rte"
)

// ============================================================================
// MySQL Store Integration Tests
// ============================================================================

// TestIntegration_MySQLStore_TransactionCRUD tests basic transaction CRUD operations
// using a real MySQL database.
func TestIntegration_MySQLStore_TransactionCRUD(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Create_Transaction", func(t *testing.T) {
		txID := ti.GenerateTxID("crud-create")
		tx := rte.NewStoreTx(txID, "test_transfer", []string{"debit", "credit"})
		tx.LockKeys = []string{"account:1", "account:2"}
		tx.MaxRetries = 3

		err := ti.StoreAdapter.CreateTransaction(ctx, tx)
		if err != nil {
			t.Fatalf("CreateTransaction failed: %v", err)
		}

		if tx.ID == 0 {
			t.Error("Expected non-zero ID after creation")
		}

		// Verify by reading back
		retrieved, err := ti.StoreAdapter.GetTransaction(ctx, txID)
		if err != nil {
			t.Fatalf("GetTransaction failed: %v", err)
		}

		if retrieved.TxID != txID {
			t.Errorf("Expected TxID %s, got %s", txID, retrieved.TxID)
		}
		if retrieved.TxType != "test_transfer" {
			t.Errorf("Expected TxType test_transfer, got %s", retrieved.TxType)
		}
		if retrieved.Status != rte.TxStatusCreated {
			t.Errorf("Expected status CREATED, got %s", retrieved.Status)
		}
		if retrieved.TotalSteps != 2 {
			t.Errorf("Expected 2 steps, got %d", retrieved.TotalSteps)
		}
	})

	t.Run("Update_Transaction", func(t *testing.T) {
		txID := ti.GenerateTxID("crud-update")
		tx := rte.NewStoreTx(txID, "test_transfer", []string{"step1"})

		err := ti.StoreAdapter.CreateTransaction(ctx, tx)
		if err != nil {
			t.Fatalf("CreateTransaction failed: %v", err)
		}

		// Update status
		tx.Status = rte.TxStatusLocked
		now := time.Now()
		tx.LockedAt = &now
		tx.IncrementVersion()

		err = ti.StoreAdapter.UpdateTransaction(ctx, tx)
		if err != nil {
			t.Fatalf("UpdateTransaction failed: %v", err)
		}

		// Verify update
		retrieved, err := ti.StoreAdapter.GetTransaction(ctx, txID)
		if err != nil {
			t.Fatalf("GetTransaction failed: %v", err)
		}

		if retrieved.Status != rte.TxStatusLocked {
			t.Errorf("Expected status LOCKED, got %s", retrieved.Status)
		}
		if retrieved.Version != 1 {
			t.Errorf("Expected version 1, got %d", retrieved.Version)
		}
	})

	t.Run("Get_Transaction_NotFound", func(t *testing.T) {
		_, err := ti.StoreAdapter.GetTransaction(ctx, "non-existent-tx")
		if err == nil {
			t.Error("Expected error for non-existent transaction")
		}
	})

	t.Run("Create_Duplicate_Transaction", func(t *testing.T) {
		txID := ti.GenerateTxID("crud-dup")
		tx := rte.NewStoreTx(txID, "test_transfer", []string{"step1"})

		err := ti.StoreAdapter.CreateTransaction(ctx, tx)
		if err != nil {
			t.Fatalf("First CreateTransaction failed: %v", err)
		}

		// Try to create duplicate
		tx2 := rte.NewStoreTx(txID, "test_transfer", []string{"step1"})
		err = ti.StoreAdapter.CreateTransaction(ctx, tx2)
		if err == nil {
			t.Error("Expected error for duplicate transaction")
		}
	})
}

// TestIntegration_MySQLStore_StepCRUD tests step CRUD operations
func TestIntegration_MySQLStore_StepCRUD(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	// Create parent transaction first
	txID := ti.GenerateTxID("step-crud")
	tx := rte.NewStoreTx(txID, "test_transfer", []string{"debit", "credit", "notify"})
	err := ti.StoreAdapter.CreateTransaction(ctx, tx)
	if err != nil {
		t.Fatalf("CreateTransaction failed: %v", err)
	}

	t.Run("Create_Steps", func(t *testing.T) {
		for i, name := range []string{"debit", "credit", "notify"} {
			step := rte.NewStoreStepRecord(txID, i, name)
			err := ti.StoreAdapter.CreateStep(ctx, step)
			if err != nil {
				t.Fatalf("CreateStep %d failed: %v", i, err)
			}
			if step.ID == 0 {
				t.Errorf("Expected non-zero ID for step %d", i)
			}
		}
	})

	t.Run("Get_Step", func(t *testing.T) {
		step, err := ti.StoreAdapter.GetStep(ctx, txID, 0)
		if err != nil {
			t.Fatalf("GetStep failed: %v", err)
		}

		if step.StepName != "debit" {
			t.Errorf("Expected step name 'debit', got '%s'", step.StepName)
		}
		if step.Status != rte.StepStatusPending {
			t.Errorf("Expected status PENDING, got %s", step.Status)
		}
	})

	t.Run("Update_Step", func(t *testing.T) {
		step, err := ti.StoreAdapter.GetStep(ctx, txID, 0)
		if err != nil {
			t.Fatalf("GetStep failed: %v", err)
		}

		step.Status = rte.StepStatusExecuting
		now := time.Now()
		step.StartedAt = &now

		err = ti.StoreAdapter.UpdateStep(ctx, step)
		if err != nil {
			t.Fatalf("UpdateStep failed: %v", err)
		}

		// Verify
		updated, err := ti.StoreAdapter.GetStep(ctx, txID, 0)
		if err != nil {
			t.Fatalf("GetStep after update failed: %v", err)
		}

		if updated.Status != rte.StepStatusExecuting {
			t.Errorf("Expected status EXECUTING, got %s", updated.Status)
		}
	})

	t.Run("Get_All_Steps", func(t *testing.T) {
		steps, err := ti.StoreAdapter.GetSteps(ctx, txID)
		if err != nil {
			t.Fatalf("GetSteps failed: %v", err)
		}

		if len(steps) != 3 {
			t.Errorf("Expected 3 steps, got %d", len(steps))
		}

		// Verify order
		expectedNames := []string{"debit", "credit", "notify"}
		for i, step := range steps {
			if step.StepName != expectedNames[i] {
				t.Errorf("Step %d: expected name '%s', got '%s'", i, expectedNames[i], step.StepName)
			}
			if step.StepIndex != i {
				t.Errorf("Step %d: expected index %d, got %d", i, i, step.StepIndex)
			}
		}
	})

	t.Run("Get_Step_NotFound", func(t *testing.T) {
		_, err := ti.StoreAdapter.GetStep(ctx, txID, 99)
		if err == nil {
			t.Error("Expected error for non-existent step")
		}
	})
}

// TestIntegration_MySQLStore_OptimisticLock tests optimistic locking behavior
func TestIntegration_MySQLStore_OptimisticLock(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Version_Increments_On_Update", func(t *testing.T) {
		txID := ti.GenerateTxID("optlock-inc")
		tx := rte.NewStoreTx(txID, "test_transfer", []string{"step1"})

		err := ti.StoreAdapter.CreateTransaction(ctx, tx)
		if err != nil {
			t.Fatalf("CreateTransaction failed: %v", err)
		}

		initialVersion := tx.Version

		// Update multiple times
		for i := 0; i < 5; i++ {
			tx.IncrementVersion()
			err = ti.StoreAdapter.UpdateTransaction(ctx, tx)
			if err != nil {
				t.Fatalf("UpdateTransaction %d failed: %v", i, err)
			}
		}

		// Verify final version
		retrieved, err := ti.StoreAdapter.GetTransaction(ctx, txID)
		if err != nil {
			t.Fatalf("GetTransaction failed: %v", err)
		}

		expectedVersion := initialVersion + 5
		if retrieved.Version != expectedVersion {
			t.Errorf("Expected version %d, got %d", expectedVersion, retrieved.Version)
		}
	})

	t.Run("Concurrent_Updates_Version_Conflict", func(t *testing.T) {
		txID := ti.GenerateTxID("optlock-conflict")
		tx := rte.NewStoreTx(txID, "test_transfer", []string{"step1"})

		err := ti.StoreAdapter.CreateTransaction(ctx, tx)
		if err != nil {
			t.Fatalf("CreateTransaction failed: %v", err)
		}

		// Read transaction twice (simulating two concurrent readers)
		tx1, err := ti.StoreAdapter.GetTransaction(ctx, txID)
		if err != nil {
			t.Fatalf("GetTransaction 1 failed: %v", err)
		}

		tx2, err := ti.StoreAdapter.GetTransaction(ctx, txID)
		if err != nil {
			t.Fatalf("GetTransaction 2 failed: %v", err)
		}

		// First update succeeds
		tx1.Status = rte.TxStatusLocked
		tx1.IncrementVersion()
		err = ti.StoreAdapter.UpdateTransaction(ctx, tx1)
		if err != nil {
			t.Fatalf("First update should succeed: %v", err)
		}

		// Second update should fail with version conflict
		tx2.Status = rte.TxStatusExecuting
		tx2.IncrementVersion()
		err = ti.StoreAdapter.UpdateTransaction(ctx, tx2)
		if err == nil {
			t.Error("Second update should fail with version conflict")
		}
	})

	t.Run("Concurrent_Updates_Only_One_Succeeds", func(t *testing.T) {
		txID := ti.GenerateTxID("optlock-race")
		tx := rte.NewStoreTx(txID, "test_transfer", []string{"step1"})

		err := ti.StoreAdapter.CreateTransaction(ctx, tx)
		if err != nil {
			t.Fatalf("CreateTransaction failed: %v", err)
		}

		// Read transaction once to get the initial state
		initialTx, err := ti.StoreAdapter.GetTransaction(ctx, txID)
		if err != nil {
			t.Fatalf("GetTransaction failed: %v", err)
		}
		initialVersion := initialTx.Version

		// Run concurrent updates - all starting from the same version
		const numGoroutines = 10
		var wg sync.WaitGroup
		successCount := 0
		var mu sync.Mutex

		// Use a barrier to ensure all goroutines start at the same time
		startCh := make(chan struct{})

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				// Create a copy with the same initial version
				localTx := &rte.StoreTx{
					TxID:        txID,
					TxType:      initialTx.TxType,
					Status:      initialTx.Status,
					CurrentStep: initialTx.CurrentStep,
					TotalSteps:  initialTx.TotalSteps,
					StepNames:   initialTx.StepNames,
					Context:     initialTx.Context,
					ErrorMsg:    "updated by goroutine",
					RetryCount:  initialTx.RetryCount,
					MaxRetries:  initialTx.MaxRetries,
					Version:     initialVersion,
					CreatedAt:   initialTx.CreatedAt,
					UpdatedAt:   initialTx.UpdatedAt,
				}

				// Wait for signal to start
				<-startCh

				localTx.IncrementVersion()
				err := ti.StoreAdapter.UpdateTransaction(ctx, localTx)
				if err == nil {
					mu.Lock()
					successCount++
					mu.Unlock()
				}
			}(i)
		}

		// Signal all goroutines to start
		close(startCh)
		wg.Wait()

		// Only one update should succeed since all start from the same version
		if successCount != 1 {
			t.Errorf("Expected exactly 1 successful update, got %d", successCount)
		}
	})
}

// TestIntegration_MySQLStore_RecoveryQueries tests recovery-related queries
func TestIntegration_MySQLStore_RecoveryQueries(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("GetStuckTransactions", func(t *testing.T) {
		// Create a stuck transaction (EXECUTING status, old updated_at)
		txID := ti.GenerateTxID("stuck")
		tx := rte.NewStoreTx(txID, "test_transfer", []string{"step1"})

		err := ti.StoreAdapter.CreateTransaction(ctx, tx)
		if err != nil {
			t.Fatalf("CreateTransaction failed: %v", err)
		}

		// Update to EXECUTING status
		tx.Status = rte.TxStatusExecuting
		tx.IncrementVersion()
		err = ti.StoreAdapter.UpdateTransaction(ctx, tx)
		if err != nil {
			t.Fatalf("UpdateTransaction failed: %v", err)
		}

		// Manually update the updated_at to make it "old"
		_, err = ti.DB.ExecContext(ctx,
			"UPDATE rte_transactions SET updated_at = DATE_SUB(NOW(), INTERVAL 10 MINUTE) WHERE tx_id = ?",
			txID)
		if err != nil {
			t.Fatalf("Failed to backdate transaction: %v", err)
		}

		// Query for stuck transactions
		stuckTxs, err := ti.StoreAdapter.GetStuckTransactions(ctx, 5*time.Minute)
		if err != nil {
			t.Fatalf("GetStuckTransactions failed: %v", err)
		}

		// Find our transaction
		found := false
		for _, stx := range stuckTxs {
			if stx.TxID == txID {
				found = true
				break
			}
		}

		if !found {
			t.Error("Expected to find stuck transaction")
		}
	})

	t.Run("GetRetryableTransactions", func(t *testing.T) {
		// Create a failed transaction with retry count < max
		txID := ti.GenerateTxID("retryable")
		tx := rte.NewStoreTx(txID, "test_transfer", []string{"step1"})
		tx.MaxRetries = 3

		err := ti.StoreAdapter.CreateTransaction(ctx, tx)
		if err != nil {
			t.Fatalf("CreateTransaction failed: %v", err)
		}

		// Update to FAILED status with retry count 1
		tx.Status = rte.TxStatusFailed
		tx.RetryCount = 1
		tx.ErrorMsg = "test error"
		tx.IncrementVersion()
		err = ti.StoreAdapter.UpdateTransaction(ctx, tx)
		if err != nil {
			t.Fatalf("UpdateTransaction failed: %v", err)
		}

		// Query for retryable transactions
		retryableTxs, err := ti.StoreAdapter.GetRetryableTransactions(ctx, 3)
		if err != nil {
			t.Fatalf("GetRetryableTransactions failed: %v", err)
		}

		// Find our transaction
		found := false
		for _, rtx := range retryableTxs {
			if rtx.TxID == txID {
				found = true
				if rtx.RetryCount >= 3 {
					t.Error("Transaction should have retry count < max")
				}
				break
			}
		}

		if !found {
			t.Error("Expected to find retryable transaction")
		}
	})

	t.Run("GetPendingTransactions", func(t *testing.T) {
		// Create a pending transaction (CREATED status, old updated_at)
		txID := ti.GenerateTxID("pending")
		tx := rte.NewStoreTx(txID, "test_transfer", []string{"step1"})

		err := ti.StoreAdapter.CreateTransaction(ctx, tx)
		if err != nil {
			t.Fatalf("CreateTransaction failed: %v", err)
		}

		// Manually update the updated_at to make it "old"
		_, err = ti.DB.ExecContext(ctx,
			"UPDATE rte_transactions SET updated_at = DATE_SUB(NOW(), INTERVAL 10 MINUTE) WHERE tx_id = ?",
			txID)
		if err != nil {
			t.Fatalf("Failed to backdate transaction: %v", err)
		}

		// Query for pending transactions
		pendingTxs, err := ti.StoreAdapter.GetPendingTransactions(ctx, 5*time.Minute)
		if err != nil {
			t.Fatalf("GetPendingTransactions failed: %v", err)
		}

		// Find our transaction
		found := false
		for _, ptx := range pendingTxs {
			if ptx.TxID == txID {
				found = true
				break
			}
		}

		if !found {
			t.Error("Expected to find pending transaction")
		}
	})
}

// TestIntegration_MySQLStore_Idempotency tests idempotency operations
func TestIntegration_MySQLStore_Idempotency(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	t.Run("Mark_And_Check_Idempotency", func(t *testing.T) {
		key := ti.GenerateTxID("idem-key")
		result := []byte(`{"status":"success","amount":100}`)

		// Check - should not exist
		exists, _, err := ti.StoreAdapter.CheckIdempotency(ctx, key)
		if err != nil {
			t.Fatalf("CheckIdempotency failed: %v", err)
		}
		if exists {
			t.Error("Key should not exist initially")
		}

		// Mark
		err = ti.StoreAdapter.MarkIdempotency(ctx, key, result, 24*time.Hour)
		if err != nil {
			t.Fatalf("MarkIdempotency failed: %v", err)
		}

		// Check again - should exist
		exists, retrieved, err := ti.StoreAdapter.CheckIdempotency(ctx, key)
		if err != nil {
			t.Fatalf("CheckIdempotency after mark failed: %v", err)
		}
		if !exists {
			t.Error("Key should exist after marking")
		}
		if string(retrieved) != string(result) {
			t.Errorf("Expected result %s, got %s", result, retrieved)
		}
	})

	t.Run("Idempotency_Expiration", func(t *testing.T) {
		key := ti.GenerateTxID("idem-expire")
		result := []byte(`{"expired":true}`)

		// Mark with short TTL (1 second - MySQL datetime has second precision)
		err := ti.StoreAdapter.MarkIdempotency(ctx, key, result, 1*time.Second)
		if err != nil {
			t.Fatalf("MarkIdempotency failed: %v", err)
		}

		// Wait for expiration
		time.Sleep(2 * time.Second)

		// Check - should not exist (expired)
		exists, _, err := ti.StoreAdapter.CheckIdempotency(ctx, key)
		if err != nil {
			t.Fatalf("CheckIdempotency failed: %v", err)
		}
		if exists {
			t.Error("Key should have expired")
		}
	})
}

// TestIntegration_MySQLStore_ListTransactions tests listing with filters
func TestIntegration_MySQLStore_ListTransactions(t *testing.T) {
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()
	defer ti.Cleanup(t)

	ctx := context.Background()

	// Create test transactions with different statuses
	statuses := []rte.TxStatus{
		rte.TxStatusCreated,
		rte.TxStatusCompleted,
		rte.TxStatusFailed,
		rte.TxStatusCompleted,
	}

	for i, status := range statuses {
		txID := ti.GenerateTxID("list-" + string(rune('a'+i)))
		tx := rte.NewStoreTx(txID, "test_transfer", []string{"step1"})

		err := ti.StoreAdapter.CreateTransaction(ctx, tx)
		if err != nil {
			t.Fatalf("CreateTransaction failed: %v", err)
		}

		if status != rte.TxStatusCreated {
			tx.Status = status
			tx.IncrementVersion()
			err = ti.StoreAdapter.UpdateTransaction(ctx, tx)
			if err != nil {
				t.Fatalf("UpdateTransaction failed: %v", err)
			}
		}
	}

	t.Run("Filter_By_Status", func(t *testing.T) {
		filter := &rte.StoreTxFilter{
			Status: []rte.TxStatus{rte.TxStatusCompleted},
			Limit:  100,
			Offset: 0,
		}

		txs, total, err := ti.StoreAdapter.ListTransactions(ctx, filter)
		if err != nil {
			t.Fatalf("ListTransactions failed: %v", err)
		}

		if total < 2 {
			t.Errorf("Expected at least 2 completed transactions, got %d", total)
		}

		for _, tx := range txs {
			if tx.Status != rte.TxStatusCompleted {
				t.Errorf("Expected status COMPLETED, got %s", tx.Status)
			}
		}
	})

	t.Run("Pagination", func(t *testing.T) {
		filter := &rte.StoreTxFilter{
			Limit:  2,
			Offset: 0,
		}

		txs, total, err := ti.StoreAdapter.ListTransactions(ctx, filter)
		if err != nil {
			t.Fatalf("ListTransactions failed: %v", err)
		}

		if len(txs) > 2 {
			t.Errorf("Expected at most 2 transactions, got %d", len(txs))
		}

		if total < int64(len(txs)) {
			t.Errorf("Total %d should be >= returned count %d", total, len(txs))
		}
	})
}
