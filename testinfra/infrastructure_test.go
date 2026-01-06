// Package testinfra provides test infrastructure for RTE production validation.
package testinfra

import (
	"context"
	"testing"
	"time"
)

// TestInfrastructureConnection validates MySQL and Redis connections
func TestInfrastructureConnection(t *testing.T) {
	// Skip if infrastructure is not available
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()

	t.Run("MySQL_Connection", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Test ping
		if err := ti.DB.PingContext(ctx); err != nil {
			t.Fatalf("MySQL ping failed: %v", err)
		}

		// Test simple query
		var result int
		err := ti.DB.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		if err != nil {
			t.Fatalf("MySQL query failed: %v", err)
		}
		if result != 1 {
			t.Fatalf("Expected 1, got %d", result)
		}

		t.Log("MySQL connection verified successfully")
	})

	t.Run("Redis_Connection", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Test ping
		if err := ti.Redis.Ping(ctx).Err(); err != nil {
			t.Fatalf("Redis ping failed: %v", err)
		}

		// Test set/get
		testKey := ti.TestID() + "-test-key"
		testValue := "test-value"

		err := ti.Redis.Set(ctx, testKey, testValue, time.Minute).Err()
		if err != nil {
			t.Fatalf("Redis SET failed: %v", err)
		}

		got, err := ti.Redis.Get(ctx, testKey).Result()
		if err != nil {
			t.Fatalf("Redis GET failed: %v", err)
		}
		if got != testValue {
			t.Fatalf("Expected %s, got %s", testValue, got)
		}

		// Cleanup test key
		ti.Redis.Del(ctx, testKey)

		t.Log("Redis connection verified successfully")
	})
}

// TestInfrastructureCleanup validates the cleanup functionality
func TestInfrastructureCleanup(t *testing.T) {
	// Skip if infrastructure is not available
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()

	ctx := context.Background()
	txID := ti.GenerateTxID("cleanup-test")

	t.Run("Create_Test_Data", func(t *testing.T) {
		// Insert a test transaction
		_, err := ti.DB.ExecContext(ctx, `
			INSERT INTO rte_transactions (tx_id, tx_type, status, total_steps, context)
			VALUES (?, 'test', 'CREATED', 1, '{}')
		`, txID)
		if err != nil {
			t.Fatalf("Failed to insert test transaction: %v", err)
		}

		// Insert a test step
		_, err = ti.DB.ExecContext(ctx, `
			INSERT INTO rte_steps (tx_id, step_index, step_name, status)
			VALUES (?, 0, 'test-step', 'PENDING')
		`, txID)
		if err != nil {
			t.Fatalf("Failed to insert test step: %v", err)
		}

		// Insert a test idempotency record
		idemKey := ti.TestID() + "-idem-test"
		_, err = ti.DB.ExecContext(ctx, `
			INSERT INTO rte_idempotency (idempotency_key, result, expires_at)
			VALUES (?, NULL, DATE_ADD(NOW(), INTERVAL 1 HOUR))
		`, idemKey)
		if err != nil {
			t.Fatalf("Failed to insert test idempotency: %v", err)
		}

		// Create a test Redis lock key
		lockKey := "rte:lock:" + ti.TestID() + "-lock-test"
		err = ti.Redis.Set(ctx, lockKey, "test-value", time.Minute).Err()
		if err != nil {
			t.Fatalf("Failed to create test Redis key: %v", err)
		}

		t.Log("Test data created successfully")
	})

	t.Run("Verify_Data_Exists", func(t *testing.T) {
		// Verify transaction exists
		var count int
		err := ti.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM rte_transactions WHERE tx_id = ?", txID).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query transaction: %v", err)
		}
		if count != 1 {
			t.Fatalf("Expected 1 transaction, got %d", count)
		}

		// Verify step exists
		err = ti.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM rte_steps WHERE tx_id = ?", txID).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query step: %v", err)
		}
		if count != 1 {
			t.Fatalf("Expected 1 step, got %d", count)
		}

		t.Log("Test data verified to exist")
	})

	t.Run("Cleanup", func(t *testing.T) {
		ti.Cleanup(t)
		t.Log("Cleanup executed")
	})

	t.Run("Verify_Data_Cleaned", func(t *testing.T) {
		// Verify transaction is cleaned
		var count int
		err := ti.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM rte_transactions WHERE tx_id = ?", txID).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query transaction: %v", err)
		}
		if count != 0 {
			t.Fatalf("Expected 0 transactions after cleanup, got %d", count)
		}

		// Verify step is cleaned (should be cascade deleted or explicitly deleted)
		err = ti.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM rte_steps WHERE tx_id = ?", txID).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to query step: %v", err)
		}
		if count != 0 {
			t.Fatalf("Expected 0 steps after cleanup, got %d", count)
		}

		t.Log("Test data verified to be cleaned")
	})
}

// TestInfrastructureComponents validates all infrastructure components are initialized
func TestInfrastructureComponents(t *testing.T) {
	// Skip if infrastructure is not available
	SkipIfNoInfrastructure(t)

	ti := NewTestInfrastructure(t)
	defer ti.Close()

	t.Run("DB_Not_Nil", func(t *testing.T) {
		if ti.DB == nil {
			t.Fatal("DB should not be nil")
		}
	})

	t.Run("Redis_Not_Nil", func(t *testing.T) {
		if ti.Redis == nil {
			t.Fatal("Redis should not be nil")
		}
	})

	t.Run("MySQLStore_Not_Nil", func(t *testing.T) {
		if ti.MySQLStore == nil {
			t.Fatal("MySQLStore should not be nil")
		}
	})

	t.Run("StoreAdapter_Not_Nil", func(t *testing.T) {
		if ti.StoreAdapter == nil {
			t.Fatal("StoreAdapter should not be nil")
		}
	})

	t.Run("Locker_Not_Nil", func(t *testing.T) {
		if ti.Locker == nil {
			t.Fatal("Locker should not be nil")
		}
	})

	t.Run("EventBus_Not_Nil", func(t *testing.T) {
		if ti.EventBus == nil {
			t.Fatal("EventBus should not be nil")
		}
	})

	t.Run("Breaker_Not_Nil", func(t *testing.T) {
		if ti.Breaker == nil {
			t.Fatal("Breaker should not be nil")
		}
	})

	t.Run("Engine_Not_Nil", func(t *testing.T) {
		if ti.Engine == nil {
			t.Fatal("Engine should not be nil")
		}
	})

	t.Run("TestID_Not_Empty", func(t *testing.T) {
		if ti.TestID() == "" {
			t.Fatal("TestID should not be empty")
		}
	})

	t.Log("All infrastructure components initialized successfully")
}
