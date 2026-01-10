package rte

import (
	"testing"

	"pgregory.net/rapid"
)

// ============================================================================
// Unit Tests for store_interface.go
// Tests ToTxContext, NewStoreTxContext, StoreTx.IsTerminal, StoreTx.IsFailed
// ============================================================================

func TestToTxContext_NilStoreTxContext(t *testing.T) {
	var storeCtx *StoreTxContext = nil
	result := storeCtx.ToTxContext()
	if result != nil {
		t.Error("ToTxContext on nil should return nil")
	}
}

func TestToTxContext_EmptyContext(t *testing.T) {
	storeCtx := &StoreTxContext{
		TxID:   "test-tx",
		TxType: "test-type",
	}

	result := storeCtx.ToTxContext()

	if result == nil {
		t.Fatal("ToTxContext should not return nil for non-nil input")
	}
	if result.TxID != "test-tx" {
		t.Errorf("TxID: expected test-tx, got %s", result.TxID)
	}
	if result.TxType != "test-type" {
		t.Errorf("TxType: expected test-type, got %s", result.TxType)
	}
	if result.StepIndex != 0 {
		t.Errorf("StepIndex: expected 0, got %d", result.StepIndex)
	}
}

func TestToTxContext_WithAllFields(t *testing.T) {
	storeCtx := &StoreTxContext{
		TxID:      "test-tx",
		TxType:    "test-type",
		StepIndex: 5,
		Input: map[string]any{
			"key1": "value1",
			"key2": 42,
		},
		Output: map[string]any{
			"result": "success",
		},
		Metadata: map[string]string{
			"meta1": "data1",
		},
	}

	result := storeCtx.ToTxContext()

	if result.TxID != "test-tx" {
		t.Errorf("TxID: expected test-tx, got %s", result.TxID)
	}
	if result.TxType != "test-type" {
		t.Errorf("TxType: expected test-type, got %s", result.TxType)
	}
	if result.StepIndex != 5 {
		t.Errorf("StepIndex: expected 5, got %d", result.StepIndex)
	}

	// Check input
	val, ok := result.GetInput("key1")
	if !ok || val != "value1" {
		t.Error("Input key1 not preserved")
	}
	val, ok = result.GetInput("key2")
	if !ok || val != 42 {
		t.Error("Input key2 not preserved")
	}

	// Check output
	val, ok = result.GetOutput("result")
	if !ok || val != "success" {
		t.Error("Output result not preserved")
	}

	// Check metadata
	meta, ok := result.GetMetadata("meta1")
	if !ok || meta != "data1" {
		t.Error("Metadata meta1 not preserved")
	}
}

func TestToTxContext_NilMaps(t *testing.T) {
	storeCtx := &StoreTxContext{
		TxID:     "test-tx",
		TxType:   "test-type",
		Input:    nil,
		Output:   nil,
		Metadata: nil,
	}

	result := storeCtx.ToTxContext()

	// Should not panic and should have initialized maps
	if result == nil {
		t.Fatal("ToTxContext should not return nil")
	}

	// Maps should be initialized (not nil)
	if result.Input == nil {
		t.Error("Input map should be initialized")
	}
	if result.Output == nil {
		t.Error("Output map should be initialized")
	}
	if result.Metadata == nil {
		t.Error("Metadata map should be initialized")
	}
}

func TestNewStoreTxContext_NilTxContext(t *testing.T) {
	var txCtx *TxContext = nil
	result := NewStoreTxContext(txCtx)
	if result != nil {
		t.Error("NewStoreTxContext on nil should return nil")
	}
}

func TestNewStoreTxContext_EmptyContext(t *testing.T) {
	txCtx := NewTxContext("test-tx", "test-type")

	result := NewStoreTxContext(txCtx)

	if result == nil {
		t.Fatal("NewStoreTxContext should not return nil for non-nil input")
	}
	if result.TxID != "test-tx" {
		t.Errorf("TxID: expected test-tx, got %s", result.TxID)
	}
	if result.TxType != "test-type" {
		t.Errorf("TxType: expected test-type, got %s", result.TxType)
	}
}

func TestNewStoreTxContext_WithAllFields(t *testing.T) {
	txCtx := NewTxContext("test-tx", "test-type")
	txCtx.StepIndex = 3
	txCtx.SetInput("key1", "value1")
	txCtx.SetOutput("result", "success")
	txCtx.SetMetadata("meta1", "data1")

	result := NewStoreTxContext(txCtx)

	if result.TxID != "test-tx" {
		t.Errorf("TxID: expected test-tx, got %s", result.TxID)
	}
	if result.TxType != "test-type" {
		t.Errorf("TxType: expected test-type, got %s", result.TxType)
	}
	if result.StepIndex != 3 {
		t.Errorf("StepIndex: expected 3, got %d", result.StepIndex)
	}

	// Check input
	if result.Input["key1"] != "value1" {
		t.Error("Input key1 not preserved")
	}

	// Check output
	if result.Output["result"] != "success" {
		t.Error("Output result not preserved")
	}

	// Check metadata
	if result.Metadata["meta1"] != "data1" {
		t.Error("Metadata meta1 not preserved")
	}
}

func TestStoreTx_IsTerminal(t *testing.T) {
	tests := []struct {
		status   TxStatus
		expected bool
	}{
		{TxStatusCreated, false},
		{TxStatusLocked, false},
		{TxStatusExecuting, false},
		{TxStatusConfirming, false},
		{TxStatusCompleted, true},
		{TxStatusFailed, false},
		{TxStatusCompensating, false},
		{TxStatusCompensated, true},
		{TxStatusCompensationFailed, true},
		{TxStatusCancelled, true},
		{TxStatusTimeout, true},
	}

	for _, tt := range tests {
		tx := &StoreTx{Status: tt.status}
		if tx.IsTerminal() != tt.expected {
			t.Errorf("StoreTx.IsTerminal() for %s: expected %v, got %v",
				tt.status, tt.expected, tx.IsTerminal())
		}
	}
}

func TestStoreTx_IsFailed(t *testing.T) {
	tests := []struct {
		status   TxStatus
		expected bool
	}{
		{TxStatusCreated, false},
		{TxStatusLocked, false},
		{TxStatusExecuting, false},
		{TxStatusConfirming, false},
		{TxStatusCompleted, false},
		{TxStatusFailed, true},
		{TxStatusCompensating, false},
		{TxStatusCompensated, false},
		{TxStatusCompensationFailed, true},
		{TxStatusCancelled, false},
		{TxStatusTimeout, true},
	}

	for _, tt := range tests {
		tx := &StoreTx{Status: tt.status}
		if tx.IsFailed() != tt.expected {
			t.Errorf("StoreTx.IsFailed() for %s: expected %v, got %v",
				tt.status, tt.expected, tx.IsFailed())
		}
	}
}

func TestStoreTx_CanRetry(t *testing.T) {
	tests := []struct {
		status     TxStatus
		retryCount int
		maxRetries int
		expected   bool
	}{
		// Can retry: FAILED status and retryCount < maxRetries
		{TxStatusFailed, 0, 3, true},
		{TxStatusFailed, 1, 3, true},
		{TxStatusFailed, 2, 3, true},
		// Cannot retry: retryCount >= maxRetries
		{TxStatusFailed, 3, 3, false},
		{TxStatusFailed, 4, 3, false},
		// Cannot retry: wrong status
		{TxStatusCompleted, 0, 3, false},
		{TxStatusExecuting, 0, 3, false},
		{TxStatusCompensated, 0, 3, false},
	}

	for _, tt := range tests {
		tx := &StoreTx{
			Status:     tt.status,
			RetryCount: tt.retryCount,
			MaxRetries: tt.maxRetries,
		}
		if tx.CanRetry() != tt.expected {
			t.Errorf("StoreTx.CanRetry() for status=%s, retryCount=%d, maxRetries=%d: expected %v, got %v",
				tt.status, tt.retryCount, tt.maxRetries, tt.expected, tx.CanRetry())
		}
	}
}

func TestStoreTx_IncrementVersion(t *testing.T) {
	tx := &StoreTx{Version: 0}

	tx.IncrementVersion()
	if tx.Version != 1 {
		t.Errorf("Version: expected 1, got %d", tx.Version)
	}

	tx.IncrementVersion()
	if tx.Version != 2 {
		t.Errorf("Version: expected 2, got %d", tx.Version)
	}

	// UpdatedAt should be set
	if tx.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should be set after IncrementVersion")
	}
}

func TestNewStoreTx(t *testing.T) {
	stepNames := []string{"step1", "step2", "step3"}
	tx := NewStoreTx("test-tx", "test-type", stepNames)

	if tx.TxID != "test-tx" {
		t.Errorf("TxID: expected test-tx, got %s", tx.TxID)
	}
	if tx.TxType != "test-type" {
		t.Errorf("TxType: expected test-type, got %s", tx.TxType)
	}
	if tx.Status != TxStatusCreated {
		t.Errorf("Status: expected CREATED, got %s", tx.Status)
	}
	if tx.CurrentStep != 0 {
		t.Errorf("CurrentStep: expected 0, got %d", tx.CurrentStep)
	}
	if tx.TotalSteps != 3 {
		t.Errorf("TotalSteps: expected 3, got %d", tx.TotalSteps)
	}
	if len(tx.StepNames) != 3 {
		t.Errorf("StepNames length: expected 3, got %d", len(tx.StepNames))
	}
	if tx.RetryCount != 0 {
		t.Errorf("RetryCount: expected 0, got %d", tx.RetryCount)
	}
	if tx.MaxRetries != 3 {
		t.Errorf("MaxRetries: expected 3, got %d", tx.MaxRetries)
	}
	if tx.Version != 0 {
		t.Errorf("Version: expected 0, got %d", tx.Version)
	}
	if tx.Context == nil {
		t.Error("Context should not be nil")
	}
}

func TestNewStoreStepRecord(t *testing.T) {
	record := NewStoreStepRecord("test-tx", 2, "step-name")

	if record.TxID != "test-tx" {
		t.Errorf("TxID: expected test-tx, got %s", record.TxID)
	}
	if record.StepIndex != 2 {
		t.Errorf("StepIndex: expected 2, got %d", record.StepIndex)
	}
	if record.StepName != "step-name" {
		t.Errorf("StepName: expected step-name, got %s", record.StepName)
	}
	if record.Status != StepStatusPending {
		t.Errorf("Status: expected PENDING, got %s", record.Status)
	}
	if record.RetryCount != 0 {
		t.Errorf("RetryCount: expected 0, got %d", record.RetryCount)
	}
}

// ============================================================================
// Property Test for StoreTxContext Round-Trip
// Property 4: StoreTxContext Round-Trip
// *For any* TxContext with non-nil fields, converting to StoreTxContext and back
// via ToTxContext should preserve all data (TxID, TxType, StepIndex, Input, Output, Metadata).
// ============================================================================

func TestProperty_StoreTxContextRoundTrip(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate random TxContext data
		txID := rapid.StringMatching(`^tx-[a-z0-9]{8}$`).Draw(rt, "txID")
		txType := rapid.StringMatching(`^type-[a-z]{4}$`).Draw(rt, "txType")
		stepIndex := rapid.IntRange(0, 100).Draw(rt, "stepIndex")

		// Generate random input map
		numInputs := rapid.IntRange(0, 5).Draw(rt, "numInputs")
		inputMap := make(map[string]any)
		for i := 0; i < numInputs; i++ {
			key := rapid.StringMatching(`^key-[a-z0-9]{4}$`).Draw(rt, "inputKey")
			value := rapid.StringMatching(`^val-[a-z0-9]{8}$`).Draw(rt, "inputValue")
			inputMap[key] = value
		}

		// Generate random output map
		numOutputs := rapid.IntRange(0, 5).Draw(rt, "numOutputs")
		outputMap := make(map[string]any)
		for i := 0; i < numOutputs; i++ {
			key := rapid.StringMatching(`^out-[a-z0-9]{4}$`).Draw(rt, "outputKey")
			value := rapid.StringMatching(`^res-[a-z0-9]{8}$`).Draw(rt, "outputValue")
			outputMap[key] = value
		}

		// Generate random metadata map
		numMeta := rapid.IntRange(0, 5).Draw(rt, "numMeta")
		metaMap := make(map[string]string)
		for i := 0; i < numMeta; i++ {
			key := rapid.StringMatching(`^meta-[a-z0-9]{4}$`).Draw(rt, "metaKey")
			value := rapid.StringMatching(`^data-[a-z0-9]{8}$`).Draw(rt, "metaValue")
			metaMap[key] = value
		}

		// Create original TxContext
		original := NewTxContext(txID, txType)
		original.StepIndex = stepIndex
		for k, v := range inputMap {
			original.SetInput(k, v)
		}
		for k, v := range outputMap {
			original.SetOutput(k, v)
		}
		for k, v := range metaMap {
			original.SetMetadata(k, v)
		}

		// Convert to StoreTxContext
		storeCtx := NewStoreTxContext(original)

		// Convert back to TxContext
		restored := storeCtx.ToTxContext()

		// Property: Basic fields should be preserved
		if restored.TxID != original.TxID {
			rt.Fatalf("TxID not preserved: expected %s, got %s", original.TxID, restored.TxID)
		}
		if restored.TxType != original.TxType {
			rt.Fatalf("TxType not preserved: expected %s, got %s", original.TxType, restored.TxType)
		}
		if restored.StepIndex != original.StepIndex {
			rt.Fatalf("StepIndex not preserved: expected %d, got %d", original.StepIndex, restored.StepIndex)
		}

		// Property: Input map should be preserved
		for k, v := range inputMap {
			restoredVal, ok := restored.GetInput(k)
			if !ok {
				rt.Fatalf("Input key %s not found after round-trip", k)
			}
			if restoredVal != v {
				rt.Fatalf("Input value for key %s not preserved: expected %v, got %v", k, v, restoredVal)
			}
		}

		// Property: Output map should be preserved
		for k, v := range outputMap {
			restoredVal, ok := restored.GetOutput(k)
			if !ok {
				rt.Fatalf("Output key %s not found after round-trip", k)
			}
			if restoredVal != v {
				rt.Fatalf("Output value for key %s not preserved: expected %v, got %v", k, v, restoredVal)
			}
		}

		// Property: Metadata map should be preserved
		for k, v := range metaMap {
			restoredVal, ok := restored.GetMetadata(k)
			if !ok {
				rt.Fatalf("Metadata key %s not found after round-trip", k)
			}
			if restoredVal != v {
				rt.Fatalf("Metadata value for key %s not preserved: expected %v, got %v", k, v, restoredVal)
			}
		}
	})
}
