package rte

import (
	"testing"

	"pgregory.net/rapid"
)

// ============================================================================
// Unit Tests for context.go
// Tests SetInput, GetOutput, Clone, and WithInput functions
// ============================================================================

func TestSetInput_NilMapInitialization(t *testing.T) {
	// Create context with nil Input map
	ctx := &TxContext{
		TxID:   "test-tx",
		TxType: "test",
		Input:  nil, // Explicitly nil
	}

	// SetInput should initialize the map
	ctx.SetInput("key1", "value1")

	// Verify the map was initialized and value was set
	if ctx.Input == nil {
		t.Fatal("Input map should be initialized after SetInput")
	}

	val, ok := ctx.GetInput("key1")
	if !ok {
		t.Fatal("key1 should exist after SetInput")
	}
	if val != "value1" {
		t.Errorf("expected value1, got %v", val)
	}
}

func TestSetInput_ExistingMap(t *testing.T) {
	ctx := NewTxContext("test-tx", "test")

	ctx.SetInput("key1", "value1")
	ctx.SetInput("key2", 42)
	ctx.SetInput("key3", true)

	// Verify all values
	tests := []struct {
		key      string
		expected any
	}{
		{"key1", "value1"},
		{"key2", 42},
		{"key3", true},
	}

	for _, tt := range tests {
		val, ok := ctx.GetInput(tt.key)
		if !ok {
			t.Errorf("key %s should exist", tt.key)
			continue
		}
		if val != tt.expected {
			t.Errorf("key %s: expected %v, got %v", tt.key, tt.expected, val)
		}
	}
}

func TestSetInput_OverwriteExisting(t *testing.T) {
	ctx := NewTxContext("test-tx", "test")

	ctx.SetInput("key1", "original")
	ctx.SetInput("key1", "updated")

	val, ok := ctx.GetInput("key1")
	if !ok {
		t.Fatal("key1 should exist")
	}
	if val != "updated" {
		t.Errorf("expected updated, got %v", val)
	}
}

func TestGetOutput_ExistingKey(t *testing.T) {
	ctx := NewTxContext("test-tx", "test")

	ctx.SetOutput("result", "success")
	ctx.SetOutput("count", 100)

	// Test existing keys
	val, ok := ctx.GetOutput("result")
	if !ok {
		t.Fatal("result should exist")
	}
	if val != "success" {
		t.Errorf("expected success, got %v", val)
	}

	val, ok = ctx.GetOutput("count")
	if !ok {
		t.Fatal("count should exist")
	}
	if val != 100 {
		t.Errorf("expected 100, got %v", val)
	}
}

func TestGetOutput_NonExistingKey(t *testing.T) {
	ctx := NewTxContext("test-tx", "test")

	val, ok := ctx.GetOutput("nonexistent")
	if ok {
		t.Error("nonexistent key should return false")
	}
	if val != nil {
		t.Errorf("expected nil for nonexistent key, got %v", val)
	}
}

func TestGetOutput_NilValue(t *testing.T) {
	ctx := NewTxContext("test-tx", "test")
	ctx.SetOutput("nilkey", nil)

	val, ok := ctx.GetOutput("nilkey")
	if !ok {
		t.Error("nilkey should exist even with nil value")
	}
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}
}

func TestClone_Independence(t *testing.T) {
	original := NewTxContext("test-tx", "test")
	original.StepIndex = 5
	original.SetInput("key1", "value1")
	original.SetOutput("out1", "result1")
	original.SetMetadata("meta1", "data1")

	clone := original.Clone()

	// Verify clone has same values
	if clone.TxID != original.TxID {
		t.Errorf("TxID mismatch: expected %s, got %s", original.TxID, clone.TxID)
	}
	if clone.TxType != original.TxType {
		t.Errorf("TxType mismatch: expected %s, got %s", original.TxType, clone.TxType)
	}
	if clone.StepIndex != original.StepIndex {
		t.Errorf("StepIndex mismatch: expected %d, got %d", original.StepIndex, clone.StepIndex)
	}

	// Verify maps are independent - modify clone
	clone.SetInput("key1", "modified")
	clone.SetOutput("out1", "modified")
	clone.SetMetadata("meta1", "modified")

	// Original should be unchanged
	val, _ := original.GetInput("key1")
	if val != "value1" {
		t.Errorf("original Input was modified: expected value1, got %v", val)
	}

	val, _ = original.GetOutput("out1")
	if val != "result1" {
		t.Errorf("original Output was modified: expected result1, got %v", val)
	}

	meta, _ := original.GetMetadata("meta1")
	if meta != "data1" {
		t.Errorf("original Metadata was modified: expected data1, got %s", meta)
	}
}

func TestClone_EmptyMaps(t *testing.T) {
	original := NewTxContext("test-tx", "test")
	clone := original.Clone()

	// Verify clone has empty but initialized maps
	if clone.Input == nil {
		t.Error("clone Input should not be nil")
	}
	if clone.Output == nil {
		t.Error("clone Output should not be nil")
	}
	if clone.Metadata == nil {
		t.Error("clone Metadata should not be nil")
	}

	// Verify maps are empty
	if len(clone.Input) != 0 {
		t.Errorf("clone Input should be empty, got %d items", len(clone.Input))
	}
	if len(clone.Output) != 0 {
		t.Errorf("clone Output should be empty, got %d items", len(clone.Output))
	}
	if len(clone.Metadata) != 0 {
		t.Errorf("clone Metadata should be empty, got %d items", len(clone.Metadata))
	}
}

func TestWithInput_MergesInput(t *testing.T) {
	ctx := NewTxContext("test-tx", "test")
	ctx.SetInput("existing", "value")

	newInput := map[string]any{
		"new1": "newvalue1",
		"new2": 42,
	}

	result := ctx.WithInput(newInput)

	// Verify it returns the same context
	if result != ctx {
		t.Error("WithInput should return the same context")
	}

	// Verify existing key is preserved
	val, ok := ctx.GetInput("existing")
	if !ok || val != "value" {
		t.Error("existing key should be preserved")
	}

	// Verify new keys are added
	val, ok = ctx.GetInput("new1")
	if !ok || val != "newvalue1" {
		t.Error("new1 should be added")
	}

	val, ok = ctx.GetInput("new2")
	if !ok || val != 42 {
		t.Error("new2 should be added")
	}
}

func TestWithInput_OverwritesExisting(t *testing.T) {
	ctx := NewTxContext("test-tx", "test")
	ctx.SetInput("key1", "original")

	ctx.WithInput(map[string]any{
		"key1": "overwritten",
	})

	val, _ := ctx.GetInput("key1")
	if val != "overwritten" {
		t.Errorf("expected overwritten, got %v", val)
	}
}

func TestWithInput_NilMapInitialization(t *testing.T) {
	ctx := &TxContext{
		TxID:   "test-tx",
		TxType: "test",
		Input:  nil,
	}

	ctx.WithInput(map[string]any{
		"key1": "value1",
	})

	if ctx.Input == nil {
		t.Error("Input map should be initialized")
	}

	val, ok := ctx.GetInput("key1")
	if !ok || val != "value1" {
		t.Error("key1 should be set")
	}
}

func TestWithInput_EmptyMap(t *testing.T) {
	ctx := NewTxContext("test-tx", "test")
	ctx.SetInput("existing", "value")

	ctx.WithInput(map[string]any{})

	// Existing key should still be there
	val, ok := ctx.GetInput("existing")
	if !ok || val != "value" {
		t.Error("existing key should be preserved with empty input")
	}
}

func TestProperty_ContextCloneIndependence(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate random context data
		txID := rapid.StringMatching(`^tx-[a-z0-9]{8}$`).Draw(rt, "txID")
		txType := rapid.StringMatching(`^type-[a-z]{4}$`).Draw(rt, "txType")
		stepIndex := rapid.IntRange(0, 100).Draw(rt, "stepIndex")

		// Generate random input map
		numInputs := rapid.IntRange(0, 10).Draw(rt, "numInputs")
		inputKeys := make([]string, numInputs)
		inputValues := make([]string, numInputs)
		for i := 0; i < numInputs; i++ {
			inputKeys[i] = rapid.StringMatching(`^key-[a-z0-9]{4}$`).Draw(rt, "inputKey")
			inputValues[i] = rapid.StringMatching(`^val-[a-z0-9]{8}$`).Draw(rt, "inputValue")
		}

		// Generate random output map
		numOutputs := rapid.IntRange(0, 10).Draw(rt, "numOutputs")
		outputKeys := make([]string, numOutputs)
		outputValues := make([]string, numOutputs)
		for i := 0; i < numOutputs; i++ {
			outputKeys[i] = rapid.StringMatching(`^out-[a-z0-9]{4}$`).Draw(rt, "outputKey")
			outputValues[i] = rapid.StringMatching(`^res-[a-z0-9]{8}$`).Draw(rt, "outputValue")
		}

		// Generate random metadata map
		numMeta := rapid.IntRange(0, 10).Draw(rt, "numMeta")
		metaKeys := make([]string, numMeta)
		metaValues := make([]string, numMeta)
		for i := 0; i < numMeta; i++ {
			metaKeys[i] = rapid.StringMatching(`^meta-[a-z0-9]{4}$`).Draw(rt, "metaKey")
			metaValues[i] = rapid.StringMatching(`^data-[a-z0-9]{8}$`).Draw(rt, "metaValue")
		}

		// Create original context
		original := NewTxContext(txID, txType)
		original.StepIndex = stepIndex

		for i := 0; i < numInputs; i++ {
			original.SetInput(inputKeys[i], inputValues[i])
		}
		for i := 0; i < numOutputs; i++ {
			original.SetOutput(outputKeys[i], outputValues[i])
		}
		for i := 0; i < numMeta; i++ {
			original.SetMetadata(metaKeys[i], metaValues[i])
		}

		// Clone the context
		clone := original.Clone()

		if clone.TxID != original.TxID {
			rt.Fatalf("TxID mismatch after clone")
		}
		if clone.TxType != original.TxType {
			rt.Fatalf("TxType mismatch after clone")
		}
		if clone.StepIndex != original.StepIndex {
			rt.Fatalf("StepIndex mismatch after clone")
		}

		// Verify all input values match
		for i := 0; i < numInputs; i++ {
			origVal, _ := original.GetInput(inputKeys[i])
			cloneVal, _ := clone.GetInput(inputKeys[i])
			if origVal != cloneVal {
				rt.Fatalf("Input value mismatch for key %s", inputKeys[i])
			}
		}

		// Verify all output values match
		for i := 0; i < numOutputs; i++ {
			origVal, _ := original.GetOutput(outputKeys[i])
			cloneVal, _ := clone.GetOutput(outputKeys[i])
			if origVal != cloneVal {
				rt.Fatalf("Output value mismatch for key %s", outputKeys[i])
			}
		}

		// Verify all metadata values match
		for i := 0; i < numMeta; i++ {
			origVal, _ := original.GetMetadata(metaKeys[i])
			cloneVal, _ := clone.GetMetadata(metaKeys[i])
			if origVal != cloneVal {
				rt.Fatalf("Metadata value mismatch for key %s", metaKeys[i])
			}
		}

		// Modify clone
		clone.TxID = "modified-tx-id"
		clone.TxType = "modified-type"
		clone.StepIndex = 999

		// Add new values to clone
		clone.SetInput("clone-only-input", "clone-value")
		clone.SetOutput("clone-only-output", "clone-result")
		clone.SetMetadata("clone-only-meta", "clone-data")

		// Modify existing values in clone (if any exist)
		if numInputs > 0 {
			clone.SetInput(inputKeys[0], "modified-input-value")
		}
		if numOutputs > 0 {
			clone.SetOutput(outputKeys[0], "modified-output-value")
		}
		if numMeta > 0 {
			clone.SetMetadata(metaKeys[0], "modified-meta-value")
		}

		// Verify original is unchanged
		if original.TxID != txID {
			rt.Fatalf("Original TxID was modified")
		}
		if original.TxType != txType {
			rt.Fatalf("Original TxType was modified")
		}
		if original.StepIndex != stepIndex {
			rt.Fatalf("Original StepIndex was modified")
		}

		// Verify original input values unchanged
		for i := 0; i < numInputs; i++ {
			val, _ := original.GetInput(inputKeys[i])
			if val != inputValues[i] {
				rt.Fatalf("Original Input[%s] was modified", inputKeys[i])
			}
		}

		// Verify original output values unchanged
		for i := 0; i < numOutputs; i++ {
			val, _ := original.GetOutput(outputKeys[i])
			if val != outputValues[i] {
				rt.Fatalf("Original Output[%s] was modified", outputKeys[i])
			}
		}

		// Verify original metadata values unchanged
		for i := 0; i < numMeta; i++ {
			val, _ := original.GetMetadata(metaKeys[i])
			if val != metaValues[i] {
				rt.Fatalf("Original Metadata[%s] was modified", metaKeys[i])
			}
		}

		// Verify clone-only keys don't exist in original
		_, ok := original.GetInput("clone-only-input")
		if ok {
			rt.Fatalf("Clone-only input key should not exist in original")
		}
		_, ok = original.GetOutput("clone-only-output")
		if ok {
			rt.Fatalf("Clone-only output key should not exist in original")
		}
		_, ok = original.GetMetadata("clone-only-meta")
		if ok {
			rt.Fatalf("Clone-only metadata key should not exist in original")
		}
	})
}

// ============================================================================
// Additional tests for context.go coverage improvement
// ============================================================================

func TestSetOutput_NilMapInitialization(t *testing.T) {
	// Create context with nil Output map
	ctx := &TxContext{
		TxID:   "test-tx",
		TxType: "test",
		Output: nil, // Explicitly nil
	}

	// SetOutput should initialize the map
	ctx.SetOutput("key1", "value1")

	// Verify the map was initialized and value was set
	if ctx.Output == nil {
		t.Fatal("Output map should be initialized after SetOutput")
	}

	val, ok := ctx.GetOutput("key1")
	if !ok {
		t.Fatal("key1 should exist after SetOutput")
	}
	if val != "value1" {
		t.Errorf("expected value1, got %v", val)
	}
}

func TestSetMetadata_NilMapInitialization(t *testing.T) {
	// Create context with nil Metadata map
	ctx := &TxContext{
		TxID:     "test-tx",
		TxType:   "test",
		Metadata: nil, // Explicitly nil
	}

	// SetMetadata should initialize the map
	ctx.SetMetadata("key1", "value1")

	// Verify the map was initialized and value was set
	if ctx.Metadata == nil {
		t.Fatal("Metadata map should be initialized after SetMetadata")
	}

	val, ok := ctx.GetMetadata("key1")
	if !ok {
		t.Fatal("key1 should exist after SetMetadata")
	}
	if val != "value1" {
		t.Errorf("expected value1, got %v", val)
	}
}

func TestGetInputAs_TypeMismatch(t *testing.T) {
	ctx := NewTxContext("test-tx", "test")
	ctx.SetInput("number", 42)

	// Try to get as string (should fail)
	_, err := GetInputAs[string](ctx, "number")
	if err == nil {
		t.Error("expected type mismatch error")
	}
}

func TestGetOutputAs_TypeMismatch(t *testing.T) {
	ctx := NewTxContext("test-tx", "test")
	ctx.SetOutput("number", 42)

	// Try to get as string (should fail)
	_, err := GetOutputAs[string](ctx, "number")
	if err == nil {
		t.Error("expected type mismatch error")
	}
}

func TestGetOutputAs_KeyNotFound(t *testing.T) {
	ctx := NewTxContext("test-tx", "test")

	// Try to get non-existent key
	_, err := GetOutputAs[string](ctx, "nonexistent")
	if err == nil {
		t.Error("expected key not found error")
	}
}
