package examples

import (
	"testing"
)

// TestSavingToForexTransferExample tests the saving-to-forex transfer example
func TestSavingToForexTransferExample(t *testing.T) {
	err := RunSavingToForexTransferExample()
	if err != nil {
		t.Fatalf("RunSavingToForexTransferExample failed: %v", err)
	}
}

// TestErrorHandlingExample tests the error handling example
func TestErrorHandlingExample(t *testing.T) {
	err := RunErrorHandlingExample()
	if err != nil {
		t.Fatalf("RunErrorHandlingExample failed: %v", err)
	}
}
