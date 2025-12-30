package rte

import (
	"context"
	"errors"
	"testing"
	"time"
)

// mockStepRegistry is a mock implementation of StepRegistry for testing
type mockStepRegistry struct {
	steps map[string]Step
}

func newMockStepRegistry() *mockStepRegistry {
	return &mockStepRegistry{
		steps: make(map[string]Step),
	}
}

func (r *mockStepRegistry) RegisterStep(step Step) {
	r.steps[step.Name()] = step
}

func (r *mockStepRegistry) GetStep(name string) Step {
	return r.steps[name]
}

func (r *mockStepRegistry) HasStep(name string) bool {
	_, ok := r.steps[name]
	return ok
}

// TestNewTransaction tests the basic transaction creation
func TestNewTransaction(t *testing.T) {
	builder := NewTransaction("test_type")

	if builder == nil {
		t.Fatal("expected non-nil builder")
	}

	if builder.tx.txType != "test_type" {
		t.Errorf("expected txType 'test_type', got '%s'", builder.tx.txType)
	}

	if builder.tx.txID == "" {
		t.Error("expected non-empty txID")
	}

	if builder.tx.maxRetries != 3 {
		t.Errorf("expected default maxRetries 3, got %d", builder.tx.maxRetries)
	}
}

// TestNewTransactionWithID tests transaction creation with specific ID
func TestNewTransactionWithID(t *testing.T) {
	builder := NewTransactionWithID("custom-tx-id", "test_type")

	if builder.tx.txID != "custom-tx-id" {
		t.Errorf("expected txID 'custom-tx-id', got '%s'", builder.tx.txID)
	}

	if builder.tx.txType != "test_type" {
		t.Errorf("expected txType 'test_type', got '%s'", builder.tx.txType)
	}
}

// TestTransactionBuilder_ChainedAPI tests the fluent chain-style API
func TestTransactionBuilder_ChainedAPI(t *testing.T) {
	tx, err := NewTransaction("saving_to_forex").
		WithLockKeys("account:saving:123", "account:forex:456").
		WithInput(map[string]any{
			"user_id": 123,
			"amount":  "100.00",
		}).
		WithInputValue("currency", "USD").
		WithMetadata(map[string]string{
			"source": "api",
		}).
		WithMetadataValue("trace_id", "trace-123").
		WithTimeout(3 * time.Minute).
		WithMaxRetries(5).
		AddStep("step1").
		AddStep("step2").
		AddStep("step3").
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify all configurations
	if tx.TxType() != "saving_to_forex" {
		t.Errorf("expected txType 'saving_to_forex', got '%s'", tx.TxType())
	}

	lockKeys := tx.LockKeys()
	if len(lockKeys) != 2 {
		t.Errorf("expected 2 lock keys, got %d", len(lockKeys))
	}
	if lockKeys[0] != "account:saving:123" || lockKeys[1] != "account:forex:456" {
		t.Errorf("unexpected lock keys: %v", lockKeys)
	}

	input := tx.Input()
	if input["user_id"] != 123 {
		t.Errorf("expected user_id 123, got %v", input["user_id"])
	}
	if input["amount"] != "100.00" {
		t.Errorf("expected amount '100.00', got %v", input["amount"])
	}
	if input["currency"] != "USD" {
		t.Errorf("expected currency 'USD', got %v", input["currency"])
	}

	metadata := tx.Metadata()
	if metadata["source"] != "api" {
		t.Errorf("expected source 'api', got '%s'", metadata["source"])
	}
	if metadata["trace_id"] != "trace-123" {
		t.Errorf("expected trace_id 'trace-123', got '%s'", metadata["trace_id"])
	}

	if tx.Timeout() != 3*time.Minute {
		t.Errorf("expected timeout 3m, got %v", tx.Timeout())
	}

	if tx.MaxRetries() != 5 {
		t.Errorf("expected maxRetries 5, got %d", tx.MaxRetries())
	}

	stepNames := tx.StepNames()
	if len(stepNames) != 3 {
		t.Errorf("expected 3 steps, got %d", len(stepNames))
	}
	if stepNames[0] != "step1" || stepNames[1] != "step2" || stepNames[2] != "step3" {
		t.Errorf("unexpected step names: %v", stepNames)
	}

	if tx.TotalSteps() != 3 {
		t.Errorf("expected TotalSteps 3, got %d", tx.TotalSteps())
	}
}

// TestTransactionBuilder_AddSteps tests adding multiple steps at once
func TestTransactionBuilder_AddSteps(t *testing.T) {
	tx, err := NewTransaction("test_type").
		AddSteps("step1", "step2", "step3").
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stepNames := tx.StepNames()
	if len(stepNames) != 3 {
		t.Errorf("expected 3 steps, got %d", len(stepNames))
	}
}

// TestTransactionBuilder_WithStepRegistry_ValidSteps tests step validation with registry
func TestTransactionBuilder_WithStepRegistry_ValidSteps(t *testing.T) {
	registry := newMockStepRegistry()
	registry.RegisterStep(NewBaseStep("local_debit"))
	registry.RegisterStep(NewBaseStep("platform_b_deposit"))
	registry.RegisterStep(NewBaseStep("local_credit"))

	tx, err := NewTransaction("saving_to_forex").
		WithStepRegistry(registry).
		AddStep("local_debit").
		AddStep("platform_b_deposit").
		AddStep("local_credit").
		Build()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tx.TotalSteps() != 3 {
		t.Errorf("expected 3 steps, got %d", tx.TotalSteps())
	}
}

// TestTransactionBuilder_WithStepRegistry_InvalidStep tests step validation failure
func TestTransactionBuilder_WithStepRegistry_InvalidStep(t *testing.T) {
	registry := newMockStepRegistry()
	registry.RegisterStep(NewBaseStep("local_debit"))

	_, err := NewTransaction("saving_to_forex").
		WithStepRegistry(registry).
		AddStep("local_debit").
		AddStep("unregistered_step").
		Build()

	if err == nil {
		t.Fatal("expected error for unregistered step")
	}

	if !errors.Is(err, ErrStepNotRegistered) {
		t.Errorf("expected ErrStepNotRegistered, got %v", err)
	}
}

// TestTransactionBuilder_EmptyStepName tests validation of empty step name
func TestTransactionBuilder_EmptyStepName(t *testing.T) {
	_, err := NewTransaction("test_type").
		AddStep("").
		Build()

	if err == nil {
		t.Fatal("expected error for empty step name")
	}

	if !errors.Is(err, ErrStepNotRegistered) {
		t.Errorf("expected ErrStepNotRegistered, got %v", err)
	}
}

// TestTransactionBuilder_NoSteps tests validation when no steps are added
func TestTransactionBuilder_NoSteps(t *testing.T) {
	_, err := NewTransaction("test_type").Build()

	if err == nil {
		t.Fatal("expected error when no steps are added")
	}

	expectedMsg := "transaction must have at least one step"
	if err.Error() != expectedMsg {
		t.Errorf("expected error '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestTransactionBuilder_EmptyTxType tests validation of empty transaction type
func TestTransactionBuilder_EmptyTxType(t *testing.T) {
	_, err := NewTransaction("").
		AddStep("step1").
		Build()

	if err == nil {
		t.Fatal("expected error for empty transaction type")
	}

	expectedMsg := "transaction type cannot be empty"
	if err.Error() != expectedMsg {
		t.Errorf("expected error '%s', got '%s'", expectedMsg, err.Error())
	}
}

// TestTransactionBuilder_MustBuild tests MustBuild with valid transaction
func TestTransactionBuilder_MustBuild_Success(t *testing.T) {
	tx := NewTransaction("test_type").
		AddStep("step1").
		MustBuild()

	if tx == nil {
		t.Fatal("expected non-nil transaction")
	}

	if tx.TxType() != "test_type" {
		t.Errorf("expected txType 'test_type', got '%s'", tx.TxType())
	}
}

// TestTransactionBuilder_MustBuild_Panic tests MustBuild panics on error
func TestTransactionBuilder_MustBuild_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for invalid transaction")
		}
	}()

	NewTransaction("test_type").MustBuild() // No steps - should panic
}

// TestTransaction_ToContext tests conversion to TxContext
func TestTransaction_ToContext(t *testing.T) {
	tx, _ := NewTransaction("test_type").
		WithInput(map[string]any{
			"user_id": 123,
			"amount":  "100.00",
		}).
		WithMetadata(map[string]string{
			"source": "api",
		}).
		AddStep("step1").
		Build()

	ctx := tx.ToContext()

	if ctx.TxID != tx.TxID() {
		t.Errorf("expected TxID '%s', got '%s'", tx.TxID(), ctx.TxID)
	}

	if ctx.TxType != tx.TxType() {
		t.Errorf("expected TxType '%s', got '%s'", tx.TxType(), ctx.TxType)
	}

	userID, ok := ctx.GetInput("user_id")
	if !ok || userID != 123 {
		t.Errorf("expected user_id 123, got %v", userID)
	}

	source, ok := ctx.GetMetadata("source")
	if !ok || source != "api" {
		t.Errorf("expected source 'api', got '%s'", source)
	}
}

// TestTransaction_Validate tests the Validate method
func TestTransaction_Validate(t *testing.T) {
	registry := newMockStepRegistry()
	registry.RegisterStep(NewBaseStep("step1"))
	registry.RegisterStep(NewBaseStep("step2"))

	tx, _ := NewTransaction("test_type").
		AddStep("step1").
		AddStep("step2").
		Build()

	// Validate with registry - should pass
	err := tx.Validate(registry)
	if err != nil {
		t.Errorf("unexpected validation error: %v", err)
	}

	// Validate with registry missing a step
	registry2 := newMockStepRegistry()
	registry2.RegisterStep(NewBaseStep("step1"))

	err = tx.Validate(registry2)
	if err == nil {
		t.Error("expected validation error for missing step")
	}
	if !errors.Is(err, ErrStepNotRegistered) {
		t.Errorf("expected ErrStepNotRegistered, got %v", err)
	}
}

// TestTransaction_Validate_NilRegistry tests validation with nil registry
func TestTransaction_Validate_NilRegistry(t *testing.T) {
	tx, _ := NewTransaction("test_type").
		AddStep("step1").
		Build()

	// Validate with nil registry - should pass (no step validation)
	err := tx.Validate(nil)
	if err != nil {
		t.Errorf("unexpected validation error with nil registry: %v", err)
	}
}

// TestTransaction_GettersReturnCopies tests that getters return copies
func TestTransaction_GettersReturnCopies(t *testing.T) {
	tx, _ := NewTransaction("test_type").
		WithLockKeys("key1", "key2").
		WithInput(map[string]any{"a": 1}).
		WithMetadata(map[string]string{"b": "2"}).
		AddStep("step1").
		Build()

	// Modify returned slices/maps
	lockKeys := tx.LockKeys()
	lockKeys[0] = "modified"

	input := tx.Input()
	input["a"] = 999

	metadata := tx.Metadata()
	metadata["b"] = "modified"

	stepNames := tx.StepNames()
	stepNames[0] = "modified"

	// Verify original values are unchanged
	if tx.LockKeys()[0] != "key1" {
		t.Error("LockKeys should return a copy")
	}

	if tx.Input()["a"] != 1 {
		t.Error("Input should return a copy")
	}

	if tx.Metadata()["b"] != "2" {
		t.Error("Metadata should return a copy")
	}

	if tx.StepNames()[0] != "step1" {
		t.Error("StepNames should return a copy")
	}
}

// TestTransactionBuilder_ImplementsStepRegistry verifies mockStepRegistry implements StepRegistry
func TestMockStepRegistry_ImplementsStepRegistry(t *testing.T) {
	var _ StepRegistry = (*mockStepRegistry)(nil)
}

// mockStep is a simple Step implementation for testing
type mockStep struct {
	*BaseStep
	executeFunc    func(ctx context.Context, txCtx *TxContext) error
	compensateFunc func(ctx context.Context, txCtx *TxContext) error
}

func newMockStep(name string) *mockStep {
	return &mockStep{
		BaseStep: NewBaseStep(name),
	}
}

func (s *mockStep) Execute(ctx context.Context, txCtx *TxContext) error {
	if s.executeFunc != nil {
		return s.executeFunc(ctx, txCtx)
	}
	return nil
}

func (s *mockStep) Compensate(ctx context.Context, txCtx *TxContext) error {
	if s.compensateFunc != nil {
		return s.compensateFunc(ctx, txCtx)
	}
	return ErrCompensationNotSupported
}
