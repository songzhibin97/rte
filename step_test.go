package rte

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewBaseStep(t *testing.T) {
	step := NewBaseStep("test_step")

	if step.Name() != "test_step" {
		t.Errorf("expected name 'test_step', got '%s'", step.Name())
	}

	if step.Config() != nil {
		t.Error("expected nil config for NewBaseStep")
	}
}

func TestNewBaseStepWithConfig(t *testing.T) {
	config := &StepConfig{
		Timeout:       5 * time.Second,
		MaxRetries:    3,
		RetryInterval: 1 * time.Second,
	}
	step := NewBaseStepWithConfig("configured_step", config)

	if step.Name() != "configured_step" {
		t.Errorf("expected name 'configured_step', got '%s'", step.Name())
	}

	if step.Config() != config {
		t.Error("expected config to match provided config")
	}

	if step.Config().Timeout != 5*time.Second {
		t.Errorf("expected timeout 5s, got %v", step.Config().Timeout)
	}
}

func TestBaseStep_SupportsCompensation(t *testing.T) {
	step := NewBaseStep("test_step")

	if step.SupportsCompensation() {
		t.Error("BaseStep should not support compensation by default")
	}
}

func TestBaseStep_SupportsIdempotency(t *testing.T) {
	step := NewBaseStep("test_step")

	if step.SupportsIdempotency() {
		t.Error("BaseStep should not support idempotency by default")
	}
}

func TestBaseStep_IdempotencyKey(t *testing.T) {
	step := NewBaseStep("test_step")
	txCtx := NewTxContext("tx-123", "test_type")

	key := step.IdempotencyKey(txCtx)
	if key != "" {
		t.Errorf("expected empty idempotency key, got '%s'", key)
	}
}

func TestBaseStep_Compensate(t *testing.T) {
	step := NewBaseStep("test_step")
	txCtx := NewTxContext("tx-123", "test_type")

	err := step.Compensate(context.Background(), txCtx)
	if !errors.Is(err, ErrCompensationNotSupported) {
		t.Errorf("expected ErrCompensationNotSupported, got %v", err)
	}
}

func TestBaseStep_Execute(t *testing.T) {
	step := NewBaseStep("test_step")
	txCtx := NewTxContext("tx-123", "test_type")

	err := step.Execute(context.Background(), txCtx)
	if err != nil {
		t.Errorf("expected nil error from default Execute, got %v", err)
	}
}

func TestBaseStep_SetConfig(t *testing.T) {
	step := NewBaseStep("test_step")

	if step.Config() != nil {
		t.Error("expected nil config initially")
	}

	config := &StepConfig{
		Timeout:    10 * time.Second,
		MaxRetries: 5,
	}
	step.SetConfig(config)

	if step.Config() != config {
		t.Error("expected config to be set")
	}

	if step.Config().MaxRetries != 5 {
		t.Errorf("expected MaxRetries 5, got %d", step.Config().MaxRetries)
	}
}

// TestBaseStep_ImplementsStepInterface verifies that BaseStep implements the Step interface
func TestBaseStep_ImplementsStepInterface(t *testing.T) {
	var _ Step = (*BaseStep)(nil)
}
