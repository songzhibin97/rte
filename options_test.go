package rte

import (
	"errors"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// ============================================================================
// Unit Tests for options.go
// Tests all With* option functions, ApplyOptions, ToBreakerConfig, and Validate
// ============================================================================

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Verify default values
	if cfg.LockTTL != 30*time.Second {
		t.Errorf("LockTTL: expected 30s, got %v", cfg.LockTTL)
	}
	if cfg.LockExtendPeriod != 10*time.Second {
		t.Errorf("LockExtendPeriod: expected 10s, got %v", cfg.LockExtendPeriod)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("MaxRetries: expected 3, got %d", cfg.MaxRetries)
	}
	if cfg.RetryInterval != 1*time.Second {
		t.Errorf("RetryInterval: expected 1s, got %v", cfg.RetryInterval)
	}
	if cfg.RetryMaxInterval != 60*time.Second {
		t.Errorf("RetryMaxInterval: expected 60s, got %v", cfg.RetryMaxInterval)
	}
	if cfg.RetryMultiplier != 2.0 {
		t.Errorf("RetryMultiplier: expected 2.0, got %f", cfg.RetryMultiplier)
	}
	if cfg.RetryJitter != 0.1 {
		t.Errorf("RetryJitter: expected 0.1, got %f", cfg.RetryJitter)
	}
	if cfg.CircuitThreshold != 5 {
		t.Errorf("CircuitThreshold: expected 5, got %d", cfg.CircuitThreshold)
	}
	if cfg.CircuitTimeout != 30*time.Second {
		t.Errorf("CircuitTimeout: expected 30s, got %v", cfg.CircuitTimeout)
	}
	if cfg.CircuitHalfOpenReqs != 3 {
		t.Errorf("CircuitHalfOpenReqs: expected 3, got %d", cfg.CircuitHalfOpenReqs)
	}
	if cfg.RecoveryInterval != 30*time.Second {
		t.Errorf("RecoveryInterval: expected 30s, got %v", cfg.RecoveryInterval)
	}
	if cfg.StuckThreshold != 5*time.Minute {
		t.Errorf("StuckThreshold: expected 5m, got %v", cfg.StuckThreshold)
	}
	if cfg.StepTimeout != 10*time.Second {
		t.Errorf("StepTimeout: expected 10s, got %v", cfg.StepTimeout)
	}
	if cfg.TxTimeout != 5*time.Minute {
		t.Errorf("TxTimeout: expected 5m, got %v", cfg.TxTimeout)
	}
	if cfg.IdempotencyTTL != 24*time.Hour {
		t.Errorf("IdempotencyTTL: expected 24h, got %v", cfg.IdempotencyTTL)
	}
}

func TestWithLockTTL(t *testing.T) {
	cfg := ApplyOptions(WithLockTTL(60 * time.Second))
	if cfg.LockTTL != 60*time.Second {
		t.Errorf("expected 60s, got %v", cfg.LockTTL)
	}
}

func TestWithLockExtendPeriod(t *testing.T) {
	cfg := ApplyOptions(WithLockExtendPeriod(20 * time.Second))
	if cfg.LockExtendPeriod != 20*time.Second {
		t.Errorf("expected 20s, got %v", cfg.LockExtendPeriod)
	}
}

func TestWithMaxRetries(t *testing.T) {
	cfg := ApplyOptions(WithMaxRetries(5))
	if cfg.MaxRetries != 5 {
		t.Errorf("expected 5, got %d", cfg.MaxRetries)
	}
}

func TestWithRetryInterval(t *testing.T) {
	cfg := ApplyOptions(WithRetryInterval(2 * time.Second))
	if cfg.RetryInterval != 2*time.Second {
		t.Errorf("expected 2s, got %v", cfg.RetryInterval)
	}
}

func TestWithRetryMaxInterval(t *testing.T) {
	cfg := ApplyOptions(WithRetryMaxInterval(120 * time.Second))
	if cfg.RetryMaxInterval != 120*time.Second {
		t.Errorf("expected 120s, got %v", cfg.RetryMaxInterval)
	}
}

func TestWithRetryMultiplier(t *testing.T) {
	cfg := ApplyOptions(WithRetryMultiplier(3.0))
	if cfg.RetryMultiplier != 3.0 {
		t.Errorf("expected 3.0, got %f", cfg.RetryMultiplier)
	}
}

func TestWithRetryJitter(t *testing.T) {
	cfg := ApplyOptions(WithRetryJitter(0.5))
	if cfg.RetryJitter != 0.5 {
		t.Errorf("expected 0.5, got %f", cfg.RetryJitter)
	}
}

func TestWithCircuitThreshold(t *testing.T) {
	cfg := ApplyOptions(WithCircuitThreshold(10))
	if cfg.CircuitThreshold != 10 {
		t.Errorf("expected 10, got %d", cfg.CircuitThreshold)
	}
}

func TestWithCircuitTimeout(t *testing.T) {
	cfg := ApplyOptions(WithCircuitTimeout(60 * time.Second))
	if cfg.CircuitTimeout != 60*time.Second {
		t.Errorf("expected 60s, got %v", cfg.CircuitTimeout)
	}
}

func TestWithCircuitHalfOpenReqs(t *testing.T) {
	cfg := ApplyOptions(WithCircuitHalfOpenReqs(5))
	if cfg.CircuitHalfOpenReqs != 5 {
		t.Errorf("expected 5, got %d", cfg.CircuitHalfOpenReqs)
	}
}

func TestWithRecoveryInterval(t *testing.T) {
	cfg := ApplyOptions(WithRecoveryInterval(60 * time.Second))
	if cfg.RecoveryInterval != 60*time.Second {
		t.Errorf("expected 60s, got %v", cfg.RecoveryInterval)
	}
}

func TestWithStuckThreshold(t *testing.T) {
	cfg := ApplyOptions(WithStuckThreshold(10 * time.Minute))
	if cfg.StuckThreshold != 10*time.Minute {
		t.Errorf("expected 10m, got %v", cfg.StuckThreshold)
	}
}

func TestWithStepTimeout(t *testing.T) {
	cfg := ApplyOptions(WithStepTimeout(30 * time.Second))
	if cfg.StepTimeout != 30*time.Second {
		t.Errorf("expected 30s, got %v", cfg.StepTimeout)
	}
}

func TestWithTxTimeout(t *testing.T) {
	cfg := ApplyOptions(WithTxTimeout(10 * time.Minute))
	if cfg.TxTimeout != 10*time.Minute {
		t.Errorf("expected 10m, got %v", cfg.TxTimeout)
	}
}

func TestWithIdempotencyTTL(t *testing.T) {
	cfg := ApplyOptions(WithIdempotencyTTL(48 * time.Hour))
	if cfg.IdempotencyTTL != 48*time.Hour {
		t.Errorf("expected 48h, got %v", cfg.IdempotencyTTL)
	}
}

func TestWithConfig(t *testing.T) {
	customCfg := Config{
		LockTTL:             100 * time.Second,
		LockExtendPeriod:    50 * time.Second,
		MaxRetries:          10,
		RetryInterval:       5 * time.Second,
		RetryMaxInterval:    120 * time.Second,
		RetryMultiplier:     3.0,
		RetryJitter:         0.2,
		CircuitThreshold:    10,
		CircuitTimeout:      60 * time.Second,
		CircuitHalfOpenReqs: 5,
		RecoveryInterval:    60 * time.Second,
		StuckThreshold:      10 * time.Minute,
		StepTimeout:         30 * time.Second,
		TxTimeout:           10 * time.Minute,
		IdempotencyTTL:      48 * time.Hour,
	}

	cfg := ApplyOptions(WithConfig(customCfg))

	if cfg != customCfg {
		t.Error("WithConfig should override all values")
	}
}

func TestApplyOptions_MultipleOptions(t *testing.T) {
	cfg := ApplyOptions(
		WithLockTTL(60*time.Second),
		WithMaxRetries(5),
		WithCircuitThreshold(10),
	)

	// Verify applied options
	if cfg.LockTTL != 60*time.Second {
		t.Errorf("LockTTL: expected 60s, got %v", cfg.LockTTL)
	}
	if cfg.MaxRetries != 5 {
		t.Errorf("MaxRetries: expected 5, got %d", cfg.MaxRetries)
	}
	if cfg.CircuitThreshold != 10 {
		t.Errorf("CircuitThreshold: expected 10, got %d", cfg.CircuitThreshold)
	}

	// Verify defaults are preserved for non-applied options
	if cfg.LockExtendPeriod != 10*time.Second {
		t.Errorf("LockExtendPeriod should be default 10s, got %v", cfg.LockExtendPeriod)
	}
}

func TestApplyOptions_NoOptions(t *testing.T) {
	cfg := ApplyOptions()
	defaultCfg := DefaultConfig()

	if cfg != defaultCfg {
		t.Error("ApplyOptions with no options should return default config")
	}
}

func TestApplyOptions_OverwriteOrder(t *testing.T) {
	// Later options should overwrite earlier ones
	cfg := ApplyOptions(
		WithMaxRetries(5),
		WithMaxRetries(10),
	)

	if cfg.MaxRetries != 10 {
		t.Errorf("expected 10 (last applied), got %d", cfg.MaxRetries)
	}
}

func TestToBreakerConfig(t *testing.T) {
	cfg := Config{
		CircuitThreshold:    10,
		CircuitTimeout:      60 * time.Second,
		CircuitHalfOpenReqs: 5,
	}

	breakerCfg := cfg.ToBreakerConfig()

	if breakerCfg.Threshold != 10 {
		t.Errorf("Threshold: expected 10, got %d", breakerCfg.Threshold)
	}
	if breakerCfg.Timeout != 60*time.Second {
		t.Errorf("Timeout: expected 60s, got %v", breakerCfg.Timeout)
	}
	if breakerCfg.HalfOpenMaxReqs != 5 {
		t.Errorf("HalfOpenMaxReqs: expected 5, got %d", breakerCfg.HalfOpenMaxReqs)
	}
}

func TestValidate_ValidConfig(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("default config should be valid, got error: %v", err)
	}
}

func TestValidate_InvalidLockTTL(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LockTTL = 0
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for zero LockTTL, got %v", err)
	}

	cfg.LockTTL = -1 * time.Second
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for negative LockTTL, got %v", err)
	}
}

func TestValidate_InvalidLockExtendPeriod(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LockExtendPeriod = 0
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for zero LockExtendPeriod, got %v", err)
	}

	cfg = DefaultConfig()
	cfg.LockExtendPeriod = cfg.LockTTL // Equal to LockTTL
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig when LockExtendPeriod >= LockTTL, got %v", err)
	}

	cfg = DefaultConfig()
	cfg.LockExtendPeriod = cfg.LockTTL + time.Second // Greater than LockTTL
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig when LockExtendPeriod > LockTTL, got %v", err)
	}
}

func TestValidate_InvalidMaxRetries(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetries = -1
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for negative MaxRetries, got %v", err)
	}

	// Zero is valid
	cfg.MaxRetries = 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("zero MaxRetries should be valid, got %v", err)
	}
}

func TestValidate_InvalidRetryInterval(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RetryInterval = -1 * time.Second
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for negative RetryInterval, got %v", err)
	}

	// Zero is valid
	cfg.RetryInterval = 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("zero RetryInterval should be valid, got %v", err)
	}
}

func TestValidate_InvalidRetryMaxInterval(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RetryMaxInterval = -1 * time.Second
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for negative RetryMaxInterval, got %v", err)
	}
}

func TestValidate_InvalidRetryMultiplier(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RetryMultiplier = 0.5 // Less than 1.0
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for RetryMultiplier < 1.0, got %v", err)
	}

	// Exactly 1.0 is valid
	cfg.RetryMultiplier = 1.0
	if err := cfg.Validate(); err != nil {
		t.Errorf("RetryMultiplier 1.0 should be valid, got %v", err)
	}
}

func TestValidate_InvalidRetryJitter(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RetryJitter = -0.1
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for negative RetryJitter, got %v", err)
	}

	cfg.RetryJitter = 1.1 // Greater than 1.0
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for RetryJitter > 1.0, got %v", err)
	}

	// Boundary values are valid
	cfg.RetryJitter = 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("RetryJitter 0 should be valid, got %v", err)
	}

	cfg.RetryJitter = 1.0
	if err := cfg.Validate(); err != nil {
		t.Errorf("RetryJitter 1.0 should be valid, got %v", err)
	}
}

func TestValidate_InvalidCircuitThreshold(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CircuitThreshold = 0
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for zero CircuitThreshold, got %v", err)
	}

	cfg.CircuitThreshold = -1
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for negative CircuitThreshold, got %v", err)
	}
}

func TestValidate_InvalidCircuitTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CircuitTimeout = 0
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for zero CircuitTimeout, got %v", err)
	}
}

func TestValidate_InvalidCircuitHalfOpenReqs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CircuitHalfOpenReqs = 0
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for zero CircuitHalfOpenReqs, got %v", err)
	}
}

func TestValidate_InvalidRecoveryInterval(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RecoveryInterval = 0
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for zero RecoveryInterval, got %v", err)
	}
}

func TestValidate_InvalidStuckThreshold(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StuckThreshold = 0
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for zero StuckThreshold, got %v", err)
	}
}

func TestValidate_InvalidStepTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StepTimeout = 0
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for zero StepTimeout, got %v", err)
	}
}

func TestValidate_InvalidTxTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TxTimeout = 0
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for zero TxTimeout, got %v", err)
	}
}

func TestValidate_InvalidIdempotencyTTL(t *testing.T) {
	cfg := DefaultConfig()
	cfg.IdempotencyTTL = 0
	if err := cfg.Validate(); !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig for zero IdempotencyTTL, got %v", err)
	}
}

func TestProperty_OptionsApplicationCorrectness(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate random option values
		lockTTL := rapid.Int64Range(1, 300).Draw(rt, "lockTTL")
		maxRetries := rapid.IntRange(0, 10).Draw(rt, "maxRetries")
		circuitThreshold := rapid.IntRange(1, 20).Draw(rt, "circuitThreshold")
		retryMultiplier := rapid.Float64Range(1.0, 5.0).Draw(rt, "retryMultiplier")
		retryJitter := rapid.Float64Range(0.0, 1.0).Draw(rt, "retryJitter")

		// Randomly decide which options to apply
		applyLockTTL := rapid.Bool().Draw(rt, "applyLockTTL")
		applyMaxRetries := rapid.Bool().Draw(rt, "applyMaxRetries")
		applyCircuitThreshold := rapid.Bool().Draw(rt, "applyCircuitThreshold")
		applyRetryMultiplier := rapid.Bool().Draw(rt, "applyRetryMultiplier")
		applyRetryJitter := rapid.Bool().Draw(rt, "applyRetryJitter")

		// Build options list
		var opts []Option
		if applyLockTTL {
			opts = append(opts, WithLockTTL(time.Duration(lockTTL)*time.Second))
		}
		if applyMaxRetries {
			opts = append(opts, WithMaxRetries(maxRetries))
		}
		if applyCircuitThreshold {
			opts = append(opts, WithCircuitThreshold(circuitThreshold))
		}
		if applyRetryMultiplier {
			opts = append(opts, WithRetryMultiplier(retryMultiplier))
		}
		if applyRetryJitter {
			opts = append(opts, WithRetryJitter(retryJitter))
		}

		// Apply options
		cfg := ApplyOptions(opts...)
		defaultCfg := DefaultConfig()

		
		if applyLockTTL {
			expected := time.Duration(lockTTL) * time.Second
			if cfg.LockTTL != expected {
				rt.Fatalf("LockTTL: expected %v, got %v", expected, cfg.LockTTL)
			}
		} else {
			if cfg.LockTTL != defaultCfg.LockTTL {
				rt.Fatalf("LockTTL should be default %v, got %v", defaultCfg.LockTTL, cfg.LockTTL)
			}
		}

		if applyMaxRetries {
			if cfg.MaxRetries != maxRetries {
				rt.Fatalf("MaxRetries: expected %d, got %d", maxRetries, cfg.MaxRetries)
			}
		} else {
			if cfg.MaxRetries != defaultCfg.MaxRetries {
				rt.Fatalf("MaxRetries should be default %d, got %d", defaultCfg.MaxRetries, cfg.MaxRetries)
			}
		}

		if applyCircuitThreshold {
			if cfg.CircuitThreshold != circuitThreshold {
				rt.Fatalf("CircuitThreshold: expected %d, got %d", circuitThreshold, cfg.CircuitThreshold)
			}
		} else {
			if cfg.CircuitThreshold != defaultCfg.CircuitThreshold {
				rt.Fatalf("CircuitThreshold should be default %d, got %d", defaultCfg.CircuitThreshold, cfg.CircuitThreshold)
			}
		}

		if applyRetryMultiplier {
			if cfg.RetryMultiplier != retryMultiplier {
				rt.Fatalf("RetryMultiplier: expected %f, got %f", retryMultiplier, cfg.RetryMultiplier)
			}
		} else {
			if cfg.RetryMultiplier != defaultCfg.RetryMultiplier {
				rt.Fatalf("RetryMultiplier should be default %f, got %f", defaultCfg.RetryMultiplier, cfg.RetryMultiplier)
			}
		}

		if applyRetryJitter {
			if cfg.RetryJitter != retryJitter {
				rt.Fatalf("RetryJitter: expected %f, got %f", retryJitter, cfg.RetryJitter)
			}
		} else {
			if cfg.RetryJitter != defaultCfg.RetryJitter {
				rt.Fatalf("RetryJitter should be default %f, got %f", defaultCfg.RetryJitter, cfg.RetryJitter)
			}
		}

		
		// (Already checked above in the else branches)
	})
}

func TestProperty_ConfigValidationCompleteness(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate a valid config first
		lockTTL := rapid.Int64Range(10, 300).Draw(rt, "lockTTL")
		lockExtendPeriod := rapid.Int64Range(1, lockTTL-1).Draw(rt, "lockExtendPeriod")
		maxRetries := rapid.IntRange(0, 10).Draw(rt, "maxRetries")
		retryInterval := rapid.Int64Range(0, 60).Draw(rt, "retryInterval")
		retryMaxInterval := rapid.Int64Range(0, 300).Draw(rt, "retryMaxInterval")
		retryMultiplier := rapid.Float64Range(1.0, 5.0).Draw(rt, "retryMultiplier")
		retryJitter := rapid.Float64Range(0.0, 1.0).Draw(rt, "retryJitter")
		circuitThreshold := rapid.IntRange(1, 20).Draw(rt, "circuitThreshold")
		circuitTimeout := rapid.Int64Range(1, 120).Draw(rt, "circuitTimeout")
		circuitHalfOpenReqs := rapid.IntRange(1, 10).Draw(rt, "circuitHalfOpenReqs")
		recoveryInterval := rapid.Int64Range(1, 120).Draw(rt, "recoveryInterval")
		stuckThreshold := rapid.Int64Range(1, 600).Draw(rt, "stuckThreshold")
		stepTimeout := rapid.Int64Range(1, 120).Draw(rt, "stepTimeout")
		txTimeout := rapid.Int64Range(1, 600).Draw(rt, "txTimeout")
		idempotencyTTL := rapid.Int64Range(1, 86400).Draw(rt, "idempotencyTTL")

		cfg := Config{
			LockTTL:             time.Duration(lockTTL) * time.Second,
			LockExtendPeriod:    time.Duration(lockExtendPeriod) * time.Second,
			MaxRetries:          maxRetries,
			RetryInterval:       time.Duration(retryInterval) * time.Second,
			RetryMaxInterval:    time.Duration(retryMaxInterval) * time.Second,
			RetryMultiplier:     retryMultiplier,
			RetryJitter:         retryJitter,
			CircuitThreshold:    circuitThreshold,
			CircuitTimeout:      time.Duration(circuitTimeout) * time.Second,
			CircuitHalfOpenReqs: circuitHalfOpenReqs,
			RecoveryInterval:    time.Duration(recoveryInterval) * time.Second,
			StuckThreshold:      time.Duration(stuckThreshold) * time.Second,
			StepTimeout:         time.Duration(stepTimeout) * time.Second,
			TxTimeout:           time.Duration(txTimeout) * time.Second,
			IdempotencyTTL:      time.Duration(idempotencyTTL) * time.Second,
		}

		
		if err := cfg.Validate(); err != nil {
			rt.Fatalf("Valid config should pass validation, got error: %v", err)
		}

		
		// Test each invalid case

		// Invalid LockTTL
		invalidCfg := cfg
		invalidCfg.LockTTL = 0
		if err := invalidCfg.Validate(); err == nil {
			rt.Fatal("Config with zero LockTTL should fail validation")
		}

		// Invalid LockExtendPeriod (>= LockTTL)
		invalidCfg = cfg
		invalidCfg.LockExtendPeriod = cfg.LockTTL
		if err := invalidCfg.Validate(); err == nil {
			rt.Fatal("Config with LockExtendPeriod >= LockTTL should fail validation")
		}

		// Invalid MaxRetries
		invalidCfg = cfg
		invalidCfg.MaxRetries = -1
		if err := invalidCfg.Validate(); err == nil {
			rt.Fatal("Config with negative MaxRetries should fail validation")
		}

		// Invalid RetryMultiplier
		invalidCfg = cfg
		invalidCfg.RetryMultiplier = 0.5
		if err := invalidCfg.Validate(); err == nil {
			rt.Fatal("Config with RetryMultiplier < 1.0 should fail validation")
		}

		// Invalid RetryJitter (out of range)
		invalidCfg = cfg
		invalidCfg.RetryJitter = 1.5
		if err := invalidCfg.Validate(); err == nil {
			rt.Fatal("Config with RetryJitter > 1.0 should fail validation")
		}

		// Invalid CircuitThreshold
		invalidCfg = cfg
		invalidCfg.CircuitThreshold = 0
		if err := invalidCfg.Validate(); err == nil {
			rt.Fatal("Config with zero CircuitThreshold should fail validation")
		}
	})
}
