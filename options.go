package rte

import (
	"time"

	"rte/circuit"
)

// Config holds the configuration for the RTE engine.
type Config struct {
	// Lock configuration
	LockTTL          time.Duration // Lock timeout, default 30s
	LockExtendPeriod time.Duration // Lock extension interval, default 10s

	// Retry configuration
	MaxRetries       int           // Maximum retry count, default 3
	RetryInterval    time.Duration // Base retry interval, default 1s
	RetryMaxInterval time.Duration // Maximum retry interval for exponential backoff, default 60s
	RetryMultiplier  float64       // Multiplier for exponential backoff, default 2.0
	RetryJitter      float64       // Jitter factor (0-1) to add randomness, default 0.1

	// Circuit breaker configuration
	CircuitThreshold    int           // Circuit breaker threshold, default 5
	CircuitTimeout      time.Duration // Circuit breaker recovery time, default 30s
	CircuitHalfOpenReqs int           // Half-open state max requests, default 3

	// Recovery configuration
	RecoveryInterval time.Duration // Recovery scan interval, default 30s
	StuckThreshold   time.Duration // Stuck transaction threshold, default 5min

	// Timeout configuration
	StepTimeout time.Duration // Single step timeout, default 10s
	TxTimeout   time.Duration // Total transaction timeout, default 5min

	// Idempotency configuration
	IdempotencyTTL time.Duration // Idempotency record TTL, default 24h
}

// DefaultConfig returns the default configuration for the RTE engine.
func DefaultConfig() Config {
	return Config{
		LockTTL:             30 * time.Second,
		LockExtendPeriod:    10 * time.Second,
		MaxRetries:          3,
		RetryInterval:       1 * time.Second,
		RetryMaxInterval:    60 * time.Second,
		RetryMultiplier:     2.0,
		RetryJitter:         0.1,
		CircuitThreshold:    5,
		CircuitTimeout:      30 * time.Second,
		CircuitHalfOpenReqs: 3,
		RecoveryInterval:    30 * time.Second,
		StuckThreshold:      5 * time.Minute,
		StepTimeout:         10 * time.Second,
		TxTimeout:           5 * time.Minute,
		IdempotencyTTL:      24 * time.Hour,
	}
}

// Option is a function that modifies the Config.
type Option func(*Config)

// WithLockTTL sets the lock TTL.
func WithLockTTL(ttl time.Duration) Option {
	return func(c *Config) {
		c.LockTTL = ttl
	}
}

// WithLockExtendPeriod sets the lock extension period.
func WithLockExtendPeriod(period time.Duration) Option {
	return func(c *Config) {
		c.LockExtendPeriod = period
	}
}

// WithMaxRetries sets the maximum retry count.
func WithMaxRetries(maxRetries int) Option {
	return func(c *Config) {
		c.MaxRetries = maxRetries
	}
}

// WithRetryInterval sets the base retry interval.
func WithRetryInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.RetryInterval = interval
	}
}

// WithRetryMaxInterval sets the maximum retry interval for exponential backoff.
func WithRetryMaxInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.RetryMaxInterval = interval
	}
}

// WithRetryMultiplier sets the multiplier for exponential backoff.
func WithRetryMultiplier(multiplier float64) Option {
	return func(c *Config) {
		c.RetryMultiplier = multiplier
	}
}

// WithRetryJitter sets the jitter factor for retry intervals.
func WithRetryJitter(jitter float64) Option {
	return func(c *Config) {
		c.RetryJitter = jitter
	}
}

// WithCircuitThreshold sets the circuit breaker failure threshold.
func WithCircuitThreshold(threshold int) Option {
	return func(c *Config) {
		c.CircuitThreshold = threshold
	}
}

// WithCircuitTimeout sets the circuit breaker recovery timeout.
func WithCircuitTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.CircuitTimeout = timeout
	}
}

// WithCircuitHalfOpenReqs sets the maximum requests in half-open state.
func WithCircuitHalfOpenReqs(reqs int) Option {
	return func(c *Config) {
		c.CircuitHalfOpenReqs = reqs
	}
}

// WithRecoveryInterval sets the recovery scan interval.
func WithRecoveryInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.RecoveryInterval = interval
	}
}

// WithStuckThreshold sets the stuck transaction threshold.
func WithStuckThreshold(threshold time.Duration) Option {
	return func(c *Config) {
		c.StuckThreshold = threshold
	}
}

// WithStepTimeout sets the single step timeout.
func WithStepTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.StepTimeout = timeout
	}
}

// WithTxTimeout sets the total transaction timeout.
func WithTxTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.TxTimeout = timeout
	}
}

// WithIdempotencyTTL sets the idempotency record TTL.
func WithIdempotencyTTL(ttl time.Duration) Option {
	return func(c *Config) {
		c.IdempotencyTTL = ttl
	}
}

// WithConfig applies a complete Config, overriding all values.
func WithConfig(cfg Config) Option {
	return func(c *Config) {
		*c = cfg
	}
}

// ApplyOptions applies the given options to a default config and returns the result.
func ApplyOptions(opts ...Option) Config {
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// ToBreakerConfig converts the circuit breaker settings to a BreakerConfig.
func (c *Config) ToBreakerConfig() circuit.BreakerConfig {
	return circuit.BreakerConfig{
		Threshold:       c.CircuitThreshold,
		Timeout:         c.CircuitTimeout,
		HalfOpenMaxReqs: c.CircuitHalfOpenReqs,
	}
}

// Validate validates the configuration and returns an error if invalid.
func (c *Config) Validate() error {
	if c.LockTTL <= 0 {
		return ErrInvalidConfig
	}
	if c.LockExtendPeriod <= 0 {
		return ErrInvalidConfig
	}
	if c.LockExtendPeriod >= c.LockTTL {
		return ErrInvalidConfig
	}
	if c.MaxRetries < 0 {
		return ErrInvalidConfig
	}
	if c.RetryInterval < 0 {
		return ErrInvalidConfig
	}
	if c.RetryMaxInterval < 0 {
		return ErrInvalidConfig
	}
	if c.RetryMultiplier < 1.0 {
		return ErrInvalidConfig
	}
	if c.RetryJitter < 0 || c.RetryJitter > 1.0 {
		return ErrInvalidConfig
	}
	if c.CircuitThreshold <= 0 {
		return ErrInvalidConfig
	}
	if c.CircuitTimeout <= 0 {
		return ErrInvalidConfig
	}
	if c.CircuitHalfOpenReqs <= 0 {
		return ErrInvalidConfig
	}
	if c.RecoveryInterval <= 0 {
		return ErrInvalidConfig
	}
	if c.StuckThreshold <= 0 {
		return ErrInvalidConfig
	}
	if c.StepTimeout <= 0 {
		return ErrInvalidConfig
	}
	if c.TxTimeout <= 0 {
		return ErrInvalidConfig
	}
	if c.IdempotencyTTL <= 0 {
		return ErrInvalidConfig
	}
	return nil
}
