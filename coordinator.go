package rte

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"rte/circuit"
	"rte/event"
	"rte/idempotency"
	"rte/lock"
)

// TxResult represents the result of a transaction execution.
type TxResult struct {
	// TxID is the transaction ID.
	TxID string
	// Status is the final transaction status.
	Status TxStatus
	// Output contains the final output from all steps.
	Output map[string]any
	// Error contains any error that occurred.
	Error error
	// Duration is the total execution time.
	Duration time.Duration
}

// Coordinator orchestrates transaction execution, including step execution,
// compensation, lock management, and state transitions.
type Coordinator struct {
	// Dependencies
	store   TxStore
	locker  lock.Locker
	breaker circuit.Breaker
	events  event.EventBus
	checker idempotency.Checker

	// Step registry
	steps map[string]Step
	mu    sync.RWMutex

	// Configuration
	config Config
}

// CoordinatorOption is a function that configures the Coordinator.
type CoordinatorOption func(*Coordinator)

// WithStore sets the store for the coordinator.
func WithStore(s TxStore) CoordinatorOption {
	return func(c *Coordinator) {
		c.store = s
	}
}

// WithLocker sets the locker for the coordinator.
func WithLocker(l lock.Locker) CoordinatorOption {
	return func(c *Coordinator) {
		c.locker = l
	}
}

// WithBreaker sets the circuit breaker for the coordinator.
func WithBreaker(b circuit.Breaker) CoordinatorOption {
	return func(c *Coordinator) {
		c.breaker = b
	}
}

// WithEventBus sets the event bus for the coordinator.
func WithEventBus(e event.EventBus) CoordinatorOption {
	return func(c *Coordinator) {
		c.events = e
	}
}

// WithChecker sets the idempotency checker for the coordinator.
func WithChecker(ch idempotency.Checker) CoordinatorOption {
	return func(c *Coordinator) {
		c.checker = ch
	}
}

// WithCoordinatorConfig sets the configuration for the coordinator.
func WithCoordinatorConfig(cfg Config) CoordinatorOption {
	return func(c *Coordinator) {
		c.config = cfg
	}
}

// NewCoordinator creates a new Coordinator with the given options.
func NewCoordinator(opts ...CoordinatorOption) *Coordinator {
	c := &Coordinator{
		steps:  make(map[string]Step),
		config: DefaultConfig(),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// RegisterStep registers a step with the coordinator.
func (c *Coordinator) RegisterStep(step Step) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.steps[step.Name()] = step
}

// GetStep returns a step by name.
func (c *Coordinator) GetStep(name string) Step {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.steps[name]
}

// HasStep returns true if a step with the given name is registered.
func (c *Coordinator) HasStep(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.steps[name]
	return ok
}

// Execute executes a transaction and returns the result.
// This is the main entry point for transaction execution.
func (c *Coordinator) Execute(ctx context.Context, tx *Transaction) (*TxResult, error) {
	startTime := time.Now()

	// Validate transaction
	if err := tx.Validate(c); err != nil {
		return nil, err
	}

	// Create transaction record in store
	storeTx := c.createStoreTx(tx)
	if err := c.store.CreateTransaction(ctx, storeTx); err != nil {
		return nil, fmt.Errorf("failed to create transaction: %w", err)
	}

	// Create step records
	for i, stepName := range tx.StepNames() {
		stepRecord := NewStoreStepRecord(tx.TxID(), i, stepName)
		if err := c.store.CreateStep(ctx, stepRecord); err != nil {
			return nil, fmt.Errorf("failed to create step record: %w", err)
		}
	}

	// Publish transaction created event
	c.publishEvent(ctx, event.NewEvent(event.EventTxCreated).
		WithTxID(tx.TxID()).
		WithTxType(tx.TxType()))

	// Acquire locks
	lockHandle, err := c.acquireLocks(ctx, tx)
	if err != nil {
		c.failTransaction(ctx, storeTx, err)
		return &TxResult{
			TxID:     tx.TxID(),
			Status:   TxStatusFailed,
			Error:    err,
			Duration: time.Since(startTime),
		}, err
	}
	defer lockHandle.Release(ctx)

	// Start lock extender
	stopExtend := c.startLockExtender(ctx, lockHandle, storeTx)
	defer stopExtend()

	// Update status to LOCKED
	storeTx.Status = TxStatusLocked
	now := time.Now()
	storeTx.LockedAt = &now
	if err := c.updateTxWithVersion(ctx, storeTx); err != nil {
		return nil, fmt.Errorf("failed to update transaction status: %w", err)
	}

	// Execute steps
	txCtx := tx.ToContext()
	_, err = c.executeSteps(ctx, storeTx, txCtx)
	if err != nil {
		// Handle failure - may trigger compensation
		return c.handleFailure(ctx, storeTx, txCtx, err, startTime)
	}

	// Confirm transaction
	return c.confirmTransaction(ctx, storeTx, txCtx, startTime)
}

// createStoreTx creates a StoreTx from a Transaction.
func (c *Coordinator) createStoreTx(tx *Transaction) *StoreTx {
	storeTx := NewStoreTx(tx.TxID(), tx.TxType(), tx.StepNames())
	storeTx.LockKeys = tx.LockKeys()
	storeTx.MaxRetries = tx.MaxRetries()
	if tx.MaxRetries() == 0 {
		storeTx.MaxRetries = c.config.MaxRetries
	}

	// Set timeout
	if tx.Timeout() > 0 {
		timeoutAt := time.Now().Add(tx.Timeout())
		storeTx.TimeoutAt = &timeoutAt
	} else if c.config.TxTimeout > 0 {
		timeoutAt := time.Now().Add(c.config.TxTimeout)
		storeTx.TimeoutAt = &timeoutAt
	}

	// Set context
	storeTx.Context = NewStoreTxContext(tx.ToContext())

	return storeTx
}

// acquireLocks acquires distributed locks for the transaction.
func (c *Coordinator) acquireLocks(ctx context.Context, tx *Transaction) (lock.LockHandle, error) {
	lockKeys := tx.LockKeys()
	if len(lockKeys) == 0 {
		// Return a no-op lock handle if no locks needed
		return &noOpLockHandle{}, nil
	}

	handle, err := c.locker.Acquire(ctx, lockKeys, c.config.LockTTL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLockAcquisitionFailed, err)
	}

	return handle, nil
}

// startLockExtender starts a goroutine that periodically extends the lock TTL.
func (c *Coordinator) startLockExtender(ctx context.Context, handle lock.LockHandle, tx *StoreTx) func() {
	// Don't start extender for no-op lock handle
	if _, ok := handle.(*noOpLockHandle); ok {
		return func() {}
	}

	ticker := time.NewTicker(c.config.LockExtendPeriod)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				if err := handle.Extend(ctx, c.config.LockTTL); err != nil {
					c.publishEvent(ctx, event.NewEvent(event.EventAlertWarning).
						WithTxID(tx.TxID).
						WithData("message", "lock extend failed").
						WithError(err))
					// Don't exit immediately - let the transaction have a chance to complete
				}
			case <-done:
				ticker.Stop()
				return
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()

	return func() { close(done) }
}

// executeSteps executes all steps in the transaction.
func (c *Coordinator) executeSteps(ctx context.Context, tx *StoreTx, txCtx *TxContext) (map[string]any, error) {
	// Update status to EXECUTING
	tx.Status = TxStatusExecuting
	if err := c.updateTxWithVersion(ctx, tx); err != nil {
		return nil, err
	}

	// Execute each step
	for i := 0; i < tx.TotalSteps; i++ {
		// Check for timeout
		if tx.TimeoutAt != nil && time.Now().After(*tx.TimeoutAt) {
			return nil, ErrTransactionTimeout
		}

		// Check context cancellation
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Update current step
		tx.CurrentStep = i
		txCtx.StepIndex = i

		// Execute the step
		if err := c.executeStep(ctx, tx, txCtx, i); err != nil {
			return nil, err
		}
	}

	return txCtx.Output, nil
}

// executeStep executes a single step with idempotency checking and circuit breaker protection.
func (c *Coordinator) executeStep(ctx context.Context, tx *StoreTx, txCtx *TxContext, stepIdx int) error {
	stepName := tx.StepNames[stepIdx]
	step := c.GetStep(stepName)
	if step == nil {
		return fmt.Errorf("%w: %s", ErrStepNotFound, stepName)
	}

	// Get step record
	stepRecord, err := c.store.GetStep(ctx, tx.TxID, stepIdx)
	if err != nil {
		return fmt.Errorf("failed to get step record: %w", err)
	}

	// Publish step started event
	c.publishEvent(ctx, event.NewEvent(event.EventStepStarted).
		WithTxID(tx.TxID).
		WithTxType(tx.TxType).
		WithStepName(stepName))

	// Update step status to EXECUTING
	stepRecord.Status = StepStatusExecuting
	now := time.Now()
	stepRecord.StartedAt = &now
	if err := c.store.UpdateStep(ctx, stepRecord); err != nil {
		return fmt.Errorf("failed to update step status: %w", err)
	}

	// Check idempotency
	if step.SupportsIdempotency() && c.checker != nil {
		key := step.IdempotencyKey(txCtx)
		if key != "" {
			exists, result, err := c.checker.Check(ctx, key)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrIdempotencyCheckFailed, err)
			}
			if exists {
				// Use cached result
				if len(result) > 0 {
					var output map[string]any
					if err := json.Unmarshal(result, &output); err == nil {
						for k, v := range output {
							txCtx.SetOutput(k, v)
						}
					}
				}
				stepRecord.Status = StepStatusCompleted
				stepRecord.IdempotencyKey = key
				completedAt := time.Now()
				stepRecord.CompletedAt = &completedAt
				c.store.UpdateStep(ctx, stepRecord)

				c.publishEvent(ctx, event.NewEvent(event.EventStepCompleted).
					WithTxID(tx.TxID).
					WithTxType(tx.TxType).
					WithStepName(stepName))
				return nil
			}
			stepRecord.IdempotencyKey = key
		}
	}

	// Execute with circuit breaker
	var execErr error
	if c.breaker != nil {
		cb := c.breaker.Get(stepName)
		execErr = cb.Execute(ctx, func() error {
			return c.executeWithTimeout(ctx, step, txCtx)
		})
	} else {
		execErr = c.executeWithTimeout(ctx, step, txCtx)
	}

	if execErr != nil {
		// Step failed
		stepRecord.Status = StepStatusFailed
		stepRecord.ErrorMsg = execErr.Error()
		c.store.UpdateStep(ctx, stepRecord)

		c.publishEvent(ctx, event.NewEvent(event.EventStepFailed).
			WithTxID(tx.TxID).
			WithTxType(tx.TxType).
			WithStepName(stepName).
			WithError(execErr))

		return fmt.Errorf("%w: step %s: %v", ErrStepExecutionFailed, stepName, execErr)
	}

	// Mark idempotency
	if step.SupportsIdempotency() && c.checker != nil && stepRecord.IdempotencyKey != "" {
		output, _ := json.Marshal(txCtx.Output)
		c.checker.Mark(ctx, stepRecord.IdempotencyKey, output, c.config.IdempotencyTTL)
	}

	// Update step status to COMPLETED
	stepRecord.Status = StepStatusCompleted
	stepRecord.Output, _ = json.Marshal(txCtx.Output)
	completedAt := time.Now()
	stepRecord.CompletedAt = &completedAt
	if err := c.store.UpdateStep(ctx, stepRecord); err != nil {
		return fmt.Errorf("failed to update step status: %w", err)
	}

	// Update transaction context
	tx.Context = NewStoreTxContext(txCtx)
	tx.CurrentStep = stepIdx + 1
	if err := c.updateTxWithVersion(ctx, tx); err != nil {
		return fmt.Errorf("failed to update transaction: %w", err)
	}

	c.publishEvent(ctx, event.NewEvent(event.EventStepCompleted).
		WithTxID(tx.TxID).
		WithTxType(tx.TxType).
		WithStepName(stepName))

	return nil
}

// executeWithTimeout executes a step with timeout.
func (c *Coordinator) executeWithTimeout(ctx context.Context, step Step, txCtx *TxContext) error {
	// Determine timeout
	timeout := c.config.StepTimeout
	if stepCfg := step.Config(); stepCfg != nil && stepCfg.Timeout > 0 {
		timeout = stepCfg.Timeout
	}

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Execute step
	done := make(chan error, 1)
	go func() {
		done <- step.Execute(timeoutCtx, txCtx)
	}()

	select {
	case err := <-done:
		return err
	case <-timeoutCtx.Done():
		if timeoutCtx.Err() == context.DeadlineExceeded {
			return ErrStepTimeout
		}
		return timeoutCtx.Err()
	}
}

// handleFailure handles a transaction failure, potentially triggering compensation.
func (c *Coordinator) handleFailure(ctx context.Context, tx *StoreTx, txCtx *TxContext, execErr error, startTime time.Time) (*TxResult, error) {
	// Check if timeout
	if execErr == ErrTransactionTimeout || execErr == context.DeadlineExceeded {
		tx.Status = TxStatusTimeout
		tx.ErrorMsg = execErr.Error()
		c.updateTxWithVersion(ctx, tx)

		c.publishEvent(ctx, event.NewEvent(event.EventTxTimeout).
			WithTxID(tx.TxID).
			WithTxType(tx.TxType).
			WithError(execErr))

		return &TxResult{
			TxID:     tx.TxID,
			Status:   TxStatusTimeout,
			Error:    execErr,
			Duration: time.Since(startTime),
		}, execErr
	}

	// Update to FAILED status
	tx.Status = TxStatusFailed
	tx.ErrorMsg = execErr.Error()
	if err := c.updateTxWithVersion(ctx, tx); err != nil {
		return nil, err
	}

	// Check if any step supports compensation
	needsCompensation := c.needsCompensation(tx)

	if needsCompensation {
		// Trigger compensation
		compErr := c.compensate(ctx, tx, txCtx, tx.CurrentStep)
		if compErr != nil {
			return &TxResult{
				TxID:     tx.TxID,
				Status:   tx.Status,
				Error:    compErr,
				Duration: time.Since(startTime),
			}, compErr
		}

		return &TxResult{
			TxID:     tx.TxID,
			Status:   TxStatusCompensated,
			Error:    execErr,
			Duration: time.Since(startTime),
		}, execErr
	}

	c.publishEvent(ctx, event.NewEvent(event.EventTxFailed).
		WithTxID(tx.TxID).
		WithTxType(tx.TxType).
		WithError(execErr))

	return &TxResult{
		TxID:     tx.TxID,
		Status:   TxStatusFailed,
		Error:    execErr,
		Duration: time.Since(startTime),
	}, execErr
}

// needsCompensation checks if any completed step supports compensation.
func (c *Coordinator) needsCompensation(tx *StoreTx) bool {
	for i := 0; i < tx.CurrentStep; i++ {
		stepName := tx.StepNames[i]
		step := c.GetStep(stepName)
		if step != nil && step.SupportsCompensation() {
			return true
		}
	}
	return false
}

// compensate performs compensation for completed steps in reverse order.
func (c *Coordinator) compensate(ctx context.Context, tx *StoreTx, txCtx *TxContext, failedStepIdx int) error {
	// Update status to COMPENSATING
	tx.Status = TxStatusCompensating
	if err := c.updateTxWithVersion(ctx, tx); err != nil {
		return err
	}

	// Compensate from failed step backwards
	for i := failedStepIdx; i >= 0; i-- {
		stepRecord, err := c.store.GetStep(ctx, tx.TxID, i)
		if err != nil {
			continue
		}

		// Skip steps that were not completed successfully
		// Only COMPLETED steps need compensation
		if stepRecord.Status != StepStatusCompleted {
			continue
		}

		stepName := tx.StepNames[i]
		step := c.GetStep(stepName)
		if step == nil {
			continue
		}

		// Check if step supports compensation
		if !step.SupportsCompensation() {
			continue
		}

		// Update step status to COMPENSATING
		stepRecord.Status = StepStatusCompensating
		c.store.UpdateStep(ctx, stepRecord)

		// Execute compensation with retry
		compErr := c.compensateWithRetry(ctx, step, txCtx, stepRecord)
		if compErr != nil {
			// Compensation failed
			tx.Status = TxStatusCompensationFailed
			tx.ErrorMsg = fmt.Sprintf("compensation failed at step %d (%s): %v", i, stepName, compErr)
			c.updateTxWithVersion(ctx, tx)

			c.publishEvent(ctx, event.NewEvent(event.EventTxCompensationFailed).
				WithTxID(tx.TxID).
				WithTxType(tx.TxType).
				WithError(compErr))

			c.publishEvent(ctx, event.NewEvent(event.EventAlertCritical).
				WithTxID(tx.TxID).
				WithTxType(tx.TxType).
				WithData("message", fmt.Sprintf("Transaction %s compensation failed at step %s", tx.TxID, stepName)).
				WithError(compErr))

			return fmt.Errorf("%w: step %s: %v", ErrCompensationFailed, stepName, compErr)
		}

		// Update step status to COMPENSATED
		stepRecord.Status = StepStatusCompensated
		c.store.UpdateStep(ctx, stepRecord)
	}

	// Update transaction status to COMPENSATED
	tx.Status = TxStatusCompensated
	if err := c.updateTxWithVersion(ctx, tx); err != nil {
		return err
	}

	c.publishEvent(ctx, event.NewEvent(event.EventTxFailed).
		WithTxID(tx.TxID).
		WithTxType(tx.TxType).
		WithData("compensated", true))

	return nil
}

// compensateWithRetry executes compensation with retry logic using exponential backoff.
func (c *Coordinator) compensateWithRetry(ctx context.Context, step Step, txCtx *TxContext, stepRecord *StoreStepRecord) error {
	maxRetries := c.config.MaxRetries
	if stepCfg := step.Config(); stepCfg != nil && stepCfg.MaxRetries > 0 {
		maxRetries = stepCfg.MaxRetries
	}

	baseInterval := c.config.RetryInterval
	if stepCfg := step.Config(); stepCfg != nil && stepCfg.RetryInterval > 0 {
		baseInterval = stepCfg.RetryInterval
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Calculate exponential backoff with jitter
			backoff := c.calculateBackoff(baseInterval, attempt)

			// Wait before retry
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// Execute compensation with timeout
		err := c.executeCompensationWithTimeout(ctx, step, txCtx)
		if err == nil {
			return nil
		}

		lastErr = err
		stepRecord.RetryCount = attempt + 1
		c.store.UpdateStep(ctx, stepRecord)
	}

	return fmt.Errorf("%w: %v", ErrCompensationMaxRetriesExceeded, lastErr)
}

// calculateBackoff calculates the backoff duration using exponential backoff with jitter.
// Formula: min(base * multiplier^attempt + jitter, maxInterval)
func (c *Coordinator) calculateBackoff(baseInterval time.Duration, attempt int) time.Duration {
	// Calculate exponential backoff: base * multiplier^attempt
	multiplier := c.config.RetryMultiplier
	if multiplier < 1.0 {
		multiplier = 2.0
	}

	// Calculate the exponential part
	backoff := float64(baseInterval)
	for i := 0; i < attempt; i++ {
		backoff *= multiplier
	}

	// Add jitter (randomness) to prevent thundering herd
	jitterFactor := c.config.RetryJitter
	if jitterFactor > 0 {
		jitter := backoff * jitterFactor * rand.Float64()
		backoff += jitter
	}

	// Cap at max interval
	maxInterval := c.config.RetryMaxInterval
	if maxInterval > 0 && time.Duration(backoff) > maxInterval {
		backoff = float64(maxInterval)
	}

	return time.Duration(backoff)
}

// executeCompensationWithTimeout executes compensation with timeout.
func (c *Coordinator) executeCompensationWithTimeout(ctx context.Context, step Step, txCtx *TxContext) error {
	// Determine timeout
	timeout := c.config.StepTimeout
	if stepCfg := step.Config(); stepCfg != nil && stepCfg.Timeout > 0 {
		timeout = stepCfg.Timeout
	}

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Execute compensation
	done := make(chan error, 1)
	go func() {
		done <- step.Compensate(timeoutCtx, txCtx)
	}()

	select {
	case err := <-done:
		return err
	case <-timeoutCtx.Done():
		if timeoutCtx.Err() == context.DeadlineExceeded {
			return ErrStepTimeout
		}
		return timeoutCtx.Err()
	}
}

// confirmTransaction confirms a successful transaction.
func (c *Coordinator) confirmTransaction(ctx context.Context, tx *StoreTx, txCtx *TxContext, startTime time.Time) (*TxResult, error) {
	// Update status to CONFIRMING
	tx.Status = TxStatusConfirming
	if err := c.updateTxWithVersion(ctx, tx); err != nil {
		return nil, err
	}

	// Update status to COMPLETED
	tx.Status = TxStatusCompleted
	now := time.Now()
	tx.CompletedAt = &now
	tx.Context = NewStoreTxContext(txCtx)
	if err := c.updateTxWithVersion(ctx, tx); err != nil {
		return nil, err
	}

	c.publishEvent(ctx, event.NewEvent(event.EventTxCompleted).
		WithTxID(tx.TxID).
		WithTxType(tx.TxType))

	return &TxResult{
		TxID:     tx.TxID,
		Status:   TxStatusCompleted,
		Output:   txCtx.Output,
		Duration: time.Since(startTime),
	}, nil
}

// updateTxWithVersion updates a transaction with optimistic locking.
func (c *Coordinator) updateTxWithVersion(ctx context.Context, tx *StoreTx) error {
	tx.IncrementVersion()
	if err := c.store.UpdateTransaction(ctx, tx); err != nil {
		return fmt.Errorf("%w: %v", ErrVersionConflict, err)
	}
	return nil
}

// failTransaction marks a transaction as failed.
func (c *Coordinator) failTransaction(ctx context.Context, tx *StoreTx, err error) {
	tx.Status = TxStatusFailed
	tx.ErrorMsg = err.Error()
	c.updateTxWithVersion(ctx, tx)

	c.publishEvent(ctx, event.NewEvent(event.EventTxFailed).
		WithTxID(tx.TxID).
		WithTxType(tx.TxType).
		WithError(err))
}

// publishEvent publishes an event to the event bus.
func (c *Coordinator) publishEvent(ctx context.Context, e event.Event) {
	if c.events != nil {
		c.events.Publish(ctx, e)
	}
}

// Resume resumes a stuck or failed transaction.
// This is called by the recovery worker.
func (c *Coordinator) Resume(ctx context.Context, tx *StoreTx) (*TxResult, error) {
	startTime := time.Now()

	// Reload transaction context
	txCtx := tx.Context.ToTxContext()

	switch tx.Status {
	case TxStatusLocked, TxStatusExecuting:
		// Continue execution from current step
		_, err := c.executeSteps(ctx, tx, txCtx)
		if err != nil {
			return c.handleFailure(ctx, tx, txCtx, err, startTime)
		}
		return c.confirmTransaction(ctx, tx, txCtx, startTime)

	case TxStatusFailed:
		// Check if can retry
		if tx.CanRetry() {
			tx.RetryCount++
			tx.Status = TxStatusExecuting
			tx.ErrorMsg = ""
			if err := c.updateTxWithVersion(ctx, tx); err != nil {
				return nil, err
			}

			_, err := c.executeSteps(ctx, tx, txCtx)
			if err != nil {
				return c.handleFailure(ctx, tx, txCtx, err, startTime)
			}
			return c.confirmTransaction(ctx, tx, txCtx, startTime)
		}

		// Trigger compensation if needed
		if c.needsCompensation(tx) {
			compErr := c.compensate(ctx, tx, txCtx, tx.CurrentStep)
			if compErr != nil {
				return &TxResult{
					TxID:     tx.TxID,
					Status:   tx.Status,
					Error:    compErr,
					Duration: time.Since(startTime),
				}, compErr
			}
		}

		return &TxResult{
			TxID:     tx.TxID,
			Status:   tx.Status,
			Duration: time.Since(startTime),
		}, nil

	default:
		return &TxResult{
			TxID:     tx.TxID,
			Status:   tx.Status,
			Duration: time.Since(startTime),
		}, nil
	}
}

// noOpLockHandle is a no-op lock handle for transactions without locks.
type noOpLockHandle struct{}

func (h *noOpLockHandle) Extend(ctx context.Context, ttl time.Duration) error {
	return nil
}

func (h *noOpLockHandle) Release(ctx context.Context) error {
	return nil
}

func (h *noOpLockHandle) Keys() []string {
	return nil
}
