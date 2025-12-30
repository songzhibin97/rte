package rte

import (
	"fmt"
	"sync"
)

// TxContext represents the transaction context passed between steps
type TxContext struct {
	TxID      string            `json:"tx_id"`      // Transaction ID
	TxType    string            `json:"tx_type"`    // Transaction type
	StepIndex int               `json:"step_index"` // Current step index
	Input     map[string]any    `json:"input"`      // Input parameters
	Output    map[string]any    `json:"output"`     // Output results (shared between steps)
	Metadata  map[string]string `json:"metadata"`   // Metadata
	mu        sync.RWMutex      // Protects concurrent access
}

// NewTxContext creates a new transaction context
func NewTxContext(txID, txType string) *TxContext {
	return &TxContext{
		TxID:     txID,
		TxType:   txType,
		Input:    make(map[string]any),
		Output:   make(map[string]any),
		Metadata: make(map[string]string),
	}
}

// GetInput retrieves an input value by key
func (c *TxContext) GetInput(key string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.Input[key]
	return v, ok
}

// SetInput sets an input value
func (c *TxContext) SetInput(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Input == nil {
		c.Input = make(map[string]any)
	}
	c.Input[key] = value
}

// GetOutput retrieves an output value by key
func (c *TxContext) GetOutput(key string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.Output[key]
	return v, ok
}

// SetOutput sets an output value
func (c *TxContext) SetOutput(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Output == nil {
		c.Output = make(map[string]any)
	}
	c.Output[key] = value
}

// GetMetadata retrieves a metadata value by key
func (c *TxContext) GetMetadata(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.Metadata[key]
	return v, ok
}

// SetMetadata sets a metadata value
func (c *TxContext) SetMetadata(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Metadata == nil {
		c.Metadata = make(map[string]string)
	}
	c.Metadata[key] = value
}

// GetInputAs is a type-safe generic input getter
func GetInputAs[T any](c *TxContext, key string) (T, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var zero T
	v, ok := c.Input[key]
	if !ok {
		return zero, fmt.Errorf("%w: %s", ErrInputKeyNotFound, key)
	}
	typed, ok := v.(T)
	if !ok {
		return zero, fmt.Errorf("%w: key %s expected %T but got %T", ErrInputTypeMismatch, key, zero, v)
	}
	return typed, nil
}

// GetOutputAs is a type-safe generic output getter
func GetOutputAs[T any](c *TxContext, key string) (T, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var zero T
	v, ok := c.Output[key]
	if !ok {
		return zero, fmt.Errorf("%w: %s", ErrOutputKeyNotFound, key)
	}
	typed, ok := v.(T)
	if !ok {
		return zero, fmt.Errorf("%w: key %s expected %T but got %T", ErrOutputTypeMismatch, key, zero, v)
	}
	return typed, nil
}

// Clone creates a deep copy of the context
func (c *TxContext) Clone() *TxContext {
	c.mu.RLock()
	defer c.mu.RUnlock()

	clone := &TxContext{
		TxID:      c.TxID,
		TxType:    c.TxType,
		StepIndex: c.StepIndex,
		Input:     make(map[string]any, len(c.Input)),
		Output:    make(map[string]any, len(c.Output)),
		Metadata:  make(map[string]string, len(c.Metadata)),
	}

	for k, v := range c.Input {
		clone.Input[k] = v
	}
	for k, v := range c.Output {
		clone.Output[k] = v
	}
	for k, v := range c.Metadata {
		clone.Metadata[k] = v
	}

	return clone
}

// WithInput returns a new context with the given input merged
func (c *TxContext) WithInput(input map[string]any) *TxContext {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Input == nil {
		c.Input = make(map[string]any)
	}
	for k, v := range input {
		c.Input[k] = v
	}
	return c
}
