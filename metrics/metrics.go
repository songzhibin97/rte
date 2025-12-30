// Package metrics provides the metrics interface for the RTE engine.
package metrics

import (
	"time"

	"rte/circuit"
)

// Metrics defines the interface for collecting observability metrics.
// Implementations can use Prometheus, StatsD, or other metrics backends.
type Metrics interface {
	// Transaction metrics
	TxStarted(txType string)
	TxCompleted(txType string, duration time.Duration)
	TxFailed(txType string, reason string)
	TxCompensated(txType string)
	TxCompensationFailed(txType string)

	// Step metrics
	StepStarted(txType, stepName string)
	StepCompleted(txType, stepName string, duration time.Duration)
	StepFailed(txType, stepName string, reason string)

	// Circuit breaker metrics
	CircuitStateChanged(service string, state circuit.State)

	// Recovery metrics
	RecoveryScanned(count int)
	RecoveryProcessed(txType string, success bool)

	// Lock metrics
	LockAcquired(duration time.Duration)
	LockFailed(reason string)
	LockExtended()
	LockExtendFailed()
}

// NoopMetrics is a no-op implementation of Metrics for testing or when metrics are disabled.
type NoopMetrics struct{}

var _ Metrics = (*NoopMetrics)(nil)

func (n *NoopMetrics) TxStarted(txType string)                                 {}
func (n *NoopMetrics) TxCompleted(txType string, duration time.Duration)       {}
func (n *NoopMetrics) TxFailed(txType string, reason string)                   {}
func (n *NoopMetrics) TxCompensated(txType string)                             {}
func (n *NoopMetrics) TxCompensationFailed(txType string)                      {}
func (n *NoopMetrics) StepStarted(txType, stepName string)                     {}
func (n *NoopMetrics) StepCompleted(txType, stepName string, d time.Duration)  {}
func (n *NoopMetrics) StepFailed(txType, stepName string, reason string)       {}
func (n *NoopMetrics) CircuitStateChanged(service string, state circuit.State) {}
func (n *NoopMetrics) RecoveryScanned(count int)                               {}
func (n *NoopMetrics) RecoveryProcessed(txType string, success bool)           {}
func (n *NoopMetrics) LockAcquired(duration time.Duration)                     {}
func (n *NoopMetrics) LockFailed(reason string)                                {}
func (n *NoopMetrics) LockExtended()                                           {}
func (n *NoopMetrics) LockExtendFailed()                                       {}
