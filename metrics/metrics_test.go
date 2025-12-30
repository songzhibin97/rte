package metrics

import (
	"testing"
	"time"

	"rte/circuit"
)

func TestNoopMetrics(t *testing.T) {
	m := &NoopMetrics{}

	// All methods should not panic
	m.TxStarted("transfer")
	m.TxCompleted("transfer", 100*time.Millisecond)
	m.TxFailed("transfer", "error")
	m.TxCompensated("transfer")
	m.TxCompensationFailed("transfer")
	m.StepStarted("transfer", "debit")
	m.StepCompleted("transfer", "debit", 50*time.Millisecond)
	m.StepFailed("transfer", "debit", "error")
	m.CircuitStateChanged("service", circuit.StateClosed)
	m.RecoveryScanned(5)
	m.RecoveryProcessed("transfer", true)
	m.LockAcquired(10 * time.Millisecond)
	m.LockFailed("timeout")
	m.LockExtended()
	m.LockExtendFailed()
}

func TestNoopMetrics_ImplementsInterface(t *testing.T) {
	var _ Metrics = (*NoopMetrics)(nil)
}
