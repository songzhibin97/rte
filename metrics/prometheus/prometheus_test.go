package prometheus

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"rte/circuit"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Namespace != "rte" {
		t.Errorf("expected namespace 'rte', got '%s'", cfg.Namespace)
	}
	if cfg.Subsystem != "" {
		t.Errorf("expected empty subsystem, got '%s'", cfg.Subsystem)
	}
	if cfg.Registry != prometheus.DefaultRegisterer {
		t.Error("expected default registry")
	}
}

func TestPrometheusMetrics_TxCompensated(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(Config{Namespace: "test", Registry: reg})

	m.TxCompensated("transfer")
	m.TxCompensated("transfer")
	m.TxCompensated("deposit")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	found := false
	for _, mf := range mfs {
		if mf.GetName() == "test_tx_compensated_total" {
			found = true
			metrics := mf.GetMetric()
			if len(metrics) != 2 {
				t.Errorf("expected 2 metric series, got %d", len(metrics))
			}
			// Check that transfer has count of 2
			for _, metric := range metrics {
				for _, label := range metric.GetLabel() {
					if label.GetName() == "tx_type" && label.GetValue() == "transfer" {
						if metric.GetCounter().GetValue() != 2 {
							t.Errorf("expected transfer count 2, got %f", metric.GetCounter().GetValue())
						}
					}
				}
			}
		}
	}
	if !found {
		t.Error("tx_compensated_total metric not found")
	}
}

func TestPrometheusMetrics_TxCompensationFailed(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(Config{Namespace: "test", Registry: reg})

	m.TxCompensationFailed("transfer")
	m.TxCompensationFailed("transfer")
	m.TxCompensationFailed("withdrawal")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	found := false
	for _, mf := range mfs {
		if mf.GetName() == "test_tx_compensation_failed_total" {
			found = true
			metrics := mf.GetMetric()
			if len(metrics) != 2 {
				t.Errorf("expected 2 metric series, got %d", len(metrics))
			}
			// Check that transfer has count of 2
			for _, metric := range metrics {
				for _, label := range metric.GetLabel() {
					if label.GetName() == "tx_type" && label.GetValue() == "transfer" {
						if metric.GetCounter().GetValue() != 2 {
							t.Errorf("expected transfer count 2, got %f", metric.GetCounter().GetValue())
						}
					}
				}
			}
		}
	}
	if !found {
		t.Error("tx_compensation_failed_total metric not found")
	}
}

func TestPrometheusMetrics_TxStarted(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(Config{Namespace: "test", Registry: reg})

	m.TxStarted("transfer")
	m.TxStarted("transfer")
	m.TxStarted("deposit")

	// Verify metrics were recorded
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	found := false
	for _, mf := range mfs {
		if mf.GetName() == "test_tx_started_total" {
			found = true
			metrics := mf.GetMetric()
			if len(metrics) != 2 {
				t.Errorf("expected 2 metric series, got %d", len(metrics))
			}
		}
	}
	if !found {
		t.Error("tx_started_total metric not found")
	}
}

func TestPrometheusMetrics_TxCompleted(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(Config{Namespace: "test", Registry: reg})

	m.TxCompleted("transfer", 100*time.Millisecond)
	m.TxCompleted("transfer", 200*time.Millisecond)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	foundCounter := false
	foundHistogram := false
	for _, mf := range mfs {
		switch mf.GetName() {
		case "test_tx_completed_total":
			foundCounter = true
		case "test_tx_duration_seconds":
			foundHistogram = true
		}
	}
	if !foundCounter {
		t.Error("tx_completed_total metric not found")
	}
	if !foundHistogram {
		t.Error("tx_duration_seconds metric not found")
	}
}

func TestPrometheusMetrics_TxFailed(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(Config{Namespace: "test", Registry: reg})

	m.TxFailed("transfer", "timeout")
	m.TxFailed("transfer", "step_error")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	found := false
	for _, mf := range mfs {
		if mf.GetName() == "test_tx_failed_total" {
			found = true
			metrics := mf.GetMetric()
			if len(metrics) != 2 {
				t.Errorf("expected 2 metric series (different reasons), got %d", len(metrics))
			}
		}
	}
	if !found {
		t.Error("tx_failed_total metric not found")
	}
}

func TestPrometheusMetrics_StepMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(Config{Namespace: "test", Registry: reg})

	m.StepStarted("transfer", "debit")
	m.StepCompleted("transfer", "debit", 50*time.Millisecond)
	m.StepFailed("transfer", "credit", "insufficient_funds")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	expectedMetrics := map[string]bool{
		"test_step_started_total":    false,
		"test_step_completed_total":  false,
		"test_step_failed_total":     false,
		"test_step_duration_seconds": false,
	}

	for _, mf := range mfs {
		if _, ok := expectedMetrics[mf.GetName()]; ok {
			expectedMetrics[mf.GetName()] = true
		}
	}

	for name, found := range expectedMetrics {
		if !found {
			t.Errorf("metric %s not found", name)
		}
	}
}

func TestPrometheusMetrics_CircuitStateChanged(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(Config{Namespace: "test", Registry: reg})

	m.CircuitStateChanged("payment-service", circuit.StateClosed)
	m.CircuitStateChanged("payment-service", circuit.StateOpen)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	found := false
	for _, mf := range mfs {
		if mf.GetName() == "test_circuit_breaker_state" {
			found = true
			metrics := mf.GetMetric()
			if len(metrics) != 1 {
				t.Errorf("expected 1 metric series, got %d", len(metrics))
			}
			// Should be StateOpen (1)
			if metrics[0].GetGauge().GetValue() != float64(circuit.StateOpen) {
				t.Errorf("expected state %d, got %f", circuit.StateOpen, metrics[0].GetGauge().GetValue())
			}
		}
	}
	if !found {
		t.Error("circuit_breaker_state metric not found")
	}
}

func TestPrometheusMetrics_RecoveryMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(Config{Namespace: "test", Registry: reg})

	m.RecoveryScanned(5)
	m.RecoveryProcessed("transfer", true)
	m.RecoveryProcessed("transfer", false)

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	foundScanned := false
	foundProcessed := false
	for _, mf := range mfs {
		switch mf.GetName() {
		case "test_recovery_scanned_total":
			foundScanned = true
		case "test_recovery_processed_total":
			foundProcessed = true
			metrics := mf.GetMetric()
			if len(metrics) != 2 {
				t.Errorf("expected 2 metric series (success/failure), got %d", len(metrics))
			}
		}
	}
	if !foundScanned {
		t.Error("recovery_scanned_total metric not found")
	}
	if !foundProcessed {
		t.Error("recovery_processed_total metric not found")
	}
}

func TestPrometheusMetrics_LockMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(Config{Namespace: "test", Registry: reg})

	m.LockAcquired(10 * time.Millisecond)
	m.LockFailed("timeout")
	m.LockExtended()
	m.LockExtendFailed()

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("failed to gather metrics: %v", err)
	}

	expectedMetrics := map[string]bool{
		"test_lock_acquired_total":           false,
		"test_lock_failed_total":             false,
		"test_lock_extended_total":           false,
		"test_lock_extend_failed_total":      false,
		"test_lock_acquire_duration_seconds": false,
	}

	for _, mf := range mfs {
		if _, ok := expectedMetrics[mf.GetName()]; ok {
			expectedMetrics[mf.GetName()] = true
		}
	}

	for name, found := range expectedMetrics {
		if !found {
			t.Errorf("metric %s not found", name)
		}
	}
}
