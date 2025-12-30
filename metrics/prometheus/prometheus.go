// Package prometheus provides a Prometheus implementation of the metrics interface.
package prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"rte/circuit"
	"rte/metrics"
)

// PrometheusMetrics implements the Metrics interface using Prometheus.
type PrometheusMetrics struct {
	// Transaction metrics
	txStartedTotal       *prometheus.CounterVec
	txCompletedTotal     *prometheus.CounterVec
	txFailedTotal        *prometheus.CounterVec
	txCompensatedTotal   *prometheus.CounterVec
	txCompensationFailed *prometheus.CounterVec
	txDuration           *prometheus.HistogramVec

	// Step metrics
	stepStartedTotal   *prometheus.CounterVec
	stepCompletedTotal *prometheus.CounterVec
	stepFailedTotal    *prometheus.CounterVec
	stepDuration       *prometheus.HistogramVec

	// Circuit breaker metrics
	circuitState *prometheus.GaugeVec

	// Recovery metrics
	recoveryScannedTotal   prometheus.Counter
	recoveryProcessedTotal *prometheus.CounterVec

	// Lock metrics
	lockAcquiredTotal     prometheus.Counter
	lockFailedTotal       *prometheus.CounterVec
	lockExtendedTotal     prometheus.Counter
	lockExtendFailedTotal prometheus.Counter
	lockAcquireDuration   prometheus.Histogram
}

var _ metrics.Metrics = (*PrometheusMetrics)(nil)

// Config holds configuration for PrometheusMetrics.
type Config struct {
	// Namespace is the prefix for all metrics (e.g., "rte")
	Namespace string
	// Subsystem is an optional subsystem name
	Subsystem string
	// Registry is the Prometheus registry to use. If nil, the default registry is used.
	Registry prometheus.Registerer
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		Namespace: "rte",
		Subsystem: "",
		Registry:  prometheus.DefaultRegisterer,
	}
}

// New creates a new PrometheusMetrics instance with the given configuration.
func New(cfg Config) *PrometheusMetrics {
	if cfg.Registry == nil {
		cfg.Registry = prometheus.DefaultRegisterer
	}

	factory := promauto.With(cfg.Registry)

	return &PrometheusMetrics{
		// Transaction metrics
		txStartedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "tx_started_total",
			Help:      "Total number of transactions started",
		}, []string{"tx_type"}),

		txCompletedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "tx_completed_total",
			Help:      "Total number of transactions completed successfully",
		}, []string{"tx_type"}),

		txFailedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "tx_failed_total",
			Help:      "Total number of transactions failed",
		}, []string{"tx_type", "reason"}),

		txCompensatedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "tx_compensated_total",
			Help:      "Total number of transactions compensated",
		}, []string{"tx_type"}),

		txCompensationFailed: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "tx_compensation_failed_total",
			Help:      "Total number of transactions where compensation failed",
		}, []string{"tx_type"}),

		txDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "tx_duration_seconds",
			Help:      "Transaction duration in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~16s
		}, []string{"tx_type"}),

		// Step metrics
		stepStartedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "step_started_total",
			Help:      "Total number of steps started",
		}, []string{"tx_type", "step_name"}),

		stepCompletedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "step_completed_total",
			Help:      "Total number of steps completed successfully",
		}, []string{"tx_type", "step_name"}),

		stepFailedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "step_failed_total",
			Help:      "Total number of steps failed",
		}, []string{"tx_type", "step_name", "reason"}),

		stepDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "step_duration_seconds",
			Help:      "Step duration in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~16s
		}, []string{"tx_type", "step_name"}),

		// Circuit breaker metrics
		circuitState: factory.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "circuit_breaker_state",
			Help:      "Current state of circuit breaker (0=closed, 1=open, 2=half-open)",
		}, []string{"service"}),

		// Recovery metrics
		recoveryScannedTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "recovery_scanned_total",
			Help:      "Total number of transactions scanned for recovery",
		}),

		recoveryProcessedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "recovery_processed_total",
			Help:      "Total number of transactions processed by recovery",
		}, []string{"tx_type", "success"}),

		// Lock metrics
		lockAcquiredTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "lock_acquired_total",
			Help:      "Total number of locks acquired",
		}),

		lockFailedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "lock_failed_total",
			Help:      "Total number of lock acquisition failures",
		}, []string{"reason"}),

		lockExtendedTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "lock_extended_total",
			Help:      "Total number of lock extensions",
		}),

		lockExtendFailedTotal: factory.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "lock_extend_failed_total",
			Help:      "Total number of lock extension failures",
		}),

		lockAcquireDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "lock_acquire_duration_seconds",
			Help:      "Time taken to acquire locks in seconds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 12), // 1ms to ~2s
		}),
	}
}

// Transaction metrics

func (p *PrometheusMetrics) TxStarted(txType string) {
	p.txStartedTotal.WithLabelValues(txType).Inc()
}

func (p *PrometheusMetrics) TxCompleted(txType string, duration time.Duration) {
	p.txCompletedTotal.WithLabelValues(txType).Inc()
	p.txDuration.WithLabelValues(txType).Observe(duration.Seconds())
}

func (p *PrometheusMetrics) TxFailed(txType string, reason string) {
	p.txFailedTotal.WithLabelValues(txType, reason).Inc()
}

func (p *PrometheusMetrics) TxCompensated(txType string) {
	p.txCompensatedTotal.WithLabelValues(txType).Inc()
}

func (p *PrometheusMetrics) TxCompensationFailed(txType string) {
	p.txCompensationFailed.WithLabelValues(txType).Inc()
}

// Step metrics

func (p *PrometheusMetrics) StepStarted(txType, stepName string) {
	p.stepStartedTotal.WithLabelValues(txType, stepName).Inc()
}

func (p *PrometheusMetrics) StepCompleted(txType, stepName string, duration time.Duration) {
	p.stepCompletedTotal.WithLabelValues(txType, stepName).Inc()
	p.stepDuration.WithLabelValues(txType, stepName).Observe(duration.Seconds())
}

func (p *PrometheusMetrics) StepFailed(txType, stepName string, reason string) {
	p.stepFailedTotal.WithLabelValues(txType, stepName, reason).Inc()
}

// Circuit breaker metrics

func (p *PrometheusMetrics) CircuitStateChanged(service string, state circuit.State) {
	p.circuitState.WithLabelValues(service).Set(float64(state))
}

// Recovery metrics

func (p *PrometheusMetrics) RecoveryScanned(count int) {
	p.recoveryScannedTotal.Add(float64(count))
}

func (p *PrometheusMetrics) RecoveryProcessed(txType string, success bool) {
	successStr := "false"
	if success {
		successStr = "true"
	}
	p.recoveryProcessedTotal.WithLabelValues(txType, successStr).Inc()
}

// Lock metrics

func (p *PrometheusMetrics) LockAcquired(duration time.Duration) {
	p.lockAcquiredTotal.Inc()
	p.lockAcquireDuration.Observe(duration.Seconds())
}

func (p *PrometheusMetrics) LockFailed(reason string) {
	p.lockFailedTotal.WithLabelValues(reason).Inc()
}

func (p *PrometheusMetrics) LockExtended() {
	p.lockExtendedTotal.Inc()
}

func (p *PrometheusMetrics) LockExtendFailed() {
	p.lockExtendFailedTotal.Inc()
}
