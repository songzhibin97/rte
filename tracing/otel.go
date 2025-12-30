// Package tracing provides OpenTelemetry tracing integration for the RTE engine.
package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Tracer defines the interface for distributed tracing.
type Tracer interface {
	// StartTransaction starts a new root span for a transaction.
	StartTransaction(ctx context.Context, txID, txType string) (context.Context, Span)

	// StartStep starts a child span for a step within a transaction.
	StartStep(ctx context.Context, txID, stepName string, stepIndex int) (context.Context, Span)
}

// Span represents an active tracing span.
type Span interface {
	// End completes the span.
	End()

	// SetError marks the span as having an error.
	SetError(err error)

	// SetStatus sets the span status.
	SetStatus(code codes.Code, description string)

	// SetAttributes adds attributes to the span.
	SetAttributes(attrs ...attribute.KeyValue)

	// AddEvent adds an event to the span.
	AddEvent(name string, attrs ...attribute.KeyValue)
}

// OTelTracer implements Tracer using OpenTelemetry.
type OTelTracer struct {
	tracer trace.Tracer
}

// Config holds configuration for OTelTracer.
type Config struct {
	// ServiceName is the name of the service for tracing.
	ServiceName string
	// TracerProvider is the OpenTelemetry tracer provider. If nil, the global provider is used.
	TracerProvider trace.TracerProvider
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		ServiceName:    "rte",
		TracerProvider: nil,
	}
}

// NewOTelTracer creates a new OTelTracer with the given configuration.
func NewOTelTracer(cfg Config) *OTelTracer {
	var tp trace.TracerProvider
	if cfg.TracerProvider != nil {
		tp = cfg.TracerProvider
	} else {
		tp = otel.GetTracerProvider()
	}

	return &OTelTracer{
		tracer: tp.Tracer(cfg.ServiceName),
	}
}

// StartTransaction starts a new root span for a transaction.
func (t *OTelTracer) StartTransaction(ctx context.Context, txID, txType string) (context.Context, Span) {
	ctx, span := t.tracer.Start(ctx, "tx.execute",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("tx.id", txID),
			attribute.String("tx.type", txType),
		),
	)
	return ctx, &otelSpan{span: span}
}

// StartStep starts a child span for a step within a transaction.
func (t *OTelTracer) StartStep(ctx context.Context, txID, stepName string, stepIndex int) (context.Context, Span) {
	ctx, span := t.tracer.Start(ctx, "step.execute",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("tx.id", txID),
			attribute.String("step.name", stepName),
			attribute.Int("step.index", stepIndex),
		),
	)
	return ctx, &otelSpan{span: span}
}

// otelSpan wraps an OpenTelemetry span.
type otelSpan struct {
	span trace.Span
}

func (s *otelSpan) End() {
	s.span.End()
}

func (s *otelSpan) SetError(err error) {
	if err != nil {
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, err.Error())
	}
}

func (s *otelSpan) SetStatus(code codes.Code, description string) {
	s.span.SetStatus(code, description)
}

func (s *otelSpan) SetAttributes(attrs ...attribute.KeyValue) {
	s.span.SetAttributes(attrs...)
}

func (s *otelSpan) AddEvent(name string, attrs ...attribute.KeyValue) {
	s.span.AddEvent(name, trace.WithAttributes(attrs...))
}

// NoopTracer is a no-op implementation of Tracer for testing or when tracing is disabled.
type NoopTracer struct{}

var _ Tracer = (*NoopTracer)(nil)

func (n *NoopTracer) StartTransaction(ctx context.Context, txID, txType string) (context.Context, Span) {
	return ctx, &noopSpan{}
}

func (n *NoopTracer) StartStep(ctx context.Context, txID, stepName string, stepIndex int) (context.Context, Span) {
	return ctx, &noopSpan{}
}

// noopSpan is a no-op span implementation.
type noopSpan struct{}

func (s *noopSpan) End()                                              {}
func (s *noopSpan) SetError(err error)                                {}
func (s *noopSpan) SetStatus(code codes.Code, description string)     {}
func (s *noopSpan) SetAttributes(attrs ...attribute.KeyValue)         {}
func (s *noopSpan) AddEvent(name string, attrs ...attribute.KeyValue) {}
