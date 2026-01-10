package tracing

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestOTelTracer_StartTransaction(t *testing.T) {
	// Create an in-memory span exporter for testing
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	defer tp.Shutdown(context.Background())

	tracer := NewOTelTracer(Config{
		ServiceName:    "test-rte",
		TracerProvider: tp,
	})

	ctx := context.Background()
	ctx, span := tracer.StartTransaction(ctx, "tx-123", "transfer")
	span.End()

	// Force flush
	tp.ForceFlush(context.Background())

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	s := spans[0]
	if s.Name != "tx.execute" {
		t.Errorf("expected span name 'tx.execute', got '%s'", s.Name)
	}

	// Check attributes
	attrs := s.Attributes
	foundTxID := false
	foundTxType := false
	for _, attr := range attrs {
		switch string(attr.Key) {
		case "tx.id":
			foundTxID = true
			if attr.Value.AsString() != "tx-123" {
				t.Errorf("expected tx.id 'tx-123', got '%s'", attr.Value.AsString())
			}
		case "tx.type":
			foundTxType = true
			if attr.Value.AsString() != "transfer" {
				t.Errorf("expected tx.type 'transfer', got '%s'", attr.Value.AsString())
			}
		}
	}
	if !foundTxID {
		t.Error("tx.id attribute not found")
	}
	if !foundTxType {
		t.Error("tx.type attribute not found")
	}
}

func TestOTelTracer_StartStep(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	defer tp.Shutdown(context.Background())

	tracer := NewOTelTracer(Config{
		ServiceName:    "test-rte",
		TracerProvider: tp,
	})

	ctx := context.Background()
	// Start a transaction first to create parent span
	ctx, txSpan := tracer.StartTransaction(ctx, "tx-123", "transfer")

	// Start a step
	_, stepSpan := tracer.StartStep(ctx, "tx-123", "debit", 0)
	stepSpan.End()
	txSpan.End()

	tp.ForceFlush(context.Background())

	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}

	// Find the step span
	var stepSpanData *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "step.execute" {
			stepSpanData = &spans[i]
			break
		}
	}
	if stepSpanData == nil {
		t.Fatal("step.execute span not found")
	}

	// Check attributes
	attrs := stepSpanData.Attributes
	foundStepName := false
	foundStepIndex := false
	for _, attr := range attrs {
		switch string(attr.Key) {
		case "step.name":
			foundStepName = true
			if attr.Value.AsString() != "debit" {
				t.Errorf("expected step.name 'debit', got '%s'", attr.Value.AsString())
			}
		case "step.index":
			foundStepIndex = true
			if attr.Value.AsInt64() != 0 {
				t.Errorf("expected step.index 0, got %d", attr.Value.AsInt64())
			}
		}
	}
	if !foundStepName {
		t.Error("step.name attribute not found")
	}
	if !foundStepIndex {
		t.Error("step.index attribute not found")
	}
}

func TestOTelTracer_SpanSetError(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	defer tp.Shutdown(context.Background())

	tracer := NewOTelTracer(Config{
		ServiceName:    "test-rte",
		TracerProvider: tp,
	})

	ctx := context.Background()
	_, span := tracer.StartTransaction(ctx, "tx-123", "transfer")
	span.SetError(errors.New("test error"))
	span.End()

	tp.ForceFlush(context.Background())

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	s := spans[0]
	if s.Status.Code != codes.Error {
		t.Errorf("expected error status, got %v", s.Status.Code)
	}
}

func TestOTelTracer_SpanSetAttributes(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	defer tp.Shutdown(context.Background())

	tracer := NewOTelTracer(Config{
		ServiceName:    "test-rte",
		TracerProvider: tp,
	})

	ctx := context.Background()
	_, span := tracer.StartTransaction(ctx, "tx-123", "transfer")
	span.SetAttributes(
		attribute.String("custom.key", "custom-value"),
		attribute.Int("custom.count", 42),
	)
	span.End()

	tp.ForceFlush(context.Background())

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	attrs := spans[0].Attributes
	foundCustomKey := false
	foundCustomCount := false
	for _, attr := range attrs {
		switch string(attr.Key) {
		case "custom.key":
			foundCustomKey = true
			if attr.Value.AsString() != "custom-value" {
				t.Errorf("expected custom.key 'custom-value', got '%s'", attr.Value.AsString())
			}
		case "custom.count":
			foundCustomCount = true
			if attr.Value.AsInt64() != 42 {
				t.Errorf("expected custom.count 42, got %d", attr.Value.AsInt64())
			}
		}
	}
	if !foundCustomKey {
		t.Error("custom.key attribute not found")
	}
	if !foundCustomCount {
		t.Error("custom.count attribute not found")
	}
}

func TestOTelTracer_SpanAddEvent(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	defer tp.Shutdown(context.Background())

	tracer := NewOTelTracer(Config{
		ServiceName:    "test-rte",
		TracerProvider: tp,
	})

	ctx := context.Background()
	_, span := tracer.StartTransaction(ctx, "tx-123", "transfer")
	span.AddEvent("step.completed", attribute.String("step.name", "debit"))
	span.End()

	tp.ForceFlush(context.Background())

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	events := spans[0].Events
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	if events[0].Name != "step.completed" {
		t.Errorf("expected event name 'step.completed', got '%s'", events[0].Name)
	}
}

func TestNoopTracer(t *testing.T) {
	tracer := &NoopTracer{}

	ctx := context.Background()
	ctx, txSpan := tracer.StartTransaction(ctx, "tx-123", "transfer")
	txSpan.SetAttributes(attribute.String("key", "value"))
	txSpan.AddEvent("event")
	txSpan.SetError(errors.New("error"))
	txSpan.SetStatus(codes.Error, "error")
	txSpan.End()

	_, stepSpan := tracer.StartStep(ctx, "tx-123", "debit", 0)
	stepSpan.End()

	// NoopTracer should not panic and should work without errors
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.ServiceName != "rte" {
		t.Errorf("expected ServiceName 'rte', got '%s'", cfg.ServiceName)
	}
	if cfg.TracerProvider != nil {
		t.Error("expected TracerProvider to be nil")
	}
}

func TestOTelTracer_SpanSetStatus(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	defer tp.Shutdown(context.Background())

	tracer := NewOTelTracer(Config{
		ServiceName:    "test-rte",
		TracerProvider: tp,
	})

	// Test with Error status (which preserves description)
	ctx := context.Background()
	_, span := tracer.StartTransaction(ctx, "tx-123", "transfer")
	span.SetStatus(codes.Error, "operation failed")
	span.End()

	tp.ForceFlush(context.Background())

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	s := spans[0]
	if s.Status.Code != codes.Error {
		t.Errorf("expected Error status, got %v", s.Status.Code)
	}
	if s.Status.Description != "operation failed" {
		t.Errorf("expected description 'operation failed', got '%s'", s.Status.Description)
	}
}

func TestOTelTracer_SpanSetStatusOk(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	defer tp.Shutdown(context.Background())

	tracer := NewOTelTracer(Config{
		ServiceName:    "test-rte",
		TracerProvider: tp,
	})

	ctx := context.Background()
	_, span := tracer.StartTransaction(ctx, "tx-123", "transfer")
	span.SetStatus(codes.Ok, "")
	span.End()

	tp.ForceFlush(context.Background())

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	s := spans[0]
	if s.Status.Code != codes.Ok {
		t.Errorf("expected Ok status, got %v", s.Status.Code)
	}
}

func TestNoopSpan_AllMethods(t *testing.T) {
	// Test noopSpan directly to ensure coverage
	span := &noopSpan{}

	// Test End - should not panic
	span.End()

	// Test SetError with nil - should not panic
	span.SetError(nil)

	// Test SetError with error - should not panic
	span.SetError(errors.New("test error"))

	// Test SetStatus - should not panic
	span.SetStatus(codes.Ok, "ok")
	span.SetStatus(codes.Error, "error")

	// Test SetAttributes - should not panic
	span.SetAttributes(attribute.String("key", "value"))
	span.SetAttributes(attribute.Int("count", 1), attribute.Bool("flag", true))

	// Test AddEvent - should not panic
	span.AddEvent("event1")
	span.AddEvent("event2", attribute.String("attr", "value"))
}
