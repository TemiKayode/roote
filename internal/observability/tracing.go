package observability

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/TemiKayode/roote"

// SetupTracing exports traces over OTLP/HTTP when OTEL_EXPORTER_OTLP_ENDPOINT or
// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT is set; otherwise spans are no-ops. The exporter reads the
// standard OTEL_* variables, and OTEL_SERVICE_NAME overrides serviceName. The returned
// function flushes buffered spans and must be called before the process exits.
func SetupTracing(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
	}
	serviceResource, err := resource.Merge(
		resource.NewSchemaless(attribute.String("service.name", serviceName)),
		resource.Environment(),
	)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("build trace resource: %w", err), exporter.Shutdown(ctx))
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(serviceResource),
	)
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

// Tracer returns the service's tracer.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// TraceParent returns ctx's W3C traceparent, or "" when ctx carries no sampled span. It is
// stored with queued work so the worker continues the request's trace.
func TraceParent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// WithTraceParent returns ctx continuing the trace described by traceParent.
func WithTraceParent(ctx context.Context, traceParent string) context.Context {
	if traceParent == "" {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": traceParent})
}

// StartTaskSpan starts a consumer span for a queued task, continuing traceParent when set.
func StartTaskSpan(ctx context.Context, taskType, traceParent string) (context.Context, trace.Span) {
	ctx = WithTraceParent(ctx, traceParent)
	return Tracer().Start(ctx, taskType,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.String("messaging.system", "asynq"),
			attribute.String("messaging.operation.name", taskType)),
	)
}

// EndSpan records err on span, if any, and ends it.
func EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// ClientTransport traces outgoing requests through base without sending trace headers, so
// third parties (providers, customer webhook endpoints) never receive internal trace IDs.
func ClientTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return otelhttp.NewTransport(base, otelhttp.WithPropagators(propagation.NewCompositeTextMapPropagator()))
}
