package observability

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func recordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	previousProvider, previousPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})
	return recorder
}

func TestRequestAndTaskSpansShareOneTrace(t *testing.T) {
	recorder := recordSpans(t)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(Middleware(NewMetrics(), slog.New(slog.NewTextHandler(io.Discard, nil))))
	var stored string
	router.POST("/v1/notifications", func(c *gin.Context) {
		stored = TraceParent(c.Request.Context())
		c.Status(http.StatusAccepted)
	})
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/notifications", nil))
	if stored == "" {
		t.Fatal("request context carried no traceparent to store with the outbox row")
	}

	// Later, in the worker, the task continues the stored trace.
	_, span := StartTaskSpan(context.Background(), "notification:deliver", stored)
	EndSpan(span, nil)

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("ended spans = %d, want 2", len(spans))
	}
	request, task := spans[0], spans[1]
	if request.Name() != "POST /v1/notifications" || task.Name() != "notification:deliver" {
		t.Fatalf("span names = %q, %q", request.Name(), task.Name())
	}
	if task.SpanContext().TraceID() != request.SpanContext().TraceID() ||
		task.Parent().SpanID() != request.SpanContext().SpanID() {
		t.Fatal("task span is not a child of the request span")
	}
}

func TestClientTransportDoesNotSendTraceHeaders(t *testing.T) {
	recordSpans(t)
	var received http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = r.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	ctx, span := Tracer().Start(context.Background(), "parent")
	defer span.End()
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	response, err := (&http.Client{Transport: ClientTransport(nil)}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if received.Get("traceparent") != "" {
		t.Fatalf("third party received traceparent %q", received.Get("traceparent"))
	}
}

func TestTracingIsNoOpWithoutEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	shutdown, err := SetupTracing(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
