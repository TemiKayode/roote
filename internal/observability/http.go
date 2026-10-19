package observability

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const RequestIDHeader = "X-Request-ID"

func Middleware(metrics *Metrics, logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID := c.GetHeader(RequestIDHeader)
		parsedID, err := uuid.Parse(requestID)
		if err != nil {
			requestID = uuid.NewString()
		} else {
			requestID = parsedID.String()
		}
		c.Set("request_id", requestID)
		c.Writer.Header().Set(RequestIDHeader, requestID)
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		ctx := otel.GetTextMapPropagator().Extract(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))
		ctx, span := Tracer().Start(ctx, c.Request.Method+" "+route,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.request.method", c.Request.Method),
				attribute.String("http.route", route),
				attribute.String("roote.request_id", requestID),
			),
		)
		c.Request = c.Request.WithContext(ctx)
		started := time.Now()
		c.Next()
		duration := time.Since(started)
		status := c.Writer.Status()
		span.SetAttributes(attribute.Int("http.response.status_code", status))
		if status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
		span.End()
		metrics.ObserveRequest(c.Request.Method, c.FullPath(), status, duration)
		attributes := []any{
			"request_id", requestID,
			"method", c.Request.Method,
			"route", c.FullPath(),
			"status", status,
			"duration_ms", duration.Milliseconds(),
		}
		if spanContext := span.SpanContext(); spanContext.IsValid() {
			attributes = append(attributes, "trace_id", spanContext.TraceID().String())
		}
		logger.InfoContext(c.Request.Context(), "http request", attributes...)
	}
}

func RequestID(ctx *gin.Context) string {
	value, ok := ctx.Get("request_id")
	if !ok {
		return ""
	}
	requestID, _ := value.(string)
	return requestID
}

func MetricsHandler(metrics *Metrics) http.HandlerFunc {
	return metrics.Handler
}
