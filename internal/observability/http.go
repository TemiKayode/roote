package observability

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
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
		started := time.Now()
		c.Next()
		duration := time.Since(started)
		route := c.FullPath()
		metrics.ObserveRequest(c.Request.Method, route, c.Writer.Status(), duration)
		logger.InfoContext(c.Request.Context(), "http request",
			"request_id", requestID,
			"method", c.Request.Method,
			"route", route,
			"status", c.Writer.Status(),
			"duration_ms", duration.Milliseconds(),
		)
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
