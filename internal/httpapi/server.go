package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/mail"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nyaruka/phonenumbers"

	"github.com/TemiKayode/roote/internal/emaildomain"
	"github.com/TemiKayode/roote/internal/observability"
	securesecret "github.com/TemiKayode/roote/internal/secret"
	"github.com/TemiKayode/roote/internal/smswebhook"
	"github.com/TemiKayode/roote/internal/store"
)

var (
	domainPattern = regexp.MustCompile(`^(?i)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$`)
	regionPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
)

type NotificationStore interface {
	AcceptNotification(
		context.Context,
		string,
		string,
		store.Notification,
		[]byte,
	) (uuid.UUID, bool, error)
	Ping(context.Context) error
}

type DomainStore interface {
	CreateDomain(context.Context, string, string, string, string, string, []byte) (store.Domain, error)
	GetDomain(context.Context, string, uuid.UUID) (store.Domain, error)
	RequestDomainVerification(context.Context, string, uuid.UUID) error
	ProcessEmailWebhookEvent(context.Context, store.EmailWebhookEvent) (bool, error)
}

type WebhookVerifier interface {
	Verify([]byte, string, string, time.Time) error
}

type SMSStore interface {
	RecordSMSConsent(context.Context, uuid.UUID, string, string, string, bool) error
	ProcessSMSInbound(context.Context, uuid.UUID, string, string, string) (bool, error)
	ProcessSMSWebhookEvent(context.Context, store.SMSWebhookEvent) (bool, error)
}

type OrchestrationStore interface {
	SetNotificationPreference(context.Context, uuid.UUID, string, string, bool) error
}

type CustomerWebhookStore interface {
	CreateWebhookEndpoint(context.Context, uuid.UUID, string, []byte, []string) (uuid.UUID, error)
}

type APIKeyAuthenticator interface {
	AuthenticateAPIKey(context.Context, []byte) (store.APIKey, error)
}

type AuditStore interface {
	RecordAuditEvent(context.Context, store.AuditEvent) error
	OperationalSummary(context.Context, uuid.UUID) (store.OperationalSummary, error)
	SetSenderCapacity(context.Context, uuid.UUID, string, string, int, int, int) error
	DeleteMessage(context.Context, uuid.UUID, uuid.UUID, string, *uuid.UUID) error
}

type RuntimeFeatures struct {
	AuthMode   string
	APIKeys    APIKeyAuthenticator
	AuditStore AuditStore
	ReadyCheck func(context.Context) error
	Metrics    *observability.Metrics
	Inspector  *asynq.Inspector
	QueueName  string
}

type EmailFeatures struct {
	DomainStore               DomainStore
	EncryptionKey             []byte
	SPFValue                  string
	EnqueueDomainVerification func(context.Context, string, uuid.UUID) error
	WebhookVerifier           WebhookVerifier
}

type SMSFeatures struct {
	Provider                string
	Store                   SMSStore
	EnqueueStopConfirmation func(context.Context, string, string, string) error
	TwilioAuthToken         string
	TwilioInboundWebhookURL string
	SMSStatusCallbackURL    string
	TelnyxWebhookPublicKey  string
}

type CustomerWebhookFeatures struct {
	Store         CustomerWebhookStore
	EncryptionKey []byte
}

type Server struct {
	store                      NotificationStore
	accountID                  string
	logger                     *slog.Logger
	domainStore                DomainStore
	domainEncryptionKey        []byte
	spfValue                   string
	enqueueDomainVerification  func(context.Context, string, uuid.UUID) error
	webhookVerifier            WebhookVerifier
	smsStore                   SMSStore
	smsProvider                string
	enqueueSMSStopConfirmation func(context.Context, string, string, string) error
	twilioAuthToken            string
	twilioInboundWebhookURL    string
	smsStatusCallbackURL       string
	telnyxWebhookPublicKey     string
	orchestrationStore         OrchestrationStore
	webhookStore               CustomerWebhookStore
	webhookEncryptionKey       []byte
	authMode                   string
	apiKeys                    APIKeyAuthenticator
	auditStore                 AuditStore
	readyCheck                 func(context.Context) error
	metrics                    *observability.Metrics
	inspector                  *asynq.Inspector
	queueName                  string
}

func New(logger *slog.Logger, accountID string, notificationStore NotificationStore) *gin.Engine {
	return NewWithAllFeatures(logger, accountID, notificationStore, EmailFeatures{}, SMSFeatures{}, CustomerWebhookFeatures{})
}

func NewWithEmail(
	logger *slog.Logger,
	accountID string,
	notificationStore NotificationStore,
	emailFeatures EmailFeatures,
) *gin.Engine {
	return newRouter(logger, accountID, notificationStore, emailFeatures, SMSFeatures{}, CustomerWebhookFeatures{}, RuntimeFeatures{})
}

func NewWithFeatures(
	logger *slog.Logger,
	accountID string,
	notificationStore NotificationStore,
	emailFeatures EmailFeatures,
	smsFeatures SMSFeatures,
) *gin.Engine {
	return newRouter(logger, accountID, notificationStore, emailFeatures, smsFeatures, CustomerWebhookFeatures{}, RuntimeFeatures{})
}

func NewWithAllFeatures(
	logger *slog.Logger,
	accountID string,
	notificationStore NotificationStore,
	emailFeatures EmailFeatures,
	smsFeatures SMSFeatures,
	webhookFeatures CustomerWebhookFeatures,
) *gin.Engine {
	return NewWithRuntime(logger, accountID, notificationStore, emailFeatures, smsFeatures, webhookFeatures, RuntimeFeatures{})
}

func NewWithRuntime(
	logger *slog.Logger,
	accountID string,
	notificationStore NotificationStore,
	emailFeatures EmailFeatures,
	smsFeatures SMSFeatures,
	webhookFeatures CustomerWebhookFeatures,
	runtime RuntimeFeatures,
) *gin.Engine {
	return newRouter(logger, accountID, notificationStore, emailFeatures, smsFeatures, webhookFeatures, runtime)
}

func newRouter(
	logger *slog.Logger,
	accountID string,
	notificationStore NotificationStore,
	emailFeatures EmailFeatures,
	smsFeatures SMSFeatures,
	webhookFeatures CustomerWebhookFeatures,
	runtime RuntimeFeatures,
) *gin.Engine {
	if runtime.Metrics == nil {
		runtime.Metrics = observability.NewMetrics()
	}
	server := &Server{
		store:                      notificationStore,
		accountID:                  accountID,
		logger:                     logger,
		domainStore:                emailFeatures.DomainStore,
		domainEncryptionKey:        emailFeatures.EncryptionKey,
		spfValue:                   emailFeatures.SPFValue,
		enqueueDomainVerification:  emailFeatures.EnqueueDomainVerification,
		webhookVerifier:            emailFeatures.WebhookVerifier,
		smsStore:                   smsFeatures.Store,
		smsProvider:                smsFeatures.Provider,
		enqueueSMSStopConfirmation: smsFeatures.EnqueueStopConfirmation,
		twilioAuthToken:            smsFeatures.TwilioAuthToken,
		twilioInboundWebhookURL:    smsFeatures.TwilioInboundWebhookURL,
		smsStatusCallbackURL:       smsFeatures.SMSStatusCallbackURL,
		telnyxWebhookPublicKey:     smsFeatures.TelnyxWebhookPublicKey,
		orchestrationStore:         orchestrationStoreFrom(notificationStore),
		webhookStore:               webhookFeatures.Store,
		webhookEncryptionKey:       webhookFeatures.EncryptionKey,
		authMode:                   runtime.AuthMode,
		apiKeys:                    runtime.APIKeys,
		auditStore:                 runtime.AuditStore,
		readyCheck:                 runtime.ReadyCheck,
		metrics:                    runtime.Metrics,
		inspector:                  runtime.Inspector,
		queueName:                  runtime.QueueName,
	}
	router := gin.New()
	router.Use(observability.Middleware(runtime.Metrics, logger))
	router.Use(gin.Recovery())
	router.GET("/healthz", server.health)
	router.GET("/readyz", server.ready)
	router.GET("/metrics", gin.WrapH(observability.MetricsHandler(runtime.Metrics)))
	router.POST("/v1/notifications", server.requireScope("notifications:write"), server.createNotification)
	router.PUT("/v1/preferences", server.requireScope("preferences:write"), server.setNotificationPreference)
	router.POST("/v1/webhooks", server.requireScope("webhooks:write"), server.createCustomerWebhook)
	router.POST("/v1/domains", server.requireScope("domains:write"), server.createDomain)
	router.POST("/v1/domains/:id/verify", server.requireScope("domains:write"), server.verifyDomain)
	router.GET("/v1/domains/:id/records", server.requireScope("domains:read"), server.domainRecords)
	router.PUT("/v1/ops/capacities", server.requireScope("ops:write"), server.setCapacity)
	router.GET("/v1/ops/summary", server.requireScope("ops:read"), server.operationalSummary)
	router.GET("/v1/ops/dead-letters", server.requireScope("ops:read"), server.listDeadLetters)
	router.POST("/v1/ops/dead-letters/:id/retry", server.requireScope("ops:write"), server.retryDeadLetter)
	router.DELETE("/v1/messages/:id", server.requireScope("data:delete"), server.deleteMessage)
	router.POST("/internal/webhooks/email", server.sendGridWebhook)
	router.POST("/v1/consents/sms", server.recordSMSConsent)
	router.POST("/internal/webhooks/sms/twilio", server.twilioSMSWebhook)
	router.POST("/internal/webhooks/sms/telnyx", server.telnyxSMSWebhook)
	return router
}

func orchestrationStoreFrom(notificationStore NotificationStore) OrchestrationStore {
	store, _ := notificationStore.(OrchestrationStore)
	return store
}

func (s *Server) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (s *Server) ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		s.logger.ErrorContext(c.Request.Context(), "readiness check failed", "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
		return
	}
	if s.readyCheck != nil {
		if err := s.readyCheck(ctx); err != nil {
			s.logger.ErrorContext(c.Request.Context(), "readiness check failed", "dependency", "redis", "error", err)
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

func (s *Server) requireScope(scope string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if s.authMode != "api_key" {
			c.Next()
			return
		}
		if s.apiKeys == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "API key authentication is unavailable"})
			c.Abort()
			return
		}
		const prefix = "Bearer "
		authorization := c.GetHeader("Authorization")
		if !strings.HasPrefix(authorization, prefix) {
			c.Header("WWW-Authenticate", "Bearer")
			c.JSON(http.StatusUnauthorized, gin.H{"error": "a valid bearer API key is required"})
			c.Abort()
			return
		}
		token := strings.TrimSpace(strings.TrimPrefix(authorization, prefix))
		if len(token) < 24 || len(token) > 256 || !strings.HasPrefix(token, "rt_live_") {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "a valid bearer API key is required"})
			c.Abort()
			return
		}
		hash := sha256.Sum256([]byte(token))
		key, err := s.apiKeys.AuthenticateAPIKey(c.Request.Context(), hash[:])
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				c.JSON(http.StatusUnauthorized, gin.H{"error": "a valid bearer API key is required"})
			} else {
				s.logger.ErrorContext(c.Request.Context(), "API key authentication failed", "error", err)
				c.JSON(http.StatusServiceUnavailable, gin.H{"error": "API key authentication is temporarily unavailable"})
			}
			c.Abort()
			return
		}
		if !slices.Contains(key.Scopes, scope) && !slices.Contains(key.Scopes, "*") {
			c.JSON(http.StatusForbidden, gin.H{"error": "API key does not have the required scope"})
			c.Abort()
			return
		}
		c.Set("account_id", key.AccountID.String())
		c.Set("api_key_id", key.ID)
		c.Next()
	}
}

func (s *Server) accountIDFor(c *gin.Context) string {
	if value, ok := c.Get("account_id"); ok {
		if accountID, valid := value.(string); valid {
			return accountID
		}
	}
	return s.accountID
}

func (s *Server) setCapacity(c *gin.Context) {
	if s.auditStore == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "capacity management is unavailable"})
		return
	}
	var request struct {
		Channel        string `json:"channel"`
		Sender         string `json:"sender"`
		InitialLimit   int    `json:"initial_daily_limit"`
		DailyIncrement int    `json:"daily_increment"`
		MaximumLimit   int    `json:"maximum_daily_limit"`
	}
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request body must be valid JSON with supported fields only"})
		return
	}
	request.Sender = strings.TrimSpace(request.Sender)
	if (request.Channel != "email" && request.Channel != "sms") ||
		request.Sender == "" || len(request.Sender) > 200 ||
		request.InitialLimit < 1 || request.DailyIncrement < 0 ||
		request.MaximumLimit < request.InitialLimit || request.MaximumLimit > 100_000_000 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "channel, sender, and valid warm-up limits are required"})
		return
	}
	accountID, err := uuid.Parse(s.accountIDFor(c))
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "authenticated account ID is invalid", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not set sender capacity"})
		return
	}
	if err := s.auditStore.SetSenderCapacity(c.Request.Context(), accountID, request.Channel,
		request.Sender, request.InitialLimit, request.DailyIncrement, request.MaximumLimit); err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not set sender capacity", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not set sender capacity"})
		return
	}
	if err := s.auditStore.RecordAuditEvent(c.Request.Context(), store.AuditEvent{
		AccountID: accountID, APIKeyID: apiKeyID(c), Action: "sender_capacity.updated",
		Resource: "sender_capacity", ResourceID: request.Channel + ":" + request.Sender,
		RequestID: observability.RequestID(c),
		Details: map[string]any{
			"initial_daily_limit": request.InitialLimit,
			"daily_increment":     request.DailyIncrement,
			"maximum_daily_limit": request.MaximumLimit,
		},
	}); err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not audit sender capacity change", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not audit sender capacity change"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"channel": request.Channel, "sender": request.Sender,
		"initial_daily_limit": request.InitialLimit,
		"daily_increment":     request.DailyIncrement,
		"maximum_daily_limit": request.MaximumLimit,
	})
}

func (s *Server) operationalSummary(c *gin.Context) {
	if s.auditStore == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "operational summary is unavailable"})
		return
	}
	accountID, err := uuid.Parse(s.accountIDFor(c))
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "authenticated account ID is invalid", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not load operational summary"})
		return
	}
	summary, err := s.auditStore.OperationalSummary(c.Request.Context(), accountID)
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not load operational summary", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not load operational summary"})
		return
	}
	if s.inspector == nil || s.queueName == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "queue inspection is unavailable"})
		return
	}
	queueInfo, err := s.inspector.GetQueueInfo(s.queueName)
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not inspect notification queue", "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "could not inspect notification queue"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"database": summary,
		"queue": gin.H{
			"name": queueInfo.Queue, "pending": queueInfo.Pending, "active": queueInfo.Active,
			"scheduled": queueInfo.Scheduled, "retry": queueInfo.Retry, "archived": queueInfo.Archived,
			"size": queueInfo.Size, "latency_seconds": queueInfo.Latency.Seconds(),
			"paused": queueInfo.Paused, "failed_today": queueInfo.Failed,
		},
	})
}

func (s *Server) listDeadLetters(c *gin.Context) {
	if s.inspector == nil || s.queueName == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "queue inspection is unavailable"})
		return
	}
	limit := 50
	if rawLimit := c.Query("limit"); rawLimit != "" {
		parsedLimit, err := strconv.Atoi(rawLimit)
		if err != nil || parsedLimit < 1 || parsedLimit > 100 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be an integer between 1 and 100"})
			return
		}
		limit = parsedLimit
	}
	tasks, err := s.inspector.ListArchivedTasks(s.queueName, asynq.PageSize(limit))
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not list archived tasks", "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "could not list archived tasks"})
		return
	}
	type archivedTask struct {
		ID           string    `json:"id"`
		Type         string    `json:"type"`
		Retried      int       `json:"retried"`
		MaxRetry     int       `json:"max_retry"`
		LastFailedAt time.Time `json:"last_failed_at"`
	}
	response := make([]archivedTask, 0, len(tasks))
	for _, task := range tasks {
		response = append(response, archivedTask{
			ID: task.ID, Type: task.Type, Retried: task.Retried,
			MaxRetry: task.MaxRetry, LastFailedAt: task.LastFailedAt,
		})
	}
	c.JSON(http.StatusOK, gin.H{"tasks": response})
}

func (s *Server) retryDeadLetter(c *gin.Context) {
	if s.inspector == nil || s.queueName == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "queue inspection is unavailable"})
		return
	}
	taskID := strings.TrimSpace(c.Param("id"))
	if taskID == "" || len(taskID) > 128 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "task ID must be between 1 and 128 characters"})
		return
	}
	if err := s.inspector.RunTask(s.queueName, taskID); err != nil {
		if errors.Is(err, asynq.ErrTaskNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "archived task not found"})
			return
		}
		s.logger.ErrorContext(c.Request.Context(), "could not retry archived task",
			"task_id", taskID, "error", err)
		c.JSON(http.StatusConflict, gin.H{"error": "task could not be retried; it may no longer be archived"})
		return
	}
	accountID, err := uuid.Parse(s.accountIDFor(c))
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "authenticated account ID is invalid", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "task was retried but the retry could not be audited"})
		return
	}
	if s.auditStore != nil {
		if err := s.auditStore.RecordAuditEvent(c.Request.Context(), store.AuditEvent{
			AccountID: accountID, APIKeyID: apiKeyID(c), Action: "queue.archived_task_retried",
			Resource: "queue_task", ResourceID: taskID, RequestID: observability.RequestID(c),
			Details: map[string]any{"queue": s.queueName},
		}); err != nil {
			s.logger.ErrorContext(c.Request.Context(), "could not audit archived task retry",
				"task_id", taskID, "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "task was retried but the retry could not be audited"})
			return
		}
	}
	c.JSON(http.StatusAccepted, gin.H{"task_id": taskID, "status": "queued"})
}

func (s *Server) deleteMessage(c *gin.Context) {
	if s.auditStore == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "message deletion is unavailable"})
		return
	}
	messageID, ok := parsePathUUID(c)
	if !ok {
		return
	}
	accountID, err := uuid.Parse(s.accountIDFor(c))
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "authenticated account ID is invalid", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not delete message"})
		return
	}
	if err := s.auditStore.DeleteMessage(
		c.Request.Context(), accountID, messageID, observability.RequestID(c), apiKeyID(c),
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"error": "message not found"})
			return
		}
		s.logger.ErrorContext(c.Request.Context(), "could not delete message", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not delete message"})
		return
	}
	c.Status(http.StatusNoContent)
}

func apiKeyID(c *gin.Context) *uuid.UUID {
	value, ok := c.Get("api_key_id")
	if !ok {
		return nil
	}
	keyID, valid := value.(uuid.UUID)
	if !valid {
		return nil
	}
	return &keyID
}

func (s *Server) auditMutation(c *gin.Context, action, resource, resourceID string, details any) {
	if s.auditStore == nil {
		return
	}
	accountID, err := uuid.Parse(s.accountIDFor(c))
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not audit API mutation",
			"action", action, "error", err)
		return
	}
	if err := s.auditStore.RecordAuditEvent(c.Request.Context(), store.AuditEvent{
		AccountID: accountID, APIKeyID: apiKeyID(c), Action: action,
		Resource: resource, ResourceID: resourceID,
		RequestID: observability.RequestID(c), Details: details,
	}); err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not persist API mutation audit event",
			"action", action, "resource", resource, "error", err)
	}
}

func (s *Server) createNotification(c *gin.Context) {
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" || len(idempotencyKey) > 128 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Idempotency-Key header is required and must be at most 128 characters"})
		return
	}
	var notification store.Notification
	if err := decodeJSON(c, &notification); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request body must be valid JSON with supported fields only"})
		return
	}
	if message := validateNotification(notification); message != "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": message})
		return
	}
	payload, err := json.Marshal(notification)
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not encode validated notification", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not process notification"})
		return
	}
	messageID, duplicate, err := s.store.AcceptNotification(
		c.Request.Context(), s.accountIDFor(c), idempotencyKey, notification, payload,
	)
	switch {
	case errors.Is(err, store.ErrIdempotencyConflict):
		c.JSON(http.StatusConflict, gin.H{"error": "Idempotency-Key was already used with a different request"})
	case err != nil:
		s.logger.ErrorContext(c.Request.Context(), "could not accept notification", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not accept notification"})
	default:
		if !duplicate && s.auditStore != nil {
			accountID, parseErr := uuid.Parse(s.accountIDFor(c))
			if parseErr != nil {
				s.logger.ErrorContext(c.Request.Context(), "authenticated account ID is invalid", "error", parseErr)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "could not audit notification acceptance"})
				return
			}
			if auditErr := s.auditStore.RecordAuditEvent(c.Request.Context(), store.AuditEvent{
				AccountID: accountID, APIKeyID: apiKeyID(c), Action: "notification.accepted",
				Resource: "message", ResourceID: messageID.String(), RequestID: observability.RequestID(c),
				Details: map[string]any{"event": notification.Event, "channels": notification.Channels, "region": notification.Region},
			}); auditErr != nil {
				s.logger.ErrorContext(c.Request.Context(), "could not audit notification acceptance", "error", auditErr)
				c.JSON(http.StatusInternalServerError, gin.H{"error": "could not audit notification acceptance"})
				return
			}
		}
		c.JSON(http.StatusAccepted, gin.H{
			"message_id": messageID.String(),
			"status":     "accepted",
			"duplicate":  duplicate,
		})
	}
}

func (s *Server) setNotificationPreference(c *gin.Context) {
	if s.orchestrationStore == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "notification preferences are unavailable"})
		return
	}
	var request struct {
		Channel   string `json:"channel"`
		Recipient string `json:"recipient"`
		Enabled   *bool  `json:"enabled"`
	}
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request body must be valid JSON with supported fields only"})
		return
	}
	if request.Enabled == nil || (request.Channel != "email" && request.Channel != "sms") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "channel must be email or sms and enabled must be provided"})
		return
	}
	recipient := strings.TrimSpace(request.Recipient)
	if request.Channel == "email" {
		address, err := mail.ParseAddress(recipient)
		if err != nil || address.Address != recipient || len(recipient) > 254 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "recipient must be a valid email address"})
			return
		}
		recipient = strings.ToLower(recipient)
	} else {
		phone, err := parseE164(recipient)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "recipient must be a valid E.164 phone number"})
			return
		}
		recipient = phone
	}
	accountID, err := uuid.Parse(s.accountIDFor(c))
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "configured account ID is invalid", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not save notification preference"})
		return
	}
	if err := s.orchestrationStore.SetNotificationPreference(
		c.Request.Context(), accountID, recipient, request.Channel, *request.Enabled,
	); err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not save notification preference", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not save notification preference"})
		return
	}
	s.auditMutation(c, "notification_preference.updated", "recipient_preference", "",
		map[string]any{"channel": request.Channel, "enabled": *request.Enabled})
	c.JSON(http.StatusOK, gin.H{
		"channel": request.Channel, "recipient": recipient, "enabled": *request.Enabled,
	})
}

func (s *Server) createCustomerWebhook(c *gin.Context) {
	if s.webhookStore == nil || len(s.webhookEncryptionKey) != 32 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "customer webhooks require a configured 32-byte WEBHOOK_ENCRYPTION_KEY"})
		return
	}
	var request struct {
		URL    string   `json:"url"`
		Events []string `json:"events"`
	}
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request body must be valid JSON with supported fields only"})
		return
	}
	endpointURL := strings.TrimSpace(request.URL)
	parsedURL, err := url.ParseRequestURI(endpointURL)
	if err != nil || parsedURL.Scheme != "https" || parsedURL.Host == "" || parsedURL.User != nil ||
		len(endpointURL) > 2048 || strings.EqualFold(parsedURL.Hostname(), "localhost") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "url must be an absolute HTTPS URL"})
		return
	}
	if ip := net.ParseIP(parsedURL.Hostname()); ip != nil &&
		(ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() || ip.IsMulticast()) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "url must target a public HTTPS host"})
		return
	}
	allowedEvents := map[string]struct{}{
		"notification.accepted":             {},
		"notification.accepted_by_provider": {},
		"notification.delivered":            {},
		"notification.partially_delivered":  {},
		"notification.failed":               {},
	}
	if len(request.Events) == 0 || len(request.Events) > len(allowedEvents) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "events must contain one or more supported notification events"})
		return
	}
	seen := make(map[string]struct{}, len(request.Events))
	for _, event := range request.Events {
		if _, ok := allowedEvents[event]; !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "events may contain only supported notification events"})
			return
		}
		if _, ok := seen[event]; ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "events must not contain duplicates"})
			return
		}
		seen[event] = struct{}{}
	}
	accountID, err := uuid.Parse(s.accountIDFor(c))
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "configured account ID is invalid", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create customer webhook"})
		return
	}
	signingSecret := make([]byte, 32)
	if _, err := rand.Read(signingSecret); err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not generate customer webhook secret", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create customer webhook"})
		return
	}
	encryptedSecret, err := securesecret.Encrypt(s.webhookEncryptionKey, signingSecret)
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not encrypt customer webhook secret", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create customer webhook"})
		return
	}
	webhookID, err := s.webhookStore.CreateWebhookEndpoint(
		c.Request.Context(), accountID, endpointURL, encryptedSecret, request.Events,
	)
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not create customer webhook", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create customer webhook"})
		return
	}
	s.auditMutation(c, "customer_webhook.created", "webhook_endpoint", webhookID.String(),
		map[string]any{"events": request.Events})
	c.JSON(http.StatusCreated, gin.H{
		"id": webhookID.String(), "url": endpointURL, "events": request.Events,
		"signing_secret": base64.RawURLEncoding.EncodeToString(signingSecret),
	})
}

func (s *Server) recordSMSConsent(c *gin.Context) {
	if s.smsStore == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "SMS consent management is unavailable"})
		return
	}
	var request struct {
		Phone     string `json:"phone"`
		Scope     string `json:"scope"`
		Source    string `json:"source"`
		Consented *bool  `json:"consented"`
	}
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request body must be valid JSON with supported fields only"})
		return
	}
	phone, err := parseE164(request.Phone)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "phone must be a valid E.164 number"})
		return
	}
	if request.Scope != "transactional" && request.Scope != "marketing" && request.Scope != "all" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "scope must be transactional, marketing, or all"})
		return
	}
	if request.Consented == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "consented must be provided"})
		return
	}
	if source := strings.TrimSpace(request.Source); source == "" || len(source) > 200 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source must be between 1 and 200 characters"})
		return
	}
	accountID, err := uuid.Parse(s.accountIDFor(c))
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "configured account ID is invalid", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not update SMS consent"})
		return
	}
	if err := s.smsStore.RecordSMSConsent(
		c.Request.Context(), accountID, phone, request.Scope, strings.TrimSpace(request.Source), *request.Consented,
	); err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not record SMS consent", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not update SMS consent"})
		return
	}
	s.auditMutation(c, "sms_consent.updated", "sms_consent", "", map[string]any{
		"scope": request.Scope, "source": strings.TrimSpace(request.Source), "consented": *request.Consented,
	})
	status := "withdrawn"
	if *request.Consented {
		status = "active"
	}
	c.JSON(http.StatusOK, gin.H{"phone": phone, "scope": request.Scope, "consent_status": status})
}

func (s *Server) createDomain(c *gin.Context) {
	if s.domainStore == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "domain management is unavailable"})
		return
	}
	if len(s.domainEncryptionKey) != 32 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "DKIM_ENCRYPTION_KEY must be configured as 32 bytes of base64"})
		return
	}
	var request struct {
		Domain string `json:"domain"`
	}
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request body must be valid JSON with supported fields only"})
		return
	}
	domain := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(request.Domain), "."))
	if !domainPattern.MatchString(domain) || net.ParseIP(domain) != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "domain must be a valid fully-qualified DNS name"})
		return
	}
	material, err := emaildomain.GenerateKeyMaterial()
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not generate domain DKIM key", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create sending domain"})
		return
	}
	ciphertext, err := emaildomain.EncryptPrivateKey(s.domainEncryptionKey, material.PrivatePEM)
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not encrypt domain DKIM key", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create sending domain"})
		return
	}
	spfValue := s.spfValue
	if spfValue == "" {
		spfValue = "v=spf1 include:sendgrid.net ~all"
	}
	domainRecord, err := s.domainStore.CreateDomain(
		c.Request.Context(), s.accountIDFor(c), domain, material.Selector, material.PublicKey, spfValue, ciphertext,
	)
	if err != nil {
		var pgError *pgconn.PgError
		if errors.As(err, &pgError) && pgError.Code == "23505" {
			c.JSON(http.StatusConflict, gin.H{"error": "domain is already registered"})
			return
		}
		s.logger.ErrorContext(c.Request.Context(), "could not save sending domain", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create sending domain"})
		return
	}
	s.auditMutation(c, "sending_domain.created", "domain", domainRecord.ID.String(),
		map[string]any{"domain": domainRecord.Name})
	c.JSON(http.StatusCreated, domainResponse(domainRecord))
}

func (s *Server) domainRecords(c *gin.Context) {
	if s.domainStore == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "domain management is unavailable"})
		return
	}
	domainID, ok := parsePathUUID(c)
	if !ok {
		return
	}
	domainRecord, err := s.domainStore.GetDomain(c.Request.Context(), s.accountIDFor(c), domainID)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "domain not found"})
		return
	}
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not load domain DNS records", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not load domain records"})
		return
	}
	c.JSON(http.StatusOK, domainResponse(domainRecord))
}

func (s *Server) verifyDomain(c *gin.Context) {
	if s.domainStore == nil || s.enqueueDomainVerification == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "domain verification is unavailable"})
		return
	}
	domainID, ok := parsePathUUID(c)
	if !ok {
		return
	}
	if err := s.domainStore.RequestDomainVerification(c.Request.Context(), s.accountIDFor(c), domainID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"error": "domain not found"})
			return
		}
		s.logger.ErrorContext(c.Request.Context(), "could not request domain verification", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not request domain verification"})
		return
	}
	if err := s.enqueueDomainVerification(c.Request.Context(), s.accountIDFor(c), domainID); err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not enqueue domain verification", "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "domain verification could not be queued; retry the request"})
		return
	}
	s.auditMutation(c, "sending_domain.verification_requested", "domain", domainID.String(), nil)
	c.JSON(http.StatusAccepted, gin.H{"domain_id": domainID.String(), "verification_status": "pending"})
}

type sendGridEvent struct {
	Event      string            `json:"event"`
	Type       string            `json:"type"`
	Status     string            `json:"status"`
	Email      string            `json:"email"`
	EventID    string            `json:"sg_event_id"`
	CustomArgs map[string]string `json:"custom_args"`
}

func (s *Server) sendGridWebhook(c *gin.Context) {
	if s.domainStore == nil || s.webhookVerifier == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "email webhook verification is not configured"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "could not read webhook body"})
		return
	}
	if err := s.webhookVerifier.Verify(
		body,
		c.GetHeader("X-Twilio-Email-Event-Webhook-Signature"),
		c.GetHeader("X-Twilio-Email-Event-Webhook-Timestamp"),
		time.Now(),
	); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid email webhook signature"})
		return
	}
	var events []sendGridEvent
	if err := json.Unmarshal(body, &events); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "webhook body must be a JSON array"})
		return
	}
	processed := 0
	for _, event := range events {
		eventType := ""
		switch {
		case event.Event == "spamreport":
			eventType = "complaint"
		case event.Event == "bounce" && event.Type == "bounce" && strings.HasPrefix(event.Status, "5"):
			eventType = "bounce"
		case event.Event == "delivered":
			eventType = "delivered"
		default:
			continue
		}
		messageID, err := uuid.Parse(event.CustomArgs["message_id"])
		if err != nil || event.EventID == "" || event.Email == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "bounce and complaint events require message_id, sg_event_id, and email"})
			return
		}
		applied, err := s.domainStore.ProcessEmailWebhookEvent(c.Request.Context(), store.EmailWebhookEvent{
			ProviderEventID: event.EventID,
			MessageID:       messageID,
			Recipient:       event.Email,
			EventType:       eventType,
		})
		if err != nil {
			s.logger.ErrorContext(c.Request.Context(), "could not process SendGrid email event", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "could not process email webhook"})
			return
		}
		if applied {
			processed++
		}
	}
	c.JSON(http.StatusOK, gin.H{"processed": processed})
}

func (s *Server) twilioSMSWebhook(c *gin.Context) {
	if s.smsStore == nil || s.twilioAuthToken == "" || s.smsProvider != "twilio" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Twilio SMS webhook verification is not configured"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "could not read SMS webhook body"})
		return
	}
	values, err := url.ParseQuery(string(body))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Twilio webhook body must be form encoded"})
		return
	}
	status := values.Get("MessageStatus")
	canonicalURL := s.twilioInboundWebhookURL
	if status != "" {
		canonicalURL = s.smsStatusCallbackURL
	}
	parsedURL, err := url.Parse(canonicalURL)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Twilio webhook URL is not configured"})
		return
	}
	parsedURL.RawQuery = c.Request.URL.RawQuery
	if err := smswebhook.VerifyTwilio(
		s.twilioAuthToken,
		c.GetHeader("X-Twilio-Signature"),
		parsedURL.String(),
		values,
	); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid Twilio SMS webhook signature"})
		return
	}
	if status != "" {
		s.processTwilioSMSStatus(c, values)
		return
	}
	eventID := values.Get("MessageSid")
	sender, err := parseE164(values.Get("From"))
	if eventID == "" || err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "inbound Twilio message requires MessageSid and a valid From number"})
		return
	}
	s.processSMSInbound(c, "twilio", eventID, sender, values.Get("Body"))
}

func (s *Server) processTwilioSMSStatus(c *gin.Context, values url.Values) {
	messageID, err := uuid.Parse(c.Query("message_id"))
	if err != nil || values.Get("MessageSid") == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Twilio status callback requires message_id and MessageSid"})
		return
	}
	recipient, err := parseE164(values.Get("To"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Twilio status callback requires a valid To number"})
		return
	}
	eventType, ok := normalizeTwilioStatus(values.Get("MessageStatus"))
	if !ok {
		c.JSON(http.StatusOK, gin.H{"processed": 0})
		return
	}
	applied, err := s.smsStore.ProcessSMSWebhookEvent(c.Request.Context(), store.SMSWebhookEvent{
		ProviderEventID:   values.Get("MessageSid") + ":" + values.Get("MessageStatus"),
		ProviderMessageID: values.Get("MessageSid"),
		Provider:          "twilio",
		MessageID:         messageID,
		Recipient:         recipient,
		EventType:         eventType,
	})
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not process Twilio SMS status", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not process SMS status"})
		return
	}
	processed := 0
	if applied {
		processed = 1
	}
	c.JSON(http.StatusOK, gin.H{"processed": processed})
}

func (s *Server) telnyxSMSWebhook(c *gin.Context) {
	if s.smsStore == nil || s.telnyxWebhookPublicKey == "" || s.smsProvider != "telnyx" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Telnyx SMS webhook verification is not configured"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "could not read SMS webhook body"})
		return
	}
	if err := smswebhook.VerifyTelnyx(
		s.telnyxWebhookPublicKey,
		c.GetHeader("telnyx-signature-ed25519"),
		c.GetHeader("telnyx-timestamp"),
		body,
		time.Now(),
	); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid Telnyx SMS webhook signature"})
		return
	}
	var event struct {
		Data struct {
			ID        string `json:"id"`
			EventType string `json:"event_type"`
			Payload   struct {
				ID   string `json:"id"`
				From struct {
					PhoneNumber string `json:"phone_number"`
				} `json:"from"`
				To []struct {
					PhoneNumber string `json:"phone_number"`
					Status      string `json:"status"`
				} `json:"to"`
				Text string `json:"text"`
			} `json:"payload"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &event); err != nil || event.Data.ID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Telnyx webhook must contain a valid event"})
		return
	}
	switch event.Data.EventType {
	case "message.received":
		sender, err := parseE164(event.Data.Payload.From.PhoneNumber)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "inbound Telnyx message requires a valid sender"})
			return
		}
		s.processSMSInbound(c, "telnyx", event.Data.ID, sender, event.Data.Payload.Text)
	case "message.sent", "message.delivered", "message.finalized":
		s.processTelnyxSMSStatus(c, event.Data.ID, event.Data.EventType, event.Data.Payload)
	default:
		c.JSON(http.StatusOK, gin.H{"processed": 0})
	}
}

func (s *Server) processTelnyxSMSStatus(c *gin.Context, eventID, eventType string, payload struct {
	ID   string `json:"id"`
	From struct {
		PhoneNumber string `json:"phone_number"`
	} `json:"from"`
	To []struct {
		PhoneNumber string `json:"phone_number"`
		Status      string `json:"status"`
	} `json:"to"`
	Text string `json:"text"`
}) {
	messageID, err := uuid.Parse(c.Query("message_id"))
	if err != nil || len(payload.To) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Telnyx status callback requires message_id and a recipient"})
		return
	}
	recipient, err := parseE164(payload.To[0].PhoneNumber)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Telnyx status callback requires a valid recipient"})
		return
	}
	status := eventType
	if eventType == "message.finalized" {
		status = "message." + strings.ToLower(payload.To[0].Status)
	}
	eventStatus, ok := normalizeTelnyxStatus(status)
	if !ok {
		c.JSON(http.StatusOK, gin.H{"processed": 0})
		return
	}
	applied, err := s.smsStore.ProcessSMSWebhookEvent(c.Request.Context(), store.SMSWebhookEvent{
		ProviderEventID:   eventID,
		ProviderMessageID: payload.ID,
		Provider:          "telnyx",
		MessageID:         messageID,
		Recipient:         recipient,
		EventType:         eventStatus,
	})
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not process Telnyx SMS status", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not process SMS status"})
		return
	}
	processed := 0
	if applied {
		processed = 1
	}
	c.JSON(http.StatusOK, gin.H{"processed": processed})
}

func (s *Server) processSMSInbound(c *gin.Context, provider, eventID, phone, text string) {
	accountID, err := uuid.Parse(s.accountIDFor(c))
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "configured account ID is invalid", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not process inbound SMS"})
		return
	}
	optedOut, err := s.smsStore.ProcessSMSInbound(c.Request.Context(), accountID, eventID, phone, text)
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not process inbound SMS", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not process inbound SMS"})
		return
	}
	if optedOut {
		if s.enqueueSMSStopConfirmation == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "SMS opt-out confirmation queue is unavailable"})
			return
		}
		if err := s.enqueueSMSStopConfirmation(c.Request.Context(), provider, phone, eventID); err != nil {
			s.logger.ErrorContext(c.Request.Context(), "could not enqueue SMS opt-out confirmation", "error", err)
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "SMS opt-out confirmation could not be queued"})
			return
		}
	}
	if provider == "twilio" {
		c.Data(http.StatusOK, "application/xml", []byte("<Response/>"))
		return
	}
	c.JSON(http.StatusOK, gin.H{"processed": 1, "opted_out": optedOut})
}

func normalizeTwilioStatus(status string) (string, bool) {
	switch strings.ToLower(status) {
	case "queued", "accepted", "sending":
		return "accepted", true
	case "sent":
		return "sent", true
	case "delivered":
		return "delivered", true
	case "failed":
		return "failed", true
	case "undelivered":
		return "undelivered", true
	default:
		return "", false
	}
}

func normalizeTelnyxStatus(status string) (string, bool) {
	switch strings.ToLower(status) {
	case "message.sent", "message.queued", "message.accepted", "message.sending", "message.sending_started":
		return "sent", true
	case "message.delivered":
		return "delivered", true
	case "message.failed", "message.undelivered", "message.sending_failed",
		"message.delivery_failed", "message.delivery_unconfirmed":
		return "undelivered", true
	default:
		return "", false
	}
}

func domainResponse(domain store.Domain) gin.H {
	return gin.H{
		"id":                  domain.ID.String(),
		"domain":              domain.Name,
		"verification_status": domain.VerificationStatus,
		"dns_records": emaildomain.Records(
			domain.Name, domain.Selector, domain.PublicKey, domain.SPFValue,
		),
	}
}

func parseE164(value string) (string, error) {
	number, err := phonenumbers.Parse(value, "")
	if err != nil || !phonenumbers.IsValidNumber(number) {
		return "", errors.New("invalid phone number")
	}
	formatted := phonenumbers.Format(number, phonenumbers.E164)
	if formatted != value {
		return "", errors.New("phone number must be canonical E.164")
	}
	return formatted, nil
}

func decodeJSON(c *gin.Context, destination any) error {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request contains trailing data")
	}
	return nil
}

func parsePathUUID(c *gin.Context) (uuid.UUID, bool) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id must be a valid UUID"})
		return uuid.Nil, false
	}
	return id, true
}

func validateNotification(notification store.Notification) string {
	if strings.TrimSpace(notification.Event) == "" || len(notification.Event) > 100 {
		return "event must be between 1 and 100 characters"
	}
	if len(notification.Channels) == 0 || len(notification.Channels) > 2 {
		return "channels must contain email, sms, or both"
	}
	seenChannels := make(map[string]struct{}, len(notification.Channels))
	for _, channel := range notification.Channels {
		if channel != "email" && channel != "sms" {
			return "channels may contain only email and sms"
		}
		if _, exists := seenChannels[channel]; exists {
			return "channels must not contain duplicates"
		}
		seenChannels[channel] = struct{}{}
	}
	if notification.Routing.Strategy != "" &&
		notification.Routing.Strategy != "explicit" &&
		notification.Routing.Strategy != "preference" &&
		notification.Routing.Strategy != "waterfall" {
		return "routing.strategy must be explicit, preference, or waterfall"
	}
	if len(notification.Routing.Order) > 0 {
		if notification.Routing.Strategy != "waterfall" ||
			len(notification.Routing.Order) != len(notification.Channels) {
			return "routing.order must list each requested channel for waterfall routing"
		}
		seenOrder := make(map[string]struct{}, len(notification.Routing.Order))
		for _, channel := range notification.Routing.Order {
			if _, requested := seenChannels[channel]; !requested {
				return "routing.order may contain only requested channels"
			}
			if _, duplicate := seenOrder[channel]; duplicate {
				return "routing.order must not contain duplicates"
			}
			seenOrder[channel] = struct{}{}
		}
	}
	if strings.TrimSpace(notification.Content.Body) == "" || len(notification.Content.Body) > 100_000 {
		return "content.body must be non-empty and at most 100000 bytes"
	}
	if notification.TrafficClass != "" &&
		notification.TrafficClass != "transactional" &&
		notification.TrafficClass != "marketing" {
		return "traffic_class must be transactional or marketing"
	}
	if notification.Region != "" &&
		(len(notification.Region) > 63 || !regionPattern.MatchString(notification.Region)) {
		return "region must be a lowercase region identifier of at most 63 characters"
	}
	if slices.Contains(notification.Channels, "email") {
		address, err := mail.ParseAddress(notification.Recipient.Email)
		if err != nil || address.Address != notification.Recipient.Email || len(notification.Recipient.Email) > 254 {
			return "recipient.email must be a valid email address when email is requested"
		}
		if strings.TrimSpace(notification.Content.Subject) == "" ||
			len(notification.Content.Subject) > 255 ||
			strings.ContainsAny(notification.Content.Subject, "\r\n") {
			return "content.subject must be between 1 and 255 characters when email is requested"
		}
	}
	if slices.Contains(notification.Channels, "sms") {
		if _, err := parseE164(notification.Recipient.Phone); err != nil {
			return "recipient.phone must be a valid E.164 number when sms is requested"
		}
	}
	return ""
}
