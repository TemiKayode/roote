package httpapi

import (
	"context"
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
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nyaruka/phonenumbers"

	"github.com/TemiKayode/roote/internal/emaildomain"
	"github.com/TemiKayode/roote/internal/smswebhook"
	"github.com/TemiKayode/roote/internal/store"
)

var (
	domainPattern = regexp.MustCompile(`^(?i)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$`)
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
}

func New(logger *slog.Logger, accountID string, notificationStore NotificationStore) *gin.Engine {
	return NewWithEmail(logger, accountID, notificationStore, EmailFeatures{})
}

func NewWithEmail(
	logger *slog.Logger,
	accountID string,
	notificationStore NotificationStore,
	emailFeatures EmailFeatures,
) *gin.Engine {
	return newRouter(logger, accountID, notificationStore, emailFeatures, SMSFeatures{})
}

func NewWithFeatures(
	logger *slog.Logger,
	accountID string,
	notificationStore NotificationStore,
	emailFeatures EmailFeatures,
	smsFeatures SMSFeatures,
) *gin.Engine {
	return newRouter(logger, accountID, notificationStore, emailFeatures, smsFeatures)
}

func newRouter(
	logger *slog.Logger,
	accountID string,
	notificationStore NotificationStore,
	emailFeatures EmailFeatures,
	smsFeatures SMSFeatures,
) *gin.Engine {
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
	}
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/healthz", server.health)
	router.GET("/readyz", server.ready)
	router.POST("/v1/notifications", server.createNotification)
	router.POST("/v1/domains", server.createDomain)
	router.POST("/v1/domains/:id/verify", server.verifyDomain)
	router.GET("/v1/domains/:id/records", server.domainRecords)
	router.POST("/internal/webhooks/email", server.sendGridWebhook)
	router.POST("/v1/consents/sms", server.recordSMSConsent)
	router.POST("/internal/webhooks/sms/twilio", server.twilioSMSWebhook)
	router.POST("/internal/webhooks/sms/telnyx", server.telnyxSMSWebhook)
	return router
}

func (s *Server) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (s *Server) ready(c *gin.Context) {
	if err := s.store.Ping(c.Request.Context()); err != nil {
		s.logger.ErrorContext(c.Request.Context(), "readiness check failed", "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
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
		c.Request.Context(), s.accountID, idempotencyKey, notification, payload,
	)
	switch {
	case errors.Is(err, store.ErrIdempotencyConflict):
		c.JSON(http.StatusConflict, gin.H{"error": "Idempotency-Key was already used with a different request"})
	case err != nil:
		s.logger.ErrorContext(c.Request.Context(), "could not accept notification", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not accept notification"})
	default:
		c.JSON(http.StatusAccepted, gin.H{
			"message_id": messageID.String(),
			"status":     "accepted",
			"duplicate":  duplicate,
		})
	}
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
	accountID, err := uuid.Parse(s.accountID)
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
		c.Request.Context(), s.accountID, domain, material.Selector, material.PublicKey, spfValue, ciphertext,
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
	domainRecord, err := s.domainStore.GetDomain(c.Request.Context(), s.accountID, domainID)
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
	if err := s.domainStore.RequestDomainVerification(c.Request.Context(), s.accountID, domainID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"error": "domain not found"})
			return
		}
		s.logger.ErrorContext(c.Request.Context(), "could not request domain verification", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not request domain verification"})
		return
	}
	if err := s.enqueueDomainVerification(c.Request.Context(), s.accountID, domainID); err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not enqueue domain verification", "error", err)
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "domain verification could not be queued; retry the request"})
		return
	}
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
	accountID, err := uuid.Parse(s.accountID)
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
	if strings.TrimSpace(notification.Content.Body) == "" || len(notification.Content.Body) > 100_000 {
		return "content.body must be non-empty and at most 100000 bytes"
	}
	if notification.TrafficClass != "" &&
		notification.TrafficClass != "transactional" &&
		notification.TrafficClass != "marketing" {
		return "traffic_class must be transactional or marketing"
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
