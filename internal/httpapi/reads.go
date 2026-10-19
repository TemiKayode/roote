package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"net/mail"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	securesecret "github.com/TemiKayode/roote/internal/secret"
	"github.com/TemiKayode/roote/internal/store"
)

// ReadStore serves the read and management endpoints for messages, webhooks, domains,
// suppressions, and consents.
type ReadStore interface {
	GetMessage(context.Context, uuid.UUID, uuid.UUID, bool) (store.MessageDetail, error)
	ListMessages(context.Context, uuid.UUID, store.MessageFilter) (store.MessagePage, error)
	ListWebhookEndpoints(context.Context, uuid.UUID) ([]store.WebhookEndpointSummary, error)
	DeactivateWebhookEndpoint(context.Context, uuid.UUID, uuid.UUID) error
	RotateWebhookSecret(context.Context, uuid.UUID, uuid.UUID, []byte) error
	ListDomains(context.Context, uuid.UUID) ([]store.DomainSummary, error)
	ListSuppressions(context.Context, uuid.UUID, store.SuppressionFilter) (store.SuppressionPage, error)
	DeleteEmailSuppression(context.Context, uuid.UUID, string) error
	ListSMSConsents(context.Context, uuid.UUID, string) ([]store.ConsentRecord, error)
}

var messageStatuses = map[string]struct{}{
	"accepted": {}, "queued": {}, "processing": {}, "delivered": {}, "partially_delivered": {}, "failed": {},
}

// readContext checks that reads are configured and resolves the caller's account. It writes
// the error response and returns false when the request cannot proceed.
func (s *Server) readContext(c *gin.Context) (uuid.UUID, bool) {
	if s.reads == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "read APIs are unavailable"})
		return uuid.Nil, false
	}
	accountID, err := uuid.Parse(s.accountIDFor(c))
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "authenticated account ID is invalid", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not resolve the account"})
		return uuid.Nil, false
	}
	return accountID, true
}

func (s *Server) getMessage(c *gin.Context) {
	accountID, ok := s.readContext(c)
	if !ok {
		return
	}
	messageID, ok := parsePathUUID(c)
	if !ok {
		return
	}
	detail, err := s.reads.GetMessage(c.Request.Context(), accountID, messageID, testMode(c))
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "message not found"})
		return
	}
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not load message", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not load message"})
		return
	}
	c.JSON(http.StatusOK, detail)
}

func (s *Server) listMessages(c *gin.Context) {
	accountID, ok := s.readContext(c)
	if !ok {
		return
	}
	limit, ok := parseLimit(c)
	if !ok {
		return
	}
	filter := store.MessageFilter{
		Status: c.Query("status"), Recipient: strings.TrimSpace(c.Query("recipient")),
		Limit: limit, Cursor: c.Query("cursor"), TestMode: testMode(c),
	}
	if _, valid := messageStatuses[filter.Status]; filter.Status != "" && !valid {
		c.JSON(http.StatusBadRequest, gin.H{"error": "status must be a message status"})
		return
	}
	if filter.Recipient != "" && !validRecipientFilter(filter.Recipient) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "recipient must be an email address or an E.164 phone number"})
		return
	}
	for name, target := range map[string]**time.Time{"since": &filter.Since, "until": &filter.Until} {
		if raw := c.Query(name); raw != "" {
			value, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": name + " must be an RFC 3339 timestamp"})
				return
			}
			*target = &value
		}
	}
	page, err := s.reads.ListMessages(c.Request.Context(), accountID, filter)
	if errors.Is(err, store.ErrInvalidCursor) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cursor is invalid"})
		return
	}
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not list messages", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list messages"})
		return
	}
	c.JSON(http.StatusOK, page)
}

func (s *Server) listCustomerWebhooks(c *gin.Context) {
	accountID, ok := s.readContext(c)
	if !ok {
		return
	}
	endpoints, err := s.reads.ListWebhookEndpoints(c.Request.Context(), accountID)
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not list customer webhooks", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list customer webhooks"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"webhooks": endpoints})
}

func (s *Server) deactivateCustomerWebhook(c *gin.Context) {
	accountID, ok := s.readContext(c)
	if !ok {
		return
	}
	endpointID, ok := parsePathUUID(c)
	if !ok {
		return
	}
	err := s.reads.DeactivateWebhookEndpoint(c.Request.Context(), accountID, endpointID)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "webhook not found"})
		return
	}
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not deactivate customer webhook", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not deactivate customer webhook"})
		return
	}
	s.auditMutation(c, "customer_webhook.deactivated", "webhook_endpoint", endpointID.String(), nil)
	c.Status(http.StatusNoContent)
}

func (s *Server) rotateCustomerWebhookSecret(c *gin.Context) {
	accountID, ok := s.readContext(c)
	if !ok {
		return
	}
	if len(s.webhookEncryptionKey) != 32 {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "customer webhooks require a configured 32-byte WEBHOOK_ENCRYPTION_KEY"})
		return
	}
	endpointID, ok := parsePathUUID(c)
	if !ok {
		return
	}
	signingSecret := make([]byte, 32)
	if _, err := rand.Read(signingSecret); err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not generate customer webhook secret", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not rotate webhook secret"})
		return
	}
	encryptedSecret, err := securesecret.Encrypt(s.webhookEncryptionKey, signingSecret)
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not encrypt customer webhook secret", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not rotate webhook secret"})
		return
	}
	err = s.reads.RotateWebhookSecret(c.Request.Context(), accountID, endpointID, encryptedSecret)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "active webhook not found"})
		return
	}
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not rotate customer webhook secret", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not rotate webhook secret"})
		return
	}
	s.auditMutation(c, "customer_webhook.secret_rotated", "webhook_endpoint", endpointID.String(), nil)
	c.JSON(http.StatusOK, gin.H{
		"id": endpointID.String(), "signing_secret": base64.RawURLEncoding.EncodeToString(signingSecret),
	})
}

func (s *Server) listDomains(c *gin.Context) {
	accountID, ok := s.readContext(c)
	if !ok {
		return
	}
	domains, err := s.reads.ListDomains(c.Request.Context(), accountID)
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not list sending domains", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list sending domains"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"domains": domains})
}

func (s *Server) listSuppressions(c *gin.Context) {
	accountID, ok := s.readContext(c)
	if !ok {
		return
	}
	limit, ok := parseLimit(c)
	if !ok {
		return
	}
	filter := store.SuppressionFilter{
		Channel: c.Query("channel"), Recipient: strings.TrimSpace(c.Query("recipient")),
		Limit: limit, Cursor: c.Query("cursor"),
	}
	if filter.Channel != "" && filter.Channel != "email" && filter.Channel != "sms" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "channel must be email or sms"})
		return
	}
	if filter.Recipient != "" && !validRecipientFilter(filter.Recipient) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "recipient must be an email address or an E.164 phone number"})
		return
	}
	page, err := s.reads.ListSuppressions(c.Request.Context(), accountID, filter)
	if errors.Is(err, store.ErrInvalidCursor) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cursor is invalid"})
		return
	}
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not list suppressions", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list suppressions"})
		return
	}
	c.JSON(http.StatusOK, page)
}

func (s *Server) deleteSuppression(c *gin.Context) {
	accountID, ok := s.readContext(c)
	if !ok {
		return
	}
	if c.Query("channel") != "email" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "only email suppressions can be removed; SMS opt-outs are cleared by new consent",
		})
		return
	}
	recipient := strings.TrimSpace(c.Query("recipient"))
	if address, err := mail.ParseAddress(recipient); err != nil || address.Address != recipient {
		c.JSON(http.StatusBadRequest, gin.H{"error": "recipient must be a valid email address"})
		return
	}
	err := s.reads.DeleteEmailSuppression(c.Request.Context(), accountID, recipient)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		c.JSON(http.StatusNotFound, gin.H{"error": "suppression not found"})
	case errors.Is(err, store.ErrSuppressionProtected):
		c.JSON(http.StatusConflict, gin.H{"error": "complaint suppressions cannot be removed through the API"})
	case err != nil:
		s.logger.ErrorContext(c.Request.Context(), "could not delete suppression", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not delete suppression"})
	default:
		s.auditMutation(c, "suppression.deleted", "suppression", "", map[string]any{"channel": "email"})
		c.Status(http.StatusNoContent)
	}
}

func (s *Server) listSMSConsents(c *gin.Context) {
	accountID, ok := s.readContext(c)
	if !ok {
		return
	}
	phone, err := parseE164(c.Query("phone"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "phone must be a valid E.164 number"})
		return
	}
	records, err := s.reads.ListSMSConsents(c.Request.Context(), accountID, phone)
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not list SMS consents", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list SMS consents"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"phone": phone, "consents": records})
}

func parseLimit(c *gin.Context) (int, bool) {
	raw := c.Query("limit")
	if raw == "" {
		return 0, true
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 || limit > 200 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be an integer between 1 and 200"})
		return 0, false
	}
	return limit, true
}

func validRecipientFilter(recipient string) bool {
	if strings.Contains(recipient, "@") {
		address, err := mail.ParseAddress(recipient)
		return err == nil && address.Address == recipient
	}
	_, err := parseE164(recipient)
	return err == nil
}
