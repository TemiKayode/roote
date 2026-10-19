package sms

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// StatusChecker is implemented by transports whose provider can report a sent message's
// current status. The worker uses it to reconcile messages whose delivery receipt never arrived.
type StatusChecker interface {
	// Status returns the provider's status for providerMessageID, normalized to one of
	// accepted, sent, delivered, failed, or undelivered. ok is false for a status that
	// carries no delivery information yet.
	Status(ctx context.Context, providerMessageID string) (status string, ok bool, err error)
}

// NormalizeTwilioStatus maps a Twilio MessageStatus to a delivery event type.
func NormalizeTwilioStatus(status string) (string, bool) {
	switch strings.ToLower(status) {
	case "queued", "accepted", "sending", "scheduled":
		return "accepted", true
	case "sent":
		return "sent", true
	case "delivered", "read":
		return "delivered", true
	case "failed", "canceled":
		return "failed", true
	case "undelivered":
		return "undelivered", true
	default:
		return "", false
	}
}

// NormalizeTelnyxStatus maps a Telnyx event type or "message."-prefixed recipient status to a
// delivery event type.
func NormalizeTelnyxStatus(status string) (string, bool) {
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

// Status fetches a message's status from Twilio.
func (t *TwilioTransport) Status(ctx context.Context, providerMessageID string) (string, bool, error) {
	endpoint := t.messagesEndpoint() + "/" + url.PathEscape(providerMessageID) + ".json"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", false, fmt.Errorf("create Twilio status request: %w", err)
	}
	request.SetBasicAuth(t.accountSID, t.authToken)
	var result struct {
		Status string `json:"status"`
	}
	if err := fetchStatus(t.client, request, &result); err != nil {
		return "", false, fmt.Errorf("fetch Twilio message status: %w", err)
	}
	status, ok := NormalizeTwilioStatus(result.Status)
	return status, ok, nil
}

// Status fetches a message's status from Telnyx.
func (t *TelnyxTransport) Status(ctx context.Context, providerMessageID string) (string, bool, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		t.endpoint+"/"+url.PathEscape(providerMessageID), nil)
	if err != nil {
		return "", false, fmt.Errorf("create Telnyx status request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+t.apiKey)
	var result struct {
		Data struct {
			To []struct {
				Status string `json:"status"`
			} `json:"to"`
		} `json:"data"`
	}
	if err := fetchStatus(t.client, request, &result); err != nil {
		return "", false, fmt.Errorf("fetch Telnyx message status: %w", err)
	}
	if len(result.Data.To) == 0 {
		return "", false, nil
	}
	status, ok := NormalizeTelnyxStatus("message." + result.Data.To[0].Status)
	return status, ok, nil
}

func fetchStatus(client *http.Client, request *http.Request, destination any) error {
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return fmt.Errorf("provider returned HTTP %d", response.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(destination)
}
