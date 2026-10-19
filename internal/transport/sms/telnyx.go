package sms

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const telnyxMessagesURL = "https://api.telnyx.com/v2/messages"

type TelnyxTransport struct {
	apiKey   string
	from     string
	callback string
	client   *http.Client
	endpoint string
}

func NewTelnyxTransport(apiKey, from, callback string) (*TelnyxTransport, error) {
	if apiKey == "" || from == "" {
		return nil, fmt.Errorf("Telnyx API key and sender are required")
	}
	transport := &TelnyxTransport{
		apiKey:   apiKey,
		from:     from,
		callback: callback,
		client:   &http.Client{Timeout: 15 * time.Second},
		endpoint: telnyxMessagesURL,
	}
	if callback != "" {
		parsed, err := url.ParseRequestURI(callback)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
			return nil, fmt.Errorf("SMS_STATUS_CALLBACK_URL must be an absolute HTTPS URL")
		}
	}
	return transport, nil
}

func (t *TelnyxTransport) Provider() string {
	return "telnyx"
}

func (t *TelnyxTransport) Send(ctx context.Context, message Message) (string, error) {
	if err := ValidateMessage(message); err != nil {
		return "", err
	}
	var callback string
	if t.callback != "" && message.TrackStatus {
		parsed, err := url.Parse(t.callback)
		if err != nil {
			return "", fmt.Errorf("parse Telnyx status callback URL: %w", err)
		}
		query := parsed.Query()
		query.Set("message_id", message.ID)
		parsed.RawQuery = query.Encode()
		callback = parsed.String()
	}
	payload := struct {
		From       string `json:"from"`
		To         string `json:"to"`
		Text       string `json:"text"`
		WebhookURL string `json:"webhook_url,omitempty"`
	}{
		From: t.from, To: message.To, Text: message.Body, WebhookURL: callback,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode Telnyx request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create Telnyx request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+t.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := t.client.Do(request)
	if err != nil {
		return "", &DeliveryError{Err: fmt.Errorf("send SMS through Telnyx: %w", err), Temporary: true}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return "", &DeliveryError{
			Err:       fmt.Errorf("Telnyx returned HTTP %d", response.StatusCode),
			Temporary: response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500,
		}
	}
	var result struct {
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return "", &DeliveryError{Err: fmt.Errorf("decode Telnyx response: %w", err), Temporary: true}
	}
	if err := ValidateTransportResult(result.Data.ID); err != nil {
		return "", &DeliveryError{Err: err, Temporary: true}
	}
	return result.Data.ID, nil
}
