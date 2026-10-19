package sms

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/TemiKayode/roote/internal/observability"
)

const twilioMessagesURL = "https://api.twilio.com/2010-04-01/Accounts/"

type TwilioTransport struct {
	accountSID string
	authToken  string
	from       string
	callback   string
	client     *http.Client
	baseURL    string
}

func NewTwilioTransport(accountSID, authToken, from, callback string) (*TwilioTransport, error) {
	if accountSID == "" || authToken == "" || from == "" {
		return nil, fmt.Errorf("Twilio account SID, auth token, and sender are required")
	}
	transport := &TwilioTransport{
		accountSID: accountSID,
		authToken:  authToken,
		from:       from,
		callback:   callback,
		client:     &http.Client{Timeout: 15 * time.Second, Transport: observability.ClientTransport(nil)},
		baseURL:    twilioMessagesURL,
	}
	if callback != "" {
		parsed, err := url.ParseRequestURI(callback)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
			return nil, fmt.Errorf("SMS_STATUS_CALLBACK_URL must be an absolute HTTPS URL")
		}
	}
	return transport, nil
}

func (t *TwilioTransport) Provider() string {
	return "twilio"
}

func (t *TwilioTransport) Send(ctx context.Context, message Message) (string, error) {
	if err := ValidateMessage(message); err != nil {
		return "", err
	}
	form := url.Values{
		"To":   {message.To},
		"From": {t.from},
		"Body": {message.Body},
	}
	if t.callback != "" && message.TrackStatus {
		callback, err := url.Parse(t.callback)
		if err != nil {
			return "", fmt.Errorf("parse Twilio status callback URL: %w", err)
		}
		query := callback.Query()
		query.Set("message_id", message.ID)
		callback.RawQuery = query.Encode()
		form.Set("StatusCallback", callback.String())
	}
	endpoint := t.messagesEndpoint() + ".json"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("create Twilio request: %w", err)
	}
	request.SetBasicAuth(t.accountSID, t.authToken)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := t.client.Do(request)
	if err != nil {
		return "", &DeliveryError{Err: fmt.Errorf("send SMS through Twilio: %w", err), Temporary: true}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return "", &DeliveryError{
			Err:       fmt.Errorf("Twilio returned HTTP %d", response.StatusCode),
			Temporary: response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500,
		}
	}
	var result struct {
		SID string `json:"sid"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return "", &DeliveryError{Err: fmt.Errorf("decode Twilio response: %w", err), Temporary: true}
	}
	if err := ValidateTransportResult(result.SID); err != nil {
		return "", &DeliveryError{Err: err, Temporary: true}
	}
	return result.SID, nil
}

func (t *TwilioTransport) messagesEndpoint() string {
	return t.baseURL + url.PathEscape(t.accountSID) + "/Messages"
}
