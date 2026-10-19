package email

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type SendGridTransport struct {
	apiKey          string
	client          *http.Client
	endpoint        string
	transactionPool string
	marketingPool   string
}

func NewSendGridTransport(apiKey, transactionalPool, marketingPool string) (*SendGridTransport, error) {
	if strings.TrimSpace(apiKey) == "" {
		return nil, errors.New("SENDGRID_API_KEY is required")
	}
	return &SendGridTransport{
		apiKey:          apiKey,
		client:          &http.Client{Timeout: 15 * time.Second},
		endpoint:        "https://api.sendgrid.com/v3/mail/send",
		transactionPool: transactionalPool,
		marketingPool:   marketingPool,
	}, nil
}

func (t *SendGridTransport) Provider() string {
	return "sendgrid"
}

func (t *SendGridTransport) Send(ctx context.Context, message Message) (string, error) {
	ipPool := t.transactionPool
	if message.TrafficClass == "marketing" {
		ipPool = t.marketingPool
	}
	payload, err := json.Marshal(sendGridRequest{
		Personalizations: []sendGridPersonalization{{
			To:         []sendGridAddress{{Email: message.To}},
			Subject:    message.Subject,
			CustomArgs: map[string]string{"message_id": message.ID},
		}},
		From:       sendGridAddress{Email: message.From},
		Content:    []sendGridContent{{Type: "text/plain", Value: message.Body}},
		IPPoolName: ipPool,
	})
	if err != nil {
		return "", &DeliveryError{Err: fmt.Errorf("encode SendGrid request: %w", err)}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(payload))
	if err != nil {
		return "", &DeliveryError{Err: fmt.Errorf("create SendGrid request: %w", err)}
	}
	request.Header.Set("Authorization", "Bearer "+t.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := t.client.Do(request)
	if err != nil {
		return "", &DeliveryError{Err: fmt.Errorf("send email through SendGrid: %w", err), Temporary: true}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, 4096))
		if readErr != nil {
			return "", &DeliveryError{Err: fmt.Errorf("read SendGrid error response: %w", readErr), Temporary: true}
		}
		temporary := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		return "", &DeliveryError{
			Err:       fmt.Errorf("SendGrid returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(responseBody))),
			Temporary: temporary,
		}
	}
	return response.Header.Get("X-Message-Id"), nil
}

type sendGridRequest struct {
	Personalizations []sendGridPersonalization `json:"personalizations"`
	From             sendGridAddress           `json:"from"`
	Content          []sendGridContent         `json:"content"`
	IPPoolName       string                    `json:"ip_pool_name,omitempty"`
}

type sendGridPersonalization struct {
	To         []sendGridAddress `json:"to"`
	Subject    string            `json:"subject"`
	CustomArgs map[string]string `json:"custom_args"`
}

type sendGridAddress struct {
	Email string `json:"email"`
}

type sendGridContent struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}
