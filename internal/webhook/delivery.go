package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/TemiKayode/roote/internal/secret"
	"github.com/TemiKayode/roote/internal/store"
)

type Sender struct {
	client *http.Client
	key    []byte
}

func NewSender(encryptionKey []byte) *Sender {
	return &Sender{
		client: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Transport: &http.Transport{DialContext: safeDialContext},
		},
		key: encryptionKey,
	}
}

func (s *Sender) Send(ctx context.Context, delivery store.WebhookDelivery) (int, error) {
	parsed, err := url.ParseRequestURI(delivery.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		return 0, fmt.Errorf("webhook endpoint must be an absolute HTTPS URL")
	}
	secretBytes, err := secret.Decrypt(s.key, delivery.EncryptedSecret)
	if err != nil {
		return 0, fmt.Errorf("decrypt webhook signing secret: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, delivery.URL, bytes.NewReader(delivery.Payload))
	if err != nil {
		return 0, fmt.Errorf("create webhook request: %w", err)
	}
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Roote-Event", delivery.EventType)
	request.Header.Set("X-Roote-Timestamp", timestamp)
	request.Header.Set("X-Roote-Signature", Signature(secretBytes, timestamp, delivery.Payload))

	response, err := s.client.Do(request)
	if err != nil {
		return 0, fmt.Errorf("send webhook request to %q failed (%T)", parsed.Hostname(), err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return response.StatusCode, fmt.Errorf("webhook endpoint returned HTTP %d", response.StatusCode)
	}
	return response.StatusCode, nil
}

func Signature(signingSecret []byte, timestamp string, payload []byte) string {
	mac := hmac.New(sha256.New, signingSecret)
	_, _ = mac.Write([]byte(timestamp + "."))
	_, _ = mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func safeDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("parse webhook destination: %w", err)
	}
	if strings.EqualFold(host, "localhost") {
		return nil, fmt.Errorf("webhook destination resolves to a non-public address")
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve webhook destination: %w", err)
	}
	for _, candidate := range ips {
		ip := candidate.IP
		if ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() ||
			ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
			continue
		}
		conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(
			ctx, network, net.JoinHostPort(ip.String(), port),
		)
		if err == nil {
			return conn, nil
		}
	}
	return nil, fmt.Errorf("webhook destination has no reachable public address")
}
