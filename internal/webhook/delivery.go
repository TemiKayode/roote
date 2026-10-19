package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/TemiKayode/roote/internal/secret"
	"github.com/TemiKayode/roote/internal/store"

	"github.com/TemiKayode/roote/internal/observability"
)

type Sender struct {
	client  *http.Client
	keyring *secret.Keyring
	// keyErr is reported on every send when the configured keys are unusable.
	keyErr error
}

// NewSender returns a sender that decrypts signing secrets with encryptionKey or, during a
// key rotation, any of the previous keys.
func NewSender(encryptionKey []byte, previous ...[]byte) *Sender {
	keyring, keyErr := secret.NewKeyring(encryptionKey, previous...)
	return &Sender{
		keyring: keyring,
		keyErr:  keyErr,
		client: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Transport: observability.ClientTransport(&http.Transport{DialContext: safeDialContext}),
		},
	}
}

func (s *Sender) Send(ctx context.Context, delivery store.WebhookDelivery) (int, error) {
	parsed, err := url.ParseRequestURI(delivery.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		return 0, fmt.Errorf("webhook endpoint must be an absolute HTTPS URL")
	}
	if s.keyErr != nil {
		return 0, fmt.Errorf("webhook encryption key: %w", s.keyErr)
	}
	secretBytes, err := s.keyring.Decrypt(delivery.EncryptedSecret)
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
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
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
		ip, ok := netip.AddrFromSlice(candidate.IP)
		if !ok || !IsPublicAddress(ip) {
			continue
		}
		conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(
			ctx, network, net.JoinHostPort(ip.Unmap().String(), port),
		)
		if err == nil {
			return conn, nil
		}
	}
	return nil, fmt.Errorf("webhook destination has no reachable public address")
}

// nonPublicPrefixes are special-purpose ranges (RFC 6890 and successors) that a customer
// webhook must never reach, in addition to the private, loopback, link-local, multicast,
// and unspecified ranges checked by IsPublicAddress.
var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
}

// IsPublicAddress reports whether ip is a globally routable unicast address.
func IsPublicAddress(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() || ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}
