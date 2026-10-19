package email

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/emersion/go-msgauth/dkim"
	"github.com/TemiKayode/roote/internal/emaildomain"
)

func TestComposeAndSignAddsDKIMAndMarketingHeaders(t *testing.T) {
	key, err := emaildomain.GenerateKeyMaterial()
	if err != nil {
		t.Fatal(err)
	}
	message, err := NewMessage(
		"id-1", "alerts@example.org", "person@example.net", "Offer", "Hello",
		"marketing", "https://example.org/unsubscribe", key.Selector, key.PrivatePEM,
	)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := composeAndSign(message)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{
		"DKIM-Signature:",
		"List-Unsubscribe: <https://example.org/unsubscribe>",
		"List-Unsubscribe-Post: List-Unsubscribe=One-Click",
	} {
		if !strings.Contains(string(raw), expected) {
			t.Fatalf("signed message missing %q: %s", expected, raw)
		}
	}
	verifications, err := dkim.VerifyWithOptions(strings.NewReader(string(raw)), &dkim.VerifyOptions{
		LookupTXT: func(name string) ([]string, error) {
			if name != key.Selector+"._domainkey.example.org" {
				return nil, fmt.Errorf("unexpected DNS lookup %q", name)
			}
			return []string{"v=DKIM1; k=rsa; p=" + key.PublicKey}, nil
		},
	})
	if err != nil {
		t.Fatalf("VerifyWithOptions() error = %v", err)
	}
	if len(verifications) != 1 || verifications[0].Err != nil {
		t.Fatalf("DKIM verification = %+v", verifications)
	}
	if strings.Contains(string(raw), string(key.PrivatePEM)) {
		t.Fatal("signed message contains the private key")
	}
}

func TestMarketingMessageRequiresHTTPSUnsubscribeURL(t *testing.T) {
	key, err := emaildomain.GenerateKeyMaterial()
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewMessage(
		"id-1", "alerts@example.org", "person@example.net", "Offer", "Hello",
		"marketing", "http://example.org/unsubscribe", key.Selector, key.PrivatePEM,
	)
	if err == nil {
		t.Fatal("NewMessage() accepted an insecure unsubscribe URL")
	}
}

func TestSendGridTransportSendsAuthenticatedRequest(t *testing.T) {
	var received sendGridRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v3/mail/send" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-api-key" {
			t.Errorf("Authorization = %q", r.Header.Get("Authorization"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		if err := json.Unmarshal(body, &received); err != nil {
			t.Errorf("decode SendGrid request: %v", err)
		}
		w.Header().Set("X-Message-Id", "provider-message-1")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	transport, err := NewSendGridTransport("test-api-key", "transactional-pool", "marketing-pool")
	if err != nil {
		t.Fatal(err)
	}
	transport.endpoint = server.URL + "/v3/mail/send"
	message := Message{
		ID: "internal-id", From: "alerts@example.org", To: "person@example.net",
		Subject: "Hello", Body: "World", TrafficClass: "marketing",
	}
	providerID, err := transport.Send(context.Background(), message)
	if err != nil {
		t.Fatal(err)
	}
	if providerID != "provider-message-1" {
		t.Fatalf("provider message ID = %q", providerID)
	}
	if received.Personalizations[0].CustomArgs["message_id"] != message.ID ||
		received.Personalizations[0].To[0].Email != message.To {
		t.Fatalf("unexpected request payload: %+v", received)
	}
	if received.IPPoolName != "marketing-pool" {
		t.Fatalf("IP pool = %q, want marketing-pool", received.IPPoolName)
	}
}

func TestSendGridTransportClassifiesRetryableResponses(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusTooManyRequests, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte("provider response"))
			}))
			defer server.Close()
			transport, err := NewSendGridTransport("test-api-key", "", "")
			if err != nil {
				t.Fatal(err)
			}
			transport.endpoint = server.URL
			_, err = transport.Send(context.Background(), Message{
				From: "alerts@example.org", To: "person@example.net", Subject: "Hello", Body: "World",
			})
			deliveryError, ok := err.(*DeliveryError)
			if !ok {
				t.Fatalf("error type = %T, want *DeliveryError", err)
			}
			wantTemporary := status == http.StatusTooManyRequests || status >= 500
			if deliveryError.Temporary != wantTemporary {
				t.Fatalf("Temporary = %t, want %t", deliveryError.Temporary, wantTemporary)
			}
		})
	}
}
