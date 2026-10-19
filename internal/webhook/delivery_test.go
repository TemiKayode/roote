package webhook

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"github.com/TemiKayode/roote/internal/secret"
	"github.com/TemiKayode/roote/internal/store"
)

type fakeDeliveryRepository struct {
	delivery store.WebhookDelivery
	loadErr  error
	status   string
	code     int
	lastErr  string
	attempts int
}

func (f *fakeDeliveryRepository) GetWebhookDelivery(context.Context, uuid.UUID) (store.WebhookDelivery, error) {
	return f.delivery, f.loadErr
}

func (f *fakeDeliveryRepository) RecordWebhookAttempt(
	_ context.Context, _ uuid.UUID, status string, code int, lastErr string,
) error {
	f.status, f.code, f.lastErr = status, code, lastErr
	f.attempts++
	return nil
}

type fakeDeliverySender struct {
	code int
	err  error
}

func (f fakeDeliverySender) Send(context.Context, store.WebhookDelivery) (int, error) {
	return f.code, f.err
}

func TestSignatureUsesTimestampAndPayload(t *testing.T) {
	key := []byte("webhook signing key")
	body := []byte(`{"id":"evt-1"}`)
	signature := Signature(key, "1700000000", body)
	if !strings.HasPrefix(signature, "sha256=") || len(signature) != len("sha256=")+64 {
		t.Fatalf("signature = %q", signature)
	}
	if signature == Signature(key, "1700000001", body) ||
		signature == Signature(key, "1700000000", []byte(`{"id":"evt-2"}`)) {
		t.Fatal("signature did not authenticate both timestamp and payload")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestSenderSignsExactRequestBody(t *testing.T) {
	encryptionKey := make([]byte, 32)
	signingSecret := []byte("customer signing secret")
	ciphertext, err := secret.Encrypt(encryptionKey, signingSecret)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"id":"evt-1","type":"notification.accepted"}`)
	sender := &Sender{key: encryptionKey}
	sender.client = &http.Client{Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		if request.Header.Get("X-Roote-Event") != "notification.accepted" ||
			request.Header.Get("X-Roote-Signature") != Signature(
				signingSecret, request.Header.Get("X-Roote-Timestamp"), body,
			) {
			t.Fatalf("webhook headers do not sign the exact body: %v", request.Header)
		}
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
			Request:    request,
		}, nil
	})}
	status, err := sender.Send(context.Background(), store.WebhookDelivery{
		URL: "https://hooks.example.net/events", EncryptedSecret: ciphertext,
		EventType: "notification.accepted", Payload: payload,
	})
	if err != nil || status != http.StatusNoContent {
		t.Fatalf("Send() = %d, %v", status, err)
	}
}

func TestSenderRejectsNonHTTPSAndPrivateDestinations(t *testing.T) {
	key := make([]byte, 32)
	ciphertext, err := secret.Encrypt(key, []byte("signing secret"))
	if err != nil {
		t.Fatal(err)
	}
	sender := NewSender(key)
	for _, endpoint := range []string{"http://example.com", "https://127.0.0.1/events"} {
		_, err := sender.Send(context.Background(), store.WebhookDelivery{
			URL: endpoint, EncryptedSecret: ciphertext, Payload: []byte(`{}`),
		})
		if err == nil {
			t.Fatalf("Send(%q) succeeded; expected insecure destination rejection", endpoint)
		}
	}
}

func TestHandlerPersistsRetriesAndExhaustion(t *testing.T) {
	repository := &fakeDeliveryRepository{delivery: store.WebhookDelivery{ID: uuid.New()}}
	handler := NewHandler(repository, fakeDeliverySender{
		code: http.StatusServiceUnavailable,
		err:  errors.New("temporary endpoint failure"),
	})
	err := handler.Handle(context.Background(), repository.delivery.ID, 1, 2)
	if err == nil || repository.status != "pending" || repository.code != http.StatusServiceUnavailable ||
		repository.attempts != 1 {
		t.Fatalf("retry state: error=%v status=%q code=%d attempts=%d",
			err, repository.status, repository.code, repository.attempts)
	}

	err = handler.Handle(context.Background(), repository.delivery.ID, 2, 2)
	if !errors.Is(err, asynq.SkipRetry) || repository.status != "failed" || repository.attempts != 2 {
		t.Fatalf("exhausted state: error=%v status=%q attempts=%d",
			err, repository.status, repository.attempts)
	}
}

func TestHandlerMarksSuccessfulWebhookDelivered(t *testing.T) {
	repository := &fakeDeliveryRepository{delivery: store.WebhookDelivery{ID: uuid.New()}}
	handler := NewHandler(repository, fakeDeliverySender{code: http.StatusNoContent})
	if err := handler.Handle(context.Background(), repository.delivery.ID, 0, 2); err != nil {
		t.Fatal(err)
	}
	if repository.status != "delivered" || repository.attempts != 1 {
		t.Fatalf("success state: status=%q attempts=%d", repository.status, repository.attempts)
	}
}
