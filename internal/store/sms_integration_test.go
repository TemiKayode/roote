package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSMSStoreControlsAndWebhooks(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	accountID := uuid.New()
	repository, err := Open(ctx, databaseURL, accountID.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = repository.pool.Exec(context.Background(), `DELETE FROM accounts WHERE id = $1`, accountID)
		repository.Close()
	})

	phone := "+14155552671"
	messageID := insertTestSMSMessage(t, ctx, repository, accountID, phone)
	budgetMessageID := insertTestSMSMessage(t, ctx, repository, accountID, phone)

	if err := repository.RecordSMSConsent(ctx, accountID, phone, "marketing", "integration-test", true); err != nil {
		t.Fatal(err)
	}
	if allowed, err := repository.HasSMSConsent(ctx, accountID, phone, "marketing"); err != nil || !allowed {
		t.Fatalf("marketing consent = %t, error = %v", allowed, err)
	}
	if allowed, err := repository.HasSMSConsent(ctx, accountID, phone, "transactional"); err != nil || allowed {
		t.Fatalf("transactional consent = %t, error = %v; want no consent", allowed, err)
	}

	if err := repository.ReserveSMSBudget(ctx, messageID, accountID, "US", 0.02, 0.03, 10); err != nil {
		t.Fatal(err)
	}
	if err := repository.ReserveSMSBudget(ctx, budgetMessageID, accountID, "US", 0.02, 0.03, 10); !errors.Is(err, ErrSMSCountryBudget) {
		t.Fatalf("budget error = %v, want ErrSMSCountryBudget", err)
	}
	if err := repository.ReserveSMSConfirmationBudget(ctx, "stop-confirmation", accountID, "US", 0.01, 0.03, 10); err != nil {
		t.Fatal(err)
	}
	if err := repository.ReleaseSMSConfirmationBudget(ctx, "stop-confirmation"); err != nil {
		t.Fatal(err)
	}
	if err := repository.ReserveSMSBudget(ctx, budgetMessageID, accountID, "US", 0.01, 0.03, 1); !errors.Is(err, ErrSMSRateLimit) {
		t.Fatalf("rate limit error = %v, want ErrSMSRateLimit", err)
	}

	job := SMSJob{ID: messageID, AccountID: accountID, Channels: []string{"sms"}, Recipient: phone}
	shouldSend, err := repository.BeginSMSDelivery(ctx, job, "twilio", "US", 0.02)
	if err != nil || !shouldSend {
		t.Fatalf("BeginSMSDelivery() = %t, error = %v", shouldSend, err)
	}
	if err := repository.CompleteSMSDelivery(ctx, messageID, "SM-integration"); err != nil {
		t.Fatal(err)
	}
	event := SMSWebhookEvent{
		ProviderEventID:   "event-delivered",
		ProviderMessageID: "SM-integration",
		Provider:          "twilio",
		MessageID:         messageID,
		Recipient:         phone,
		EventType:         "delivered",
	}
	applied, err := repository.ProcessSMSWebhookEvent(ctx, event)
	if err != nil || !applied {
		t.Fatalf("ProcessSMSWebhookEvent() = %t, error = %v", applied, err)
	}
	applied, err = repository.ProcessSMSWebhookEvent(ctx, event)
	if err != nil || applied {
		t.Fatalf("duplicate SMS event = %t, error = %v; want false, nil", applied, err)
	}
	event.ProviderMessageID = "different-provider-id"
	event.ProviderEventID = "event-mismatch"
	if _, err := repository.ProcessSMSWebhookEvent(ctx, event); err == nil {
		t.Fatal("SMS receipt with a mismatched provider message ID was accepted")
	}

	optedOut, err := repository.ProcessSMSInbound(ctx, accountID, "inbound-stop", phone, " STOP ")
	if err != nil || !optedOut {
		t.Fatalf("ProcessSMSInbound() = %t, error = %v", optedOut, err)
	}
	if allowed, err := repository.HasSMSConsent(ctx, accountID, phone, "marketing"); err != nil || allowed {
		t.Fatalf("post-STOP marketing consent = %t, error = %v", allowed, err)
	}
	if suppressed, err := repository.IsSMSSuppressed(ctx, accountID, phone); err != nil || !suppressed {
		t.Fatalf("post-STOP suppression = %t, error = %v", suppressed, err)
	}
}

func insertTestSMSMessage(
	t *testing.T,
	ctx context.Context,
	repository *Store,
	accountID uuid.UUID,
	phone string,
) uuid.UUID {
	t.Helper()
	messageID := uuid.New()
	notification := Notification{
		Event:    "integration-test",
		Channels: []string{"sms"},
	}
	notification.Recipient.Phone = phone
	notification.Content.Body = "Hello"
	payload, err := json.Marshal(notification)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repository.pool.Exec(ctx, `
		INSERT INTO messages (id, account_id, idempotency_key, event_name, channels, payload, request_hash)
		VALUES ($1, $2, $3, 'integration-test', ARRAY['sms'], $4, $5)`,
		messageID, accountID, messageID.String(), payload, make([]byte, 32),
	)
	if err != nil {
		t.Fatal(err)
	}
	return messageID
}
