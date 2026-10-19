package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func openIntegrationStore(t *testing.T) (*Store, uuid.UUID, context.Context) {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	accountID := uuid.New()
	repository, err := Open(ctx, databaseURL, accountID.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = repository.pool.Exec(context.Background(), `DELETE FROM accounts WHERE id = $1`, accountID)
		repository.Close()
	})
	return repository, accountID, ctx
}

func acceptMixedNotification(
	t *testing.T, ctx context.Context, repository *Store, accountID uuid.UUID, key string, routing Routing,
) uuid.UUID {
	t.Helper()
	notification := Notification{Event: "review", Channels: []string{"email", "sms"}, Routing: routing}
	notification.Recipient.Email = "review@example.net"
	notification.Recipient.Phone = "+14155552671"
	notification.Content.Subject = "Review"
	notification.Content.Body = "Review body"
	payload, err := json.Marshal(notification)
	if err != nil {
		t.Fatal(err)
	}
	messageID, _, err := repository.AcceptNotification(ctx, accountID.String(), key, notification, payload)
	if err != nil {
		t.Fatal(err)
	}
	return messageID
}

func messageStatus(t *testing.T, ctx context.Context, repository *Store, messageID uuid.UUID) string {
	t.Helper()
	var status string
	if err := repository.pool.QueryRow(ctx, `SELECT status FROM messages WHERE id = $1`, messageID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func TestWaterfallFallbackDeliveryIsDelivered(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	routing := Routing{Strategy: "waterfall", Order: []string{"email", "sms"}}

	// The first channel fails synchronously and the fallback delivers.
	fallbackID := acceptMixedNotification(t, ctx, repository, accountID, "waterfall-fallback", routing)
	if err := repository.RecordChannelFailure(ctx, fallbackID, "email", "blocked"); err != nil {
		t.Fatal(err)
	}
	job := SMSJob{ID: fallbackID, AccountID: accountID, Recipient: "+14155552671"}
	if _, err := repository.BeginSMSDelivery(ctx, job, "twilio", "US", 0.01); err != nil {
		t.Fatal(err)
	}
	if err := repository.CompleteSMSDelivery(ctx, fallbackID, "SM-waterfall"); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.ProcessSMSWebhookEvent(ctx, SMSWebhookEvent{
		ProviderEventID: "waterfall-delivered", ProviderMessageID: "SM-waterfall", Provider: "twilio",
		MessageID: fallbackID, Recipient: "+14155552671", EventType: "delivered",
	}); err != nil {
		t.Fatal(err)
	}
	if status := messageStatus(t, ctx, repository, fallbackID); status != "delivered" {
		t.Fatalf("waterfall fallback status = %q, want delivered", status)
	}

	// The first channel succeeds and the remaining channel is skipped.
	primaryID := acceptMixedNotification(t, ctx, repository, accountID, "waterfall-primary", routing)
	emailJob := EmailJob{ID: primaryID, AccountID: accountID}
	if _, err := repository.BeginEmailDelivery(ctx, emailJob, "sendgrid"); err != nil {
		t.Fatal(err)
	}
	if err := repository.CompleteEmailDelivery(ctx, primaryID, "sg-primary"); err != nil {
		t.Fatal(err)
	}
	if err := repository.RecordChannelSkipped(ctx, primaryID, "sms"); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.ProcessEmailWebhookEvent(ctx, EmailWebhookEvent{
		ProviderEventID: "waterfall-email-delivered", MessageID: primaryID,
		Recipient: "review@example.net", EventType: "delivered",
	}); err != nil {
		t.Fatal(err)
	}
	if status := messageStatus(t, ctx, repository, primaryID); status != "delivered" {
		t.Fatalf("waterfall primary status = %q, want delivered", status)
	}
}

func TestProviderAcceptedMessageIsNotReportedAsQueued(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	notification := Notification{Event: "accepted", Channels: []string{"email"}}
	notification.Recipient.Email = "accepted@example.net"
	notification.Content.Subject = "Accepted"
	notification.Content.Body = "Accepted body"
	payload, _ := json.Marshal(notification)
	messageID, _, err := repository.AcceptNotification(ctx, accountID.String(), "accepted", notification, payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.BeginEmailDelivery(ctx, EmailJob{ID: messageID, AccountID: accountID}, "sendgrid"); err != nil {
		t.Fatal(err)
	}
	if err := repository.CompleteEmailDelivery(ctx, messageID, "sg-accepted"); err != nil {
		t.Fatal(err)
	}
	if status := messageStatus(t, ctx, repository, messageID); status != "processing" {
		t.Fatalf("provider-accepted status = %q, want processing", status)
	}
}

func TestWebhookRelayDoesNotSpinOnStaleDelivery(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	if _, err := repository.CreateWebhookEndpoint(
		ctx, accountID, "https://hooks.example.net/review", []byte("secret"),
		[]string{"notification.accepted"},
	); err != nil {
		t.Fatal(err)
	}
	notification := Notification{Event: "relay", Channels: []string{"email"}}
	notification.Recipient.Email = "relay@example.net"
	notification.Content.Subject = "Relay"
	notification.Content.Body = "Relay body"
	first, _ := json.Marshal(notification)
	if _, _, err := repository.AcceptNotification(ctx, accountID.String(), "relay-1", notification, first); err != nil {
		t.Fatal(err)
	}
	var staleID uuid.UUID
	if _, err := repository.RelayOneWebhook(ctx, func(_ context.Context, id uuid.UUID) error {
		staleID = id
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// Simulate a delivery that is still pending in a long Asynq retry backoff.
	if _, err := repository.pool.Exec(ctx, `
		UPDATE webhook_deliveries SET queued_at = now() - interval '3 hours' WHERE id = $1`, staleID); err != nil {
		t.Fatal(err)
	}
	notification.Event = "relay-2"
	second, _ := json.Marshal(notification)
	if _, _, err := repository.AcceptNotification(ctx, accountID.String(), "relay-2", notification, second); err != nil {
		t.Fatal(err)
	}
	relayed := map[uuid.UUID]int{}
	for range 3 {
		if _, err := repository.RelayOneWebhook(ctx, func(_ context.Context, id uuid.UUID) error {
			relayed[id]++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if relayed[staleID] != 1 || len(relayed) != 2 {
		t.Fatalf("relayed = %v; stale delivery %s must be re-relayed once and the new delivery must not starve",
			relayed, staleID)
	}
}

func TestEmailWebhookForDeletedMessageIsIgnored(t *testing.T) {
	repository, _, ctx := openIntegrationStore(t)
	_, err := repository.ProcessEmailWebhookEvent(ctx, EmailWebhookEvent{
		ProviderEventID: "missing", MessageID: uuid.New(), Recipient: "gone@example.net", EventType: "delivered",
	})
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("missing message error = %v, want pgx.ErrNoRows", err)
	}
	_, err = repository.ProcessSMSWebhookEvent(ctx, SMSWebhookEvent{
		ProviderEventID: "missing-sms", ProviderMessageID: "SM-missing", Provider: "twilio",
		MessageID: uuid.New(), Recipient: "+14155552671", EventType: "delivered",
	})
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("missing SMS message error = %v, want pgx.ErrNoRows", err)
	}
}
