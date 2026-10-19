package store

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestOrchestrationAndCustomerWebhookStore(t *testing.T) {
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

	if err := repository.SetNotificationPreference(ctx, accountID, "person@example.net", "email", true); err != nil {
		t.Fatal(err)
	}
	if preferred, err := repository.IsNotificationPreferred(ctx, accountID, "person@example.net", "email"); err != nil || !preferred {
		t.Fatalf("email preference = %t, error = %v", preferred, err)
	}
	if preferred, err := repository.IsNotificationPreferred(ctx, accountID, "person@example.net", "sms"); err != nil || preferred {
		t.Fatalf("unconfigured SMS preference = %t, error = %v", preferred, err)
	}

	endpointID, err := repository.CreateWebhookEndpoint(
		ctx, accountID, "https://hooks.example.net/events", []byte("encrypted-secret"),
		[]string{
			"notification.accepted",
			"notification.failed",
			"notification.partially_delivered",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	notification := Notification{Event: "test", Channels: []string{"email"}}
	notification.Recipient.Email = "person@example.net"
	notification.Content.Subject = "Hello"
	notification.Content.Body = "World"
	payload, err := json.Marshal(notification)
	if err != nil {
		t.Fatal(err)
	}
	messageID, duplicate, err := repository.AcceptNotification(
		ctx, accountID.String(), "orchestration-test", notification, payload,
	)
	if err != nil || duplicate {
		t.Fatalf("AcceptNotification() = %s, duplicate=%t, error=%v", messageID, duplicate, err)
	}

	var queuedID uuid.UUID
	queued, err := repository.RelayOneWebhook(ctx, func(_ context.Context, id uuid.UUID) error {
		queuedID = id
		return nil
	})
	if err != nil || !queued || queuedID == uuid.Nil {
		t.Fatalf("RelayOneWebhook() = %t, id=%s, error=%v", queued, queuedID, err)
	}
	webhookDelivery, err := repository.GetWebhookDelivery(ctx, queuedID)
	if err != nil {
		t.Fatal(err)
	}
	if webhookDelivery.EventType != "notification.accepted" ||
		webhookDelivery.URL != "https://hooks.example.net/events" ||
		string(webhookDelivery.EncryptedSecret) != "encrypted-secret" {
		t.Fatalf("webhook delivery = %+v", webhookDelivery)
	}
	if err := repository.RecordWebhookAttempt(ctx, queuedID, "delivered", 204, ""); err != nil {
		t.Fatal(err)
	}
	var status string
	var attempts int
	err = repository.pool.QueryRow(ctx, `
		SELECT status, attempts FROM webhook_deliveries
		WHERE endpoint_id = $1 AND event_type = 'notification.accepted' AND source_id = $2`,
		endpointID, messageID.String(),
	).Scan(&status, &attempts)
	if err != nil || status != "delivered" || attempts != 1 {
		t.Fatalf("webhook status = %q attempts = %d, error = %v", status, attempts, err)
	}

	failedNotification := Notification{Event: "failed-test", Channels: []string{"email"}}
	failedNotification.Recipient.Email = "failed@example.net"
	failedNotification.Content.Subject = "Failure"
	failedNotification.Content.Body = "Failure test"
	failedPayload, err := json.Marshal(failedNotification)
	if err != nil {
		t.Fatal(err)
	}
	failedMessageID, _, err := repository.AcceptNotification(
		ctx, accountID.String(), "failed-message", failedNotification, failedPayload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.RecordChannelFailure(ctx, failedMessageID, "email", "blocked"); err != nil {
		t.Fatal(err)
	}
	if err := repository.EmitNotificationFailure(ctx, failedMessageID); err != nil {
		t.Fatal(err)
	}
	if err := repository.pool.QueryRow(ctx, `SELECT status FROM messages WHERE id = $1`, failedMessageID).Scan(&status); err != nil ||
		status != "failed" {
		t.Fatalf("failed notification status = %q, error = %v", status, err)
	}

	mixedNotification := Notification{Event: "partial-test", Channels: []string{"email", "sms"}}
	mixedNotification.Recipient.Email = "partial@example.net"
	mixedNotification.Recipient.Phone = "+14155552671"
	mixedNotification.Content.Subject = "Partial"
	mixedNotification.Content.Body = "Partial delivery test"
	mixedPayload, err := json.Marshal(mixedNotification)
	if err != nil {
		t.Fatal(err)
	}
	mixedMessageID, _, err := repository.AcceptNotification(
		ctx, accountID.String(), "partial-message", mixedNotification, mixedPayload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.RecordChannelFailure(ctx, mixedMessageID, "email", "blocked"); err != nil {
		t.Fatal(err)
	}
	mixedSMSJob := SMSJob{
		ID: mixedMessageID, AccountID: accountID, Channels: []string{"email", "sms"},
		Recipient: mixedNotification.Recipient.Phone,
	}
	shouldSend, err := repository.BeginSMSDelivery(ctx, mixedSMSJob, "twilio", "US", 0.02)
	if err != nil || !shouldSend {
		t.Fatalf("BeginSMSDelivery() = %t, error = %v", shouldSend, err)
	}
	if err := repository.CompleteSMSDelivery(ctx, mixedMessageID, "SM-partial"); err != nil {
		t.Fatal(err)
	}
	applied, err := repository.ProcessSMSWebhookEvent(ctx, SMSWebhookEvent{
		ProviderEventID:   "partial-delivered",
		ProviderMessageID: "SM-partial",
		Provider:          "twilio",
		MessageID:         mixedMessageID,
		Recipient:         mixedNotification.Recipient.Phone,
		EventType:         "delivered",
	})
	if err != nil || !applied {
		t.Fatalf("partial delivery receipt = %t, error = %v", applied, err)
	}
	if err := repository.pool.QueryRow(ctx, `SELECT status FROM messages WHERE id = $1`, mixedMessageID).Scan(&status); err != nil ||
		status != "partially_delivered" {
		t.Fatalf("partial notification status = %q, error = %v", status, err)
	}
}
