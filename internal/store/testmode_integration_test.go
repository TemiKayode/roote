package store

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestTestModeKeysAndMessagesAreSeparateFromLive(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	hash := sha256.Sum256([]byte("rt_test_integration"))
	if _, err := repository.CreateAPIKey(ctx, accountID, "test key", hash[:], []string{"*"}, nil, true); err != nil {
		t.Fatal(err)
	}
	key, err := repository.AuthenticateAPIKey(ctx, hash[:])
	if err != nil || !key.TestMode {
		t.Fatalf("test key = %+v, error = %v", key, err)
	}
	if _, err := repository.CreateWebhookEndpoint(
		ctx, accountID, "https://hooks.example.net/test-mode", []byte("secret"), []string{"notification.accepted"},
	); err != nil {
		t.Fatal(err)
	}

	notification := Notification{Event: "mode", Channels: []string{"email"}}
	notification.Recipient.Email = "mode@example.net"
	notification.Content.Subject = "Mode"
	notification.Content.Body = "Mode body"
	payload, _ := json.Marshal(notification)
	accept := func(testMode bool) Acceptance {
		return Acceptance{
			AccountID: accountID.String(), IdempotencyKey: "same-key", Notification: notification,
			Payload: payload, TestMode: testMode,
		}
	}
	liveID, liveDuplicate, err := repository.Accept(ctx, accept(false))
	if err != nil || liveDuplicate {
		t.Fatal(err)
	}
	testID, testDuplicate, err := repository.Accept(ctx, accept(true))
	if err != nil || testDuplicate || testID == liveID {
		t.Fatalf("test acceptance = %s duplicate=%t error=%v; the idempotency key must not collide with live",
			testID, testDuplicate, err)
	}

	if _, err := repository.GetMessage(ctx, accountID, testID, false); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("live read of a test message error = %v, want pgx.ErrNoRows", err)
	}
	if _, err := repository.GetMessage(ctx, accountID, testID, true); err != nil {
		t.Fatalf("test read of a test message: %v", err)
	}
	livePage, err := repository.ListMessages(ctx, accountID, MessageFilter{})
	if err != nil || len(livePage.Messages) != 1 || livePage.Messages[0].ID != liveID {
		t.Fatalf("live listing = %+v, error = %v", livePage, err)
	}
	testPage, err := repository.ListMessages(ctx, accountID, MessageFilter{TestMode: true})
	if err != nil || len(testPage.Messages) != 1 || testPage.Messages[0].ID != testID {
		t.Fatalf("test listing = %+v, error = %v", testPage, err)
	}
	job, err := repository.GetEmailJob(ctx, testID)
	if err != nil || !job.TestMode {
		t.Fatalf("test email job = %+v, error = %v", job, err)
	}

	var webhookTestMode bool
	if err := repository.pool.QueryRow(ctx, `
		SELECT (payload->>'test_mode')::boolean FROM webhook_deliveries WHERE source_message_id = $1`, testID,
	).Scan(&webhookTestMode); err != nil || !webhookTestMode {
		t.Fatalf("test webhook payload test_mode = %t, error = %v", webhookTestMode, err)
	}
}
