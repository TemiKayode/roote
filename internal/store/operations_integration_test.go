package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestOperationsStoreLifecycle(t *testing.T) {
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

	keyHash := sha256.Sum256([]byte("high-entropy-integration-key"))
	keyID, err := repository.CreateAPIKey(
		ctx, accountID, "integration key", keyHash[:], []string{"notifications:write"}, nil, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	authenticated, err := repository.AuthenticateAPIKey(ctx, keyHash[:])
	if err != nil || authenticated.ID != keyID || authenticated.AccountID != accountID ||
		len(authenticated.Scopes) != 1 || authenticated.Scopes[0] != "notifications:write" {
		t.Fatalf("authenticated API key = %+v, error = %v", authenticated, err)
	}
	if err := repository.RevokeAPIKey(ctx, accountID, keyID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.AuthenticateAPIKey(ctx, keyHash[:]); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("revoked API key authentication error = %v, want pgx.ErrNoRows", err)
	}

	if err := repository.SetSenderCapacity(ctx, accountID, "email", "alerts@example.org", 2, 1, 5); err != nil {
		t.Fatal(err)
	}
	for count := 0; count < 2; count++ {
		if err := repository.ReserveSenderCapacity(ctx, accountID, "email", "alerts@example.org", 2, 1, 5); err != nil {
			t.Fatalf("capacity reservation %d: %v", count+1, err)
		}
	}
	if err := repository.ReserveSenderCapacity(ctx, accountID, "email", "alerts@example.org", 2, 1, 5); !errors.Is(err, ErrSenderCapacity) {
		t.Fatalf("third capacity reservation error = %v, want ErrSenderCapacity", err)
	}
	if err := repository.ReleaseSenderCapacity(ctx, accountID, "email", "alerts@example.org"); err != nil {
		t.Fatal(err)
	}
	if err := repository.ReserveSenderCapacity(ctx, accountID, "email", "alerts@example.org", 2, 1, 5); err != nil {
		t.Fatalf("capacity reservation after release: %v", err)
	}

	notification := Notification{Event: "retention-test", Channels: []string{"email"}}
	notification.Recipient.Email = "person@example.org"
	notification.Content.Subject = "Retention"
	notification.Content.Body = "Test"
	payload, err := json.Marshal(notification)
	if err != nil {
		t.Fatal(err)
	}
	messageID, _, err := repository.AcceptNotification(ctx, accountID.String(), "old-message", notification, payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.pool.Exec(ctx,
		`UPDATE messages SET created_at = now() - interval '120 days' WHERE id = $1`, messageID,
	); err != nil {
		t.Fatal(err)
	}
	deleted, err := repository.PurgeExpiredMessages(ctx, accountID, 90)
	if err != nil || deleted != 1 {
		t.Fatalf("retention purge deleted %d messages, error = %v", deleted, err)
	}
	if err := repository.DeleteMessage(ctx, accountID, messageID, uuid.NewString(), &keyID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("deleted message lookup error = %v, want pgx.ErrNoRows", err)
	}
	failedMessageID, _, err := repository.AcceptNotification(
		ctx, accountID.String(), "failed-message", notification, payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.FailMessageRetriesExhausted(ctx, failedMessageID, 25, 25); err != nil {
		t.Fatal(err)
	}
	var failedStatus string
	if err := repository.pool.QueryRow(ctx,
		`SELECT status FROM messages WHERE id = $1`, failedMessageID,
	).Scan(&failedStatus); err != nil {
		t.Fatal(err)
	}
	if failedStatus != "failed" {
		t.Fatalf("retry-exhausted status = %q, want failed", failedStatus)
	}
	summary, err := repository.OperationalSummary(ctx, accountID)
	if err != nil {
		t.Fatal(err)
	}
	if summary.PendingOutbox != 1 || summary.PendingWebhooks != 0 || summary.FailedWebhooks != 0 {
		t.Fatalf("operational summary = %+v, want one durable pending outbox row", summary)
	}
	var auditCount int
	if err := repository.pool.QueryRow(ctx,
		`SELECT count(*) FROM audit_events WHERE account_id = $1`, accountID,
	).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if auditCount < 2 {
		t.Fatalf("audit event count = %d, want key creation and retention purge entries", auditCount)
	}
}
