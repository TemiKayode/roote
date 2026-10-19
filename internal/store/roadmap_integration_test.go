package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
)

func acceptEmail(t *testing.T, ctx context.Context, repository *Store, accountID uuid.UUID, key, trafficClass string) uuid.UUID {
	t.Helper()
	notification := Notification{Event: key, Channels: []string{"email"}, TrafficClass: trafficClass}
	notification.Recipient.Email = "batch@example.net"
	notification.Content.Subject = "Batch"
	notification.Content.Body = "Batch body"
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

func TestOutboxBatchRelaysInOrderWithTrafficClass(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	// Clear rows left by other tests in the shared database so this test sees only its own.
	drainOutbox(t, ctx, repository)
	first := acceptEmail(t, ctx, repository, accountID, "batch-1", "")
	second := acceptEmail(t, ctx, repository, accountID, "batch-2", "marketing")

	var order []uuid.UUID
	classes := map[uuid.UUID]string{}
	relayed, err := repository.RelayOutboxBatch(ctx, func(_ context.Context, id uuid.UUID, trafficClass string) error {
		order = append(order, id)
		classes[id] = trafficClass
		return nil
	})
	if err != nil || relayed != 2 || len(order) != 2 || order[0] != first || order[1] != second {
		t.Fatalf("RelayOutboxBatch() = %d, order=%v, error=%v", relayed, order, err)
	}
	if classes[first] != "" || classes[second] != "marketing" {
		t.Fatalf("traffic classes = %v", classes)
	}
	again, err := repository.RelayOutboxBatch(ctx, func(context.Context, uuid.UUID, string) error {
		t.Fatal("published rows were relayed again")
		return nil
	})
	if err != nil || again != 0 {
		t.Fatalf("second relay = %d, error = %v", again, err)
	}
}

func TestOutboxBatchMarksOnlyEnqueuedRowsOnFailure(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	drainOutbox(t, ctx, repository)
	first := acceptEmail(t, ctx, repository, accountID, "partial-1", "")
	second := acceptEmail(t, ctx, repository, accountID, "partial-2", "")
	relayed, err := repository.RelayOutboxBatch(ctx, func(_ context.Context, id uuid.UUID, _ string) error {
		if id == second {
			return fmt.Errorf("redis unavailable")
		}
		return nil
	})
	if err == nil || relayed != 1 {
		t.Fatalf("partial relay = %d, error = %v", relayed, err)
	}
	var firstPublished, secondPublished bool
	if err := repository.pool.QueryRow(ctx, `
		SELECT
			(SELECT published_at IS NOT NULL FROM outbox WHERE message_id = $1),
			(SELECT published_at IS NOT NULL FROM outbox WHERE message_id = $2)`, first, second,
	).Scan(&firstPublished, &secondPublished); err != nil {
		t.Fatal(err)
	}
	if !firstPublished || secondPublished {
		t.Fatalf("published: first=%t second=%t", firstPublished, secondPublished)
	}
}

func TestOutboxRelayWakesOnNotify(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	drainOutbox(t, ctx, repository)
	relayCtx, stop := context.WithCancel(ctx)
	defer stop()
	enqueued := make(chan uuid.UUID, 10)
	go repository.RunOutboxRelay(relayCtx, func(_ context.Context, id uuid.UUID, _ string) error {
		enqueued <- id
		return nil
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	// Let the relay drain and start waiting, then accept a message well inside the poll interval.
	time.Sleep(300 * time.Millisecond)
	started := time.Now()
	messageID := acceptEmail(t, ctx, repository, accountID, "notify-wake", "")
	for {
		select {
		case id := <-enqueued:
			if id != messageID {
				continue
			}
			if elapsed := time.Since(started); elapsed > relayPollInterval/2 {
				t.Fatalf("relay took %s; it should wake on NOTIFY, not the %s poll", elapsed, relayPollInterval)
			}
			return
		case <-time.After(relayPollInterval * 2):
			t.Fatal("relay never enqueued the accepted message")
		}
	}
}

func TestAuditedAcceptWritesAuditInSameTransaction(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	notification := Notification{Event: "audited", Channels: []string{"email"}}
	notification.Recipient.Email = "audited@example.net"
	notification.Content.Subject = "Audited"
	notification.Content.Body = "Audited body"
	payload, _ := json.Marshal(notification)
	audit := &AuditEvent{AccountID: accountID, Action: "notification.accepted", Resource: "message"}
	messageID, _, err := repository.AcceptAuditedNotification(ctx, accountID.String(), "audited", notification, payload, audit)
	if err != nil {
		t.Fatal(err)
	}
	if _, duplicate, err := repository.AcceptAuditedNotification(
		ctx, accountID.String(), "audited", notification, payload, audit,
	); err != nil || !duplicate {
		t.Fatalf("replay duplicate=%t error=%v", duplicate, err)
	}
	var count int
	if err := repository.pool.QueryRow(ctx, `
		SELECT count(*) FROM audit_events
		WHERE account_id = $1 AND action = 'notification.accepted' AND resource_id = $2`,
		accountID, messageID.String(),
	).Scan(&count); err != nil || count != 1 {
		t.Fatalf("acceptance audit events = %d, error = %v; want exactly one", count, err)
	}
}

func TestReverifyingVerifiedDomainKeepsItVerifiedUntilFinalFailure(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	domain, err := repository.CreateDomain(ctx, accountID.String(), "reverify.example.org",
		"notify-test", "public", "v=spf1 include:sendgrid.net ~all", []byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	status := func() string {
		t.Helper()
		var value string
		if err := repository.pool.QueryRow(ctx, `SELECT verification_status FROM domains WHERE id = $1`, domain.ID).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	if err := repository.SetDomainVerificationResult(ctx, domain.ID, true, false, ""); err != nil {
		t.Fatal(err)
	}
	if err := repository.RequestDomainVerification(ctx, accountID.String(), domain.ID); err != nil {
		t.Fatal(err)
	}
	if got := status(); got != "verified" {
		t.Fatalf("status after re-verification request = %q, want verified", got)
	}
	if err := repository.SetDomainVerificationResult(ctx, domain.ID, false, false, "SPF missing"); err != nil {
		t.Fatal(err)
	}
	if got := status(); got != "verified" {
		t.Fatalf("status after retryable failure = %q, want verified", got)
	}
	if err := repository.SetDomainVerificationResult(ctx, domain.ID, false, true, "SPF missing"); err != nil {
		t.Fatal(err)
	}
	if got := status(); got != "failed" {
		t.Fatalf("status after final failure = %q, want failed", got)
	}
}

func TestPurgeExpiredRateWindows(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	if _, err := repository.pool.Exec(ctx, `
		INSERT INTO sms_rate_limits (account_id, window_start, send_count) VALUES
			($1, now() - interval '2 days', 5),
			($1, date_trunc('minute', now()), 1)`, accountID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.PurgeExpiredRateWindows(ctx); err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err := repository.pool.QueryRow(ctx, `SELECT count(*) FROM sms_rate_limits WHERE account_id = $1`, accountID).Scan(&remaining); err != nil || remaining != 1 {
		t.Fatalf("rate windows remaining = %d, error = %v; want only the current one", remaining, err)
	}
}

func drainOutbox(t *testing.T, ctx context.Context, repository *Store) {
	t.Helper()
	if _, err := repository.pool.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE published_at IS NULL`); err != nil {
		t.Fatal(err)
	}
}
