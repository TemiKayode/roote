package store

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestGetMessageReturnsTimelineForOwningAccountOnly(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	messageID := acceptEmail(t, ctx, repository, accountID, "read-detail", "marketing")
	job := EmailJob{ID: messageID, AccountID: accountID}
	if _, err := repository.BeginEmailDelivery(ctx, job, "sendgrid"); err != nil {
		t.Fatal(err)
	}
	if err := repository.CompleteEmailDelivery(ctx, messageID, "sg-read"); err != nil {
		t.Fatal(err)
	}

	detail, err := repository.GetMessage(ctx, accountID, messageID, false)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Status != "processing" || detail.TrafficClass != "marketing" ||
		detail.Recipient.Email != "batch@example.net" {
		t.Fatalf("detail = %+v", detail.MessageSummary)
	}
	if len(detail.Deliveries) != 1 || detail.Deliveries[0].ProviderMessageID != "sg-read" ||
		detail.Deliveries[0].Status != "accepted" {
		t.Fatalf("deliveries = %+v", detail.Deliveries)
	}
	if len(detail.Attempts) != 1 || detail.Attempts[0].Status != "accepted" {
		t.Fatalf("attempts = %+v", detail.Attempts)
	}
	if len(detail.Events) < 2 || detail.Events[0].Type != "accepted" {
		t.Fatalf("events = %+v", detail.Events)
	}
	if _, err := repository.GetMessage(ctx, uuid.New(), messageID, false); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("other account read error = %v, want pgx.ErrNoRows", err)
	}
}

func TestListMessagesFiltersAndPaginates(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	var ids []uuid.UUID
	for _, key := range []string{"page-1", "page-2", "page-3"} {
		ids = append(ids, acceptEmail(t, ctx, repository, accountID, key, ""))
	}
	if err := repository.RecordChannelFailure(ctx, ids[0], "email", "blocked"); err != nil {
		t.Fatal(err)
	}

	first, err := repository.ListMessages(ctx, accountID, MessageFilter{Limit: 2})
	if err != nil || len(first.Messages) != 2 || first.NextCursor == "" ||
		first.Messages[0].ID != ids[2] || first.Messages[1].ID != ids[1] {
		t.Fatalf("first page = %+v, error = %v", first, err)
	}
	second, err := repository.ListMessages(ctx, accountID, MessageFilter{Limit: 2, Cursor: first.NextCursor})
	if err != nil || len(second.Messages) != 1 || second.NextCursor != "" || second.Messages[0].ID != ids[0] {
		t.Fatalf("second page = %+v, error = %v", second, err)
	}
	failed, err := repository.ListMessages(ctx, accountID, MessageFilter{Status: "failed"})
	if err != nil || len(failed.Messages) != 1 || failed.Messages[0].ID != ids[0] {
		t.Fatalf("failed filter = %+v, error = %v", failed, err)
	}
	byRecipient, err := repository.ListMessages(ctx, accountID, MessageFilter{Recipient: "BATCH@example.net"})
	if err != nil || len(byRecipient.Messages) != 3 {
		t.Fatalf("recipient filter = %d messages, error = %v", len(byRecipient.Messages), err)
	}
	if _, err := repository.ListMessages(ctx, accountID, MessageFilter{Cursor: "not-a-cursor"}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("invalid cursor error = %v", err)
	}
}

func TestWebhookEndpointManagement(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	endpointID, err := repository.CreateWebhookEndpoint(
		ctx, accountID, "https://hooks.example.net/manage", []byte("old"), []string{"notification.failed"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.RotateWebhookSecret(ctx, accountID, endpointID, []byte("new")); err != nil {
		t.Fatal(err)
	}
	var secret []byte
	if err := repository.pool.QueryRow(ctx, `SELECT secret_ciphertext FROM webhook_endpoints WHERE id = $1`, endpointID).Scan(&secret); err != nil ||
		string(secret) != "new" {
		t.Fatalf("rotated secret = %q, error = %v", secret, err)
	}
	if err := repository.DeactivateWebhookEndpoint(ctx, accountID, endpointID); err != nil {
		t.Fatal(err)
	}
	endpoints, err := repository.ListWebhookEndpoints(ctx, accountID)
	if err != nil || len(endpoints) != 1 || endpoints[0].Active {
		t.Fatalf("endpoints = %+v, error = %v", endpoints, err)
	}
	if err := repository.RotateWebhookSecret(ctx, accountID, endpointID, []byte("x")); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("rotate inactive endpoint error = %v, want pgx.ErrNoRows", err)
	}
	if err := repository.DeactivateWebhookEndpoint(ctx, uuid.New(), endpointID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("other account deactivate error = %v, want pgx.ErrNoRows", err)
	}
}

func TestSuppressionAndConsentReads(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	if _, err := repository.pool.Exec(ctx, `
		INSERT INTO suppressions (account_id, recipient, channel, reason, source) VALUES
			($1, 'bounced@example.net', 'email', 'hard_bounce', 'sendgrid_webhook'),
			($1, 'complained@example.net', 'email', 'complaint', 'sendgrid_webhook')`, accountID); err != nil {
		t.Fatal(err)
	}
	if err := repository.RecordSMSConsent(ctx, accountID, "+14155552671", "marketing", "form", true); err != nil {
		t.Fatal(err)
	}
	if err := repository.RecordSMSConsent(ctx, accountID, "+14155552671", "marketing", "form", false); err != nil {
		t.Fatal(err)
	}

	page, err := repository.ListSuppressions(ctx, accountID, SuppressionFilter{Channel: "email"})
	if err != nil || len(page.Suppressions) != 2 {
		t.Fatalf("email suppressions = %+v, error = %v", page, err)
	}
	if err := repository.DeleteEmailSuppression(ctx, accountID, "Bounced@example.net"); err != nil {
		t.Fatal(err)
	}
	if err := repository.DeleteEmailSuppression(ctx, accountID, "complained@example.net"); !errors.Is(err, ErrSuppressionProtected) {
		t.Fatalf("complaint delete error = %v, want ErrSuppressionProtected", err)
	}
	if err := repository.DeleteEmailSuppression(ctx, accountID, "nobody@example.net"); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("missing suppression delete error = %v, want pgx.ErrNoRows", err)
	}

	consents, err := repository.ListSMSConsents(ctx, accountID, "+14155552671")
	if err != nil || len(consents) != 1 || consents[0].Scope != "marketing" || consents[0].WithdrawnAt == nil {
		t.Fatalf("consents = %+v, error = %v", consents, err)
	}
	domains, err := repository.ListDomains(ctx, accountID)
	if err != nil || len(domains) != 0 {
		t.Fatalf("domains = %+v, error = %v", domains, err)
	}
}
