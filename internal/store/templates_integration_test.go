package store

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/TemiKayode/roote/internal/rendering"
)

func TestTemplateVersioningAndPublishing(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	if _, err := repository.CreateTemplate(ctx, accountID, "welcome", "Welcome"); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.CreateTemplate(ctx, accountID, "welcome", "Again"); !errors.Is(err, ErrTemplateExists) {
		t.Fatalf("duplicate key error = %v, want ErrTemplateExists", err)
	}
	if _, err := repository.GetTemplateVersion(ctx, accountID, "welcome", nil); !errors.Is(err, ErrTemplateNotPublished) {
		t.Fatalf("unpublished template error = %v", err)
	}
	contents := map[string]rendering.Content{"en": {SMSText: "v{{.n}}"}}
	for want := 1; want <= 2; want++ {
		version, err := repository.CreateTemplateVersion(ctx, accountID, "welcome", "en", contents)
		if err != nil || version != want {
			t.Fatalf("CreateTemplateVersion() = %d, %v; want %d", version, err, want)
		}
	}
	if err := repository.PublishTemplateVersion(ctx, accountID, "welcome", 3); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("publish missing version error = %v", err)
	}
	if err := repository.PublishTemplateVersion(ctx, accountID, "welcome", 1); err != nil {
		t.Fatal(err)
	}
	active, err := repository.GetTemplateVersion(ctx, accountID, "welcome", nil)
	if err != nil || active.Version != 1 || active.Contents["en"].SMSText != "v{{.n}}" {
		t.Fatalf("active version = %+v, error = %v", active, err)
	}
	detail, err := repository.GetTemplate(ctx, accountID, "welcome")
	if err != nil || len(detail.Versions) != 2 || detail.Versions[0].Version != 2 || *detail.ActiveVersion != 1 {
		t.Fatalf("template detail = %+v, error = %v", detail, err)
	}
	if _, err := repository.CreateTemplateVersion(ctx, accountID, "missing", "en", contents); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("version for missing template error = %v", err)
	}
}

func TestRenderedContentReachesDeliveryJobs(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	notification := Notification{Event: "rendered", Channels: []string{"email", "sms"}}
	notification.Recipient.Email = "rendered@example.net"
	notification.Recipient.Phone = "+14155552671"
	notification.Content.Subject = "Subject"
	notification.Content.Body = "Email text"
	notification.Content.HTML = "<p>Email</p>"
	notification.Content.SMS = "SMS text"
	payload, _ := json.Marshal(notification)
	messageID, _, err := repository.AcceptNotification(ctx, accountID.String(), "rendered", notification, payload)
	if err != nil {
		t.Fatal(err)
	}
	emailJob, err := repository.GetEmailJob(ctx, messageID)
	if err != nil || emailJob.Body != "Email text" || emailJob.HTML != "<p>Email</p>" {
		t.Fatalf("email job = %+v, error = %v", emailJob, err)
	}
	smsJob, err := repository.GetSMSJob(ctx, messageID)
	if err != nil || smsJob.Body != "SMS text" {
		t.Fatalf("SMS job body = %q, error = %v", smsJob.Body, err)
	}
}
