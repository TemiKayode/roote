package store

import (
	"testing"
)

func TestClaimUnconfirmedSMSThrottlesAndSkipsFinalStatuses(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	stale := acceptEmail(t, ctx, repository, accountID, "reconcile-stale", "")
	fresh := acceptEmail(t, ctx, repository, accountID, "reconcile-fresh", "")
	delivered := acceptEmail(t, ctx, repository, accountID, "reconcile-delivered", "")
	if _, err := repository.pool.Exec(ctx, `
		INSERT INTO sms_deliveries (message_id, provider, provider_message_id, recipient, country_code, estimated_cost_usd, status, updated_at)
		VALUES
			($1, 'twilio', 'SM-stale', '+14155552671', 'US', 0.01, 'sent', now() - interval '1 hour'),
			($2, 'twilio', 'SM-fresh', '+14155552671', 'US', 0.01, 'accepted', now()),
			($3, 'twilio', 'SM-done', '+14155552671', 'US', 0.01, 'delivered', now() - interval '1 hour')`,
		stale, fresh, delivered); err != nil {
		t.Fatal(err)
	}
	claimed, err := repository.ClaimUnconfirmedSMS(ctx, "twilio", 50)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, message := range claimed {
		if message.MessageID == fresh || message.MessageID == delivered {
			t.Fatalf("claimed %s, which is fresh or already delivered", message.MessageID)
		}
		found = found || (message.MessageID == stale && message.ProviderMessageID == "SM-stale")
	}
	if !found {
		t.Fatalf("stale SMS was not claimed: %+v", claimed)
	}
	again, err := repository.ClaimUnconfirmedSMS(ctx, "twilio", 50)
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range again {
		if message.MessageID == stale {
			t.Fatal("stale SMS was claimed again within the 15-minute throttle")
		}
	}
	summary, err := repository.OperationalSummary(ctx, accountID)
	if err != nil || summary.UnconfirmedSMS != 0 {
		t.Fatalf("summary = %+v, error = %v; a one-hour-old SMS is not yet unconfirmed", summary, err)
	}
}
