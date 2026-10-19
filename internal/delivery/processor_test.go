package delivery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"

	"github.com/TemiKayode/roote/internal/config"
	"github.com/TemiKayode/roote/internal/emaildomain"
	"github.com/TemiKayode/roote/internal/store"
	"github.com/TemiKayode/roote/internal/transport/email"
	"github.com/TemiKayode/roote/internal/transport/sms"
)

type fakeRepository struct {
	job                   store.EmailJob
	dkimKey               store.DKIMKey
	suppressed            bool
	preferred             bool
	beginCount            int
	completeCount         int
	failureReason         string
	providerMessage       string
	smsJob                store.SMSJob
	smsConsent            bool
	smsSuppressed         bool
	smsBudgetError        error
	senderCapacityError   error
	smsReservedCost       float64
	smsBudgetReleaseCount int
	senderCapacityCount   int
	smsSendCount          int
	smsComplete           int
	smsConfirmationBudget float64
	preferredChannels     map[string]bool
	channelFailures       []string
	channelSkips          []string
	failureWebhookCount   int
	exhaustedRetries      int
	loadErr               error
	dkimErr               error
}

func (f *fakeRepository) GetDomain(context.Context, string, uuid.UUID) (store.Domain, error) {
	return store.Domain{}, errors.New("not implemented")
}
func (f *fakeRepository) SetDomainVerificationResult(context.Context, uuid.UUID, bool, string) error {
	return errors.New("not implemented")
}
func (f *fakeRepository) GetEmailJob(context.Context, uuid.UUID) (store.EmailJob, error) {
	if f.loadErr != nil {
		return store.EmailJob{}, f.loadErr
	}
	if f.job.ID == uuid.Nil && f.smsJob.ID != uuid.Nil {
		return store.EmailJob{}, errors.New("not an email job")
	}
	if !contains(f.job.Channels, "email") {
		return store.EmailJob{}, errors.New("not an email job")
	}
	return f.job, nil
}
func (f *fakeRepository) GetDKIMKey(context.Context, uuid.UUID, string) (store.DKIMKey, error) {
	return f.dkimKey, f.dkimErr
}
func (f *fakeRepository) IsEmailSuppressed(context.Context, uuid.UUID, string) (bool, error) {
	return f.suppressed, nil
}
func (f *fakeRepository) IsNotificationPreferred(_ context.Context, _ uuid.UUID, _, channel string) (bool, error) {
	if f.preferredChannels != nil {
		return f.preferredChannels[channel], nil
	}
	return f.preferred, nil
}
func (f *fakeRepository) BeginEmailDelivery(context.Context, store.EmailJob, string) (bool, error) {
	f.beginCount++
	return true, nil
}
func (f *fakeRepository) CompleteEmailDelivery(_ context.Context, _ uuid.UUID, providerID string) error {
	f.completeCount++
	f.providerMessage = providerID
	return nil
}
func (f *fakeRepository) FailEmailDelivery(context.Context, uuid.UUID, error) error {
	return nil
}
func (f *fakeRepository) GetSMSJob(context.Context, uuid.UUID) (store.SMSJob, error) {
	if f.loadErr != nil {
		return store.SMSJob{}, f.loadErr
	}
	return f.smsJob, nil
}
func (f *fakeRepository) IsSMSSuppressed(context.Context, uuid.UUID, string) (bool, error) {
	return f.smsSuppressed, nil
}
func (f *fakeRepository) HasSMSConsent(context.Context, uuid.UUID, string, string) (bool, error) {
	return f.smsConsent, nil
}
func (f *fakeRepository) ReserveSMSBudget(_ context.Context, _, _ uuid.UUID, _ string, cost, _ float64, _ int) error {
	f.smsReservedCost = cost
	return f.smsBudgetError
}
func (f *fakeRepository) ReserveSenderCapacity(context.Context, uuid.UUID, string, string, int, int, int) error {
	f.senderCapacityCount++
	return f.senderCapacityError
}
func (f *fakeRepository) ReleaseSenderCapacity(context.Context, uuid.UUID, string, string) error {
	return nil
}
func (f *fakeRepository) ReleaseSMSBudget(context.Context, uuid.UUID) error {
	f.smsBudgetReleaseCount++
	return nil
}
func (f *fakeRepository) BeginSMSDelivery(_ context.Context, _ store.SMSJob, _ string, _ string, _ float64) (bool, error) {
	f.smsSendCount++
	return true, nil
}
func (f *fakeRepository) CompleteSMSDelivery(context.Context, uuid.UUID, string) error {
	f.smsComplete++
	return nil
}
func (f *fakeRepository) FailSMSDelivery(context.Context, uuid.UUID, error) error { return nil }
func (f *fakeRepository) ReserveSMSConfirmationBudget(
	_ context.Context, _ string, _ uuid.UUID, _ string, cost, _ float64, _ int,
) error {
	f.smsConfirmationBudget = cost
	return nil
}
func (f *fakeRepository) ReleaseSMSConfirmationBudget(context.Context, string) error { return nil }
func (f *fakeRepository) MarkMessageFailed(_ context.Context, _ uuid.UUID, reason string) error {
	f.failureReason = reason
	return nil
}
func (f *fakeRepository) FailMessageRetriesExhausted(_ context.Context, _ uuid.UUID, retries, maximum int) error {
	f.exhaustedRetries = retries + maximum
	return nil
}
func (f *fakeRepository) RecordChannelFailure(
	_ context.Context, _ uuid.UUID, channel, _ string,
) error {
	f.channelFailures = append(f.channelFailures, channel)
	return nil
}
func (f *fakeRepository) RecordChannelSkipped(_ context.Context, _ uuid.UUID, channel string) error {
	f.channelSkips = append(f.channelSkips, channel)
	return nil
}
func (f *fakeRepository) EmitNotificationFailure(context.Context, uuid.UUID) error {
	f.failureWebhookCount++
	return nil
}

type fakeTransport struct {
	message email.Message
	err     error
}

type fakeSMSTransport struct {
	message sms.Message
	err     error
}

func (f *fakeSMSTransport) Provider() string { return "test-sms" }
func (f *fakeSMSTransport) Send(_ context.Context, message sms.Message) (string, error) {
	f.message = message
	return "sms-provider-id", f.err
}

func (f *fakeTransport) Provider() string {
	return "test"
}

func (f *fakeTransport) Send(_ context.Context, message email.Message) (string, error) {
	f.message = message
	return "provider-id", f.err
}

func TestDeliverNotificationUsesVerifiedDKIMAndPersistsProviderID(t *testing.T) {
	material, err := emaildomain.GenerateKeyMaterial()
	if err != nil {
		t.Fatal(err)
	}
	encryptionKey := make([]byte, 32)
	for i := range encryptionKey {
		encryptionKey[i] = 9
	}
	ciphertext, err := emaildomain.EncryptPrivateKey(encryptionKey, material.PrivatePEM)
	if err != nil {
		t.Fatal(err)
	}
	messageID := uuid.New()
	accountID := uuid.New()
	repository := &fakeRepository{
		job: store.EmailJob{
			ID: messageID, AccountID: accountID, Channels: []string{"email"},
			Recipient: "person@example.net", Subject: "Hello", Body: "World",
		},
		dkimKey: store.DKIMKey{Selector: material.Selector, EncryptedPrivateKey: ciphertext},
	}
	transport := &fakeTransport{}
	cfg := config.Config{EmailFrom: "notifications@example.org", DKIMEncryptionKey: encryptionKey}
	processor := NewProcessor(
		repository, transport, nil, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	if err := processor.DeliverNotification(context.Background(), messageID); err != nil {
		t.Fatal(err)
	}
	if repository.beginCount != 1 || repository.completeCount != 1 || repository.providerMessage != "provider-id" {
		t.Fatalf("delivery persistence = begin:%d complete:%d provider:%q",
			repository.beginCount, repository.completeCount, repository.providerMessage)
	}
	if transport.message.DKIMDomain != "example.org" || transport.message.DKIMSelector != material.Selector {
		t.Fatalf("transport did not receive verified DKIM identity: %+v", transport.message)
	}
	if len(transport.message.DKIMPrivateKeyPEM) == 0 || transport.message.To != "person@example.net" {
		t.Fatalf("transport message = %+v", transport.message)
	}
}

func TestDeliverNotificationSkipsSuppressedRecipient(t *testing.T) {
	messageID := uuid.New()
	repository := &fakeRepository{
		job: store.EmailJob{
			ID: messageID, AccountID: uuid.New(), Channels: []string{"email"},
			Recipient: "person@example.net",
		},
		suppressed: true,
	}
	processor := NewProcessor(
		repository, &fakeTransport{}, nil, config.Config{EmailFrom: "notifications@example.org"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	err := processor.DeliverNotification(context.Background(), messageID)
	if !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("error = %v, want archived non-retryable task", err)
	}
	if repository.beginCount != 0 || len(repository.channelFailures) != 1 ||
		repository.channelFailures[0] != "email" {
		t.Fatalf("suppressed delivery proceeded: channel failures=%v begin=%d",
			repository.channelFailures, repository.beginCount)
	}
}

func TestDeliverSMSRequiresConsentAndReservesCountryCost(t *testing.T) {
	messageID := uuid.New()
	accountID := uuid.New()
	repository := &fakeRepository{
		smsJob: store.SMSJob{
			ID: messageID, AccountID: accountID, Channels: []string{"sms"},
			Recipient: "+14155552671", Body: "Hello",
		},
		smsConsent: true,
	}
	transport := &fakeSMSTransport{}
	cfg := config.Config{
		SMSTransport:             "twilio",
		SMSRateLimitPerMinute:    20,
		SMSCountryRatesUSD:       map[string]float64{"US": 0.02},
		SMSCountryMonthlyCapsUSD: map[string]float64{"US": 10},
	}
	processor := NewProcessor(
		repository, nil, transport, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err := processor.DeliverNotification(context.Background(), messageID); err != nil {
		t.Fatal(err)
	}
	if repository.smsSendCount != 1 || repository.smsComplete != 1 ||
		repository.smsReservedCost != 0.02 || transport.message.To != "+14155552671" {
		t.Fatalf("SMS delivery state: send=%d complete=%d reserved=%.4f message=%+v",
			repository.smsSendCount, repository.smsComplete, repository.smsReservedCost, transport.message)
	}
}

func TestDeliverSMSFailsClosedWithoutConsent(t *testing.T) {
	messageID := uuid.New()
	repository := &fakeRepository{smsJob: store.SMSJob{
		ID: messageID, AccountID: uuid.New(), Channels: []string{"sms"},
		Recipient: "+14155552671", Body: "Hello",
	}}
	processor := NewProcessor(
		repository, nil, &fakeSMSTransport{},
		config.Config{
			SMSTransport:             "twilio",
			SMSCountryRatesUSD:       map[string]float64{"US": 0.02},
			SMSCountryMonthlyCapsUSD: map[string]float64{"US": 10},
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	err := processor.DeliverNotification(context.Background(), messageID)
	if !errors.Is(err, asynq.SkipRetry) || len(repository.channelFailures) != 1 ||
		repository.channelFailures[0] != "sms" {
		t.Fatalf("error = %v, channel failures = %v", err, repository.channelFailures)
	}
	if repository.smsSendCount != 0 || repository.smsReservedCost != 0 {
		t.Fatal("SMS was budgeted or sent without consent")
	}
}

func TestDeliverSMSStopsWhenSenderWarmupLimitIsReached(t *testing.T) {
	repository := &fakeRepository{
		smsJob: store.SMSJob{
			ID: uuid.New(), AccountID: uuid.New(),
			Channels: []string{"sms"}, Recipient: "+14155552671", Body: "Hello",
		},
		smsConsent:          true,
		senderCapacityError: store.ErrSenderCapacity,
	}
	transport := &fakeSMSTransport{}
	processor := NewProcessor(repository, nil, transport, config.Config{
		SMSTransport:             "twilio",
		SMSRateLimitPerMinute:    10,
		SMSCountryRatesUSD:       map[string]float64{"US": 0.01},
		SMSCountryMonthlyCapsUSD: map[string]float64{"US": 20},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := processor.DeliverSMS(context.Background(), repository.smsJob.ID); !errors.Is(err, store.ErrSenderCapacity) {
		t.Fatalf("DeliverSMS() error = %v, want ErrSenderCapacity", err)
	}
	if repository.smsSendCount != 0 || repository.smsReservedCost != 0 ||
		repository.senderCapacityCount != 1 || transport.message.To != "" {
		t.Fatalf("warm-up limit did not stop sending before SMS budget reservation: sends=%d reserved=%.4f capacity=%d message=%+v",
			repository.smsSendCount, repository.smsReservedCost, repository.senderCapacityCount, transport.message)
	}
}

func TestEmailCapacityIdentityUsesTrafficClassIPPool(t *testing.T) {
	tests := []struct {
		name         string
		trafficClass string
		want         string
	}{
		{name: "transactional pool", trafficClass: "transactional", want: "sendgrid:ip-pool:transactional-pool"},
		{name: "marketing pool", trafficClass: "marketing", want: "sendgrid:ip-pool:marketing-pool"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := emailCapacityIdentity(
				"sendgrid", test.trafficClass, "Alerts.Example.org",
				"transactional-pool", "marketing-pool",
			)
			if got != test.want {
				t.Fatalf("emailCapacityIdentity() = %q, want %q", got, test.want)
			}
		})
	}
	if got := emailCapacityIdentity("kumo-smtp", "transactional", "Alerts.Example.org", "", ""); got != "kumo-smtp:domain:alerts.example.org" {
		t.Fatalf("SMTP capacity identity = %q", got)
	}
	if got := emailCapacityIdentity("sendgrid", "marketing", "Alerts.Example.org", "transactional-pool", ""); got != "sendgrid:domain:alerts.example.org" {
		t.Fatalf("unpooled SendGrid capacity identity = %q", got)
	}
}

func TestPreferenceRoutingSelectsOnlyEnabledChannel(t *testing.T) {
	messageID, accountID := uuid.New(), uuid.New()
	routing := store.Routing{Strategy: "preference"}
	repository := &fakeRepository{
		job: store.EmailJob{
			ID: messageID, AccountID: accountID, Channels: []string{"email", "sms"},
			Recipient: "person@example.net", Routing: routing,
		},
		smsJob: store.SMSJob{
			ID: messageID, AccountID: accountID, Channels: []string{"email", "sms"},
			Recipient: "+14155552671", Body: "Hello", Routing: routing,
		},
		preferredChannels: map[string]bool{"sms": true},
		smsConsent:        true,
	}
	smsTransport := &fakeSMSTransport{}
	processor := NewProcessor(
		repository, nil, smsTransport,
		config.Config{
			SMSTransport:             "twilio",
			SMSRateLimitPerMinute:    10,
			SMSCountryRatesUSD:       map[string]float64{"US": 0.01},
			SMSCountryMonthlyCapsUSD: map[string]float64{"US": 1},
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err := processor.DeliverNotification(context.Background(), messageID); err != nil {
		t.Fatal(err)
	}
	if repository.smsSendCount != 1 || repository.beginCount != 0 ||
		smsTransport.message.To != "+14155552671" || len(repository.channelSkips) != 1 ||
		repository.channelSkips[0] != "email" {
		t.Fatalf("preference routing selected wrong channel: sms=%d email=%d message=%+v",
			repository.smsSendCount, repository.beginCount, smsTransport.message)
	}
}

func TestPreferenceRoutingFailsWhenAllChannelsAreDisabled(t *testing.T) {
	messageID, accountID := uuid.New(), uuid.New()
	repository := &fakeRepository{
		job: store.EmailJob{
			ID: messageID, AccountID: accountID, Channels: []string{"email", "sms"},
			Recipient: "person@example.net", Routing: store.Routing{Strategy: "preference"},
		},
		smsJob: store.SMSJob{
			ID: messageID, AccountID: accountID, Channels: []string{"email", "sms"},
			Recipient: "+14155552671", Routing: store.Routing{Strategy: "preference"},
		},
		preferredChannels: map[string]bool{},
	}
	processor := NewProcessor(
		repository, nil, nil, config.Config{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	err := processor.DeliverNotification(context.Background(), messageID)
	if !errors.Is(err, asynq.SkipRetry) || repository.failureWebhookCount != 1 ||
		len(repository.channelFailures) != 2 {
		t.Fatalf("no-preference result: error=%v failures=%v webhook=%d",
			err, repository.channelFailures, repository.failureWebhookCount)
	}
}

func TestWaterfallRoutingFallsBackAfterEmailFailure(t *testing.T) {
	messageID, accountID := uuid.New(), uuid.New()
	material, err := emaildomain.GenerateKeyMaterial()
	if err != nil {
		t.Fatal(err)
	}
	encryptionKey := make([]byte, 32)
	for i := range encryptionKey {
		encryptionKey[i] = 3
	}
	ciphertext, err := emaildomain.EncryptPrivateKey(encryptionKey, material.PrivatePEM)
	if err != nil {
		t.Fatal(err)
	}
	routing := store.Routing{Strategy: "waterfall", Order: []string{"email", "sms"}}
	repository := &fakeRepository{
		job: store.EmailJob{
			ID: messageID, AccountID: accountID, Channels: []string{"email", "sms"},
			Recipient: "person@example.net", Subject: "Hello", Body: "World", Routing: routing,
		},
		dkimKey: store.DKIMKey{Selector: material.Selector, EncryptedPrivateKey: ciphertext},
		smsJob: store.SMSJob{
			ID: messageID, AccountID: accountID, Channels: []string{"email", "sms"},
			Recipient: "+14155552671", Body: "Hello", Routing: routing,
		},
		smsConsent: true,
	}
	emailTransport := &fakeTransport{err: errors.New("permanent provider rejection")}
	smsTransport := &fakeSMSTransport{}
	processor := NewProcessor(
		repository, emailTransport, smsTransport,
		config.Config{
			EmailFrom:                "notifications@example.org",
			DKIMEncryptionKey:        encryptionKey,
			SMSTransport:             "twilio",
			SMSRateLimitPerMinute:    10,
			SMSCountryRatesUSD:       map[string]float64{"US": 0.01},
			SMSCountryMonthlyCapsUSD: map[string]float64{"US": 1},
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err := processor.DeliverNotification(context.Background(), messageID); err != nil {
		t.Fatal(err)
	}
	if repository.beginCount != 1 || repository.smsSendCount != 1 || repository.smsComplete != 1 {
		t.Fatalf("waterfall attempts: email=%d SMS=%d completed=%d",
			repository.beginCount, repository.smsSendCount, repository.smsComplete)
	}
}

func TestWaterfallRoutingSkipsRemainingChannelsAfterSuccess(t *testing.T) {
	messageID, accountID := uuid.New(), uuid.New()
	material, err := emaildomain.GenerateKeyMaterial()
	if err != nil {
		t.Fatal(err)
	}
	encryptionKey := make([]byte, 32)
	ciphertext, err := emaildomain.EncryptPrivateKey(encryptionKey, material.PrivatePEM)
	if err != nil {
		t.Fatal(err)
	}
	routing := store.Routing{Strategy: "waterfall", Order: []string{"email", "sms"}}
	repository := &fakeRepository{
		job: store.EmailJob{
			ID: messageID, AccountID: accountID, Channels: []string{"email", "sms"},
			Recipient: "person@example.net", Subject: "Hello", Body: "World", Routing: routing,
		},
		dkimKey: store.DKIMKey{Selector: material.Selector, EncryptedPrivateKey: ciphertext},
		smsJob: store.SMSJob{
			ID: messageID, AccountID: accountID, Channels: []string{"email", "sms"},
			Recipient: "+14155552671", Body: "Hello", Routing: routing,
		},
		smsConsent: true,
	}
	processor := NewProcessor(
		repository, &fakeTransport{}, &fakeSMSTransport{},
		config.Config{EmailFrom: "notifications@example.org", DKIMEncryptionKey: encryptionKey},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err := processor.DeliverNotification(context.Background(), messageID); err != nil {
		t.Fatal(err)
	}
	if repository.completeCount != 1 || repository.smsSendCount != 0 ||
		len(repository.channelSkips) != 1 || repository.channelSkips[0] != "sms" {
		t.Fatalf("waterfall success: email=%d SMS=%d skips=%v",
			repository.completeCount, repository.smsSendCount, repository.channelSkips)
	}
}

func TestDeletedNotificationIsNotRetried(t *testing.T) {
	repository := &fakeRepository{loadErr: fmt.Errorf("load message: %w", pgx.ErrNoRows)}
	processor := NewProcessor(repository, nil, nil, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := processor.DeliverNotification(context.Background(), uuid.New()); !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("deleted notification error = %v, want SkipRetry", err)
	}
}

func TestTransientDKIMLookupErrorIsRetried(t *testing.T) {
	messageID := uuid.New()
	repository := &fakeRepository{
		job: store.EmailJob{
			ID: messageID, AccountID: uuid.New(), Channels: []string{"email"},
			Recipient: "person@example.net", Subject: "Hello", Body: "World",
		},
		dkimErr: errors.New("connection reset"),
	}
	processor := NewProcessor(
		repository, &fakeTransport{}, nil,
		config.Config{EmailFrom: "notifications@example.org", DKIMEncryptionKey: make([]byte, 32)},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	err := processor.DeliverNotification(context.Background(), messageID)
	if err == nil || errors.Is(err, asynq.SkipRetry) || len(repository.channelFailures) != 0 {
		t.Fatalf("transient DKIM error = %v failures=%v, want retryable error", err, repository.channelFailures)
	}
}

func TestExplicitRoutingFansOutToBothChannels(t *testing.T) {
	messageID, accountID := uuid.New(), uuid.New()
	material, err := emaildomain.GenerateKeyMaterial()
	if err != nil {
		t.Fatal(err)
	}
	encryptionKey := make([]byte, 32)
	for i := range encryptionKey {
		encryptionKey[i] = 4
	}
	ciphertext, err := emaildomain.EncryptPrivateKey(encryptionKey, material.PrivatePEM)
	if err != nil {
		t.Fatal(err)
	}
	repository := &fakeRepository{
		job: store.EmailJob{
			ID: messageID, AccountID: accountID, Channels: []string{"email", "sms"},
			Recipient: "person@example.net", Subject: "Hello", Body: "World",
		},
		dkimKey: store.DKIMKey{Selector: material.Selector, EncryptedPrivateKey: ciphertext},
		smsJob: store.SMSJob{
			ID: messageID, AccountID: accountID, Channels: []string{"email", "sms"},
			Recipient: "+14155552671", Body: "Hello",
		},
		smsConsent: true,
	}
	processor := NewProcessor(
		repository, &fakeTransport{}, &fakeSMSTransport{},
		config.Config{
			EmailFrom:                "notifications@example.org",
			DKIMEncryptionKey:        encryptionKey,
			SMSTransport:             "twilio",
			SMSRateLimitPerMinute:    10,
			SMSCountryRatesUSD:       map[string]float64{"US": 0.01},
			SMSCountryMonthlyCapsUSD: map[string]float64{"US": 1},
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err := processor.DeliverNotification(context.Background(), messageID); err != nil {
		t.Fatal(err)
	}
	if repository.beginCount != 1 || repository.smsSendCount != 1 || repository.completeCount != 1 ||
		repository.smsComplete != 1 {
		t.Fatalf("fan-out attempts: email=%d/%d SMS=%d/%d",
			repository.beginCount, repository.completeCount, repository.smsSendCount, repository.smsComplete)
	}
}

func TestFanoutFailureOnOneChannelDoesNotBlockOtherChannel(t *testing.T) {
	messageID, accountID := uuid.New(), uuid.New()
	repository := &fakeRepository{
		job: store.EmailJob{
			ID: messageID, AccountID: accountID, Channels: []string{"email", "sms"},
			Recipient: "person@example.net",
		},
		smsJob: store.SMSJob{
			ID: messageID, AccountID: accountID, Channels: []string{"email", "sms"},
			Recipient: "+14155552671", Body: "Hello",
		},
		suppressed: true,
		smsConsent: true,
	}
	processor := NewProcessor(
		repository, &fakeTransport{}, &fakeSMSTransport{},
		config.Config{
			SMSTransport:             "twilio",
			SMSRateLimitPerMinute:    10,
			SMSCountryRatesUSD:       map[string]float64{"US": 0.01},
			SMSCountryMonthlyCapsUSD: map[string]float64{"US": 1},
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err := processor.DeliverNotification(context.Background(), messageID); err != nil {
		t.Fatal(err)
	}
	if repository.smsSendCount != 1 || repository.failureWebhookCount != 0 ||
		len(repository.channelFailures) != 1 || repository.channelFailures[0] != "email" {
		t.Fatalf("channel isolation failed: sends=%d failures=%v terminal_webhooks=%d",
			repository.smsSendCount, repository.channelFailures, repository.failureWebhookCount)
	}
}

func TestSMSStopConfirmationReservesRateAndCountryBudget(t *testing.T) {
	repository := &fakeRepository{}
	transport := &fakeSMSTransport{}
	processor := NewProcessor(
		repository, nil, transport,
		config.Config{
			AccountID:                uuid.NewString(),
			SMSTransport:             "twilio",
			SMSStopConfirmation:      "You are unsubscribed.",
			SMSRateLimitPerMinute:    20,
			SMSCountryRatesUSD:       map[string]float64{"US": 0.02},
			SMSCountryMonthlyCapsUSD: map[string]float64{"US": 10},
		},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err := processor.SendSMSStopConfirmation(
		context.Background(), "test-sms", "+14155552671", "evt-1",
	); err != nil {
		t.Fatal(err)
	}
	if repository.smsConfirmationBudget != 0.02 || transport.message.TrackStatus {
		t.Fatalf("confirmation reservation = %.4f, transport message = %+v",
			repository.smsConfirmationBudget, transport.message)
	}
}

func TestHandleNotificationTaskPersistsRetryExhaustion(t *testing.T) {
	repository := &fakeRepository{}
	processor := NewProcessor(repository, nil, nil, config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := processor.HandleNotificationTask(context.Background(), uuid.New(), 24, 25)
	if err == nil {
		t.Fatal("HandleNotificationTask() returned no error for an invalid notification")
	}
	if repository.exhaustedRetries != 0 {
		t.Fatalf("retry exhaustion was recorded before the final retry: %d", repository.exhaustedRetries)
	}
	err = processor.HandleNotificationTask(context.Background(), uuid.New(), 25, 25)
	if err == nil {
		t.Fatal("HandleNotificationTask() returned no error for an invalid notification")
	}
	if repository.exhaustedRetries != 50 {
		t.Fatalf("retry exhaustion record = %d, want 50", repository.exhaustedRetries)
	}
}
