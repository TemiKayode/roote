package delivery

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

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
	beginCount            int
	completeCount         int
	failureReason         string
	providerMessage       string
	smsJob                store.SMSJob
	smsConsent            bool
	smsSuppressed         bool
	smsBudgetError        error
	smsReservedCost       float64
	smsSendCount          int
	smsComplete           int
	smsConfirmationBudget float64
}

func (f *fakeRepository) GetDomain(context.Context, string, uuid.UUID) (store.Domain, error) {
	return store.Domain{}, errors.New("not implemented")
}
func (f *fakeRepository) SetDomainVerificationResult(context.Context, uuid.UUID, bool, string) error {
	return errors.New("not implemented")
}
func (f *fakeRepository) GetEmailJob(context.Context, uuid.UUID) (store.EmailJob, error) {
	if f.job.ID == uuid.Nil && f.smsJob.ID != uuid.Nil {
		return store.EmailJob{}, errors.New("not an email job")
	}
	if !contains(f.job.Channels, "email") {
		return store.EmailJob{}, errors.New("not an email job")
	}
	return f.job, nil
}
func (f *fakeRepository) GetDKIMKey(context.Context, uuid.UUID, string) (store.DKIMKey, error) {
	return f.dkimKey, nil
}
func (f *fakeRepository) IsEmailSuppressed(context.Context, uuid.UUID, string) (bool, error) {
	return f.suppressed, nil
}
func (f *fakeRepository) BeginEmailDelivery(context.Context, store.EmailJob, string) error {
	f.beginCount++
	return nil
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
func (f *fakeRepository) ReleaseSMSBudget(context.Context, uuid.UUID) error { return nil }
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

type fakeTransport struct {
	message email.Message
}

type fakeSMSTransport struct {
	message sms.Message
}

func (f *fakeSMSTransport) Provider() string { return "test-sms" }
func (f *fakeSMSTransport) Send(_ context.Context, message sms.Message) (string, error) {
	f.message = message
	return "sms-provider-id", nil
}

func (f *fakeTransport) Provider() string {
	return "test"
}

func (f *fakeTransport) Send(_ context.Context, message email.Message) (string, error) {
	f.message = message
	return "provider-id", nil
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
	if repository.failureReason != "recipient is suppressed for email" || repository.beginCount != 0 {
		t.Fatalf("suppressed delivery proceeded: failure=%q begin=%d", repository.failureReason, repository.beginCount)
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
	if !errors.Is(err, asynq.SkipRetry) || repository.failureReason != "active SMS consent is required" {
		t.Fatalf("error = %v, failure reason = %q", err, repository.failureReason)
	}
	if repository.smsSendCount != 0 || repository.smsReservedCost != 0 {
		t.Fatal("SMS was budgeted or sent without consent")
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
