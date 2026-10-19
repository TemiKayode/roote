package delivery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/mail"
	"strings"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/nyaruka/phonenumbers"

	"github.com/TemiKayode/roote/internal/config"
	"github.com/TemiKayode/roote/internal/emaildomain"
	"github.com/TemiKayode/roote/internal/store"
	"github.com/TemiKayode/roote/internal/transport/email"
	"github.com/TemiKayode/roote/internal/transport/sms"
)

type Repository interface {
	GetDomain(context.Context, string, uuid.UUID) (store.Domain, error)
	SetDomainVerificationResult(context.Context, uuid.UUID, bool, string) error
	GetEmailJob(context.Context, uuid.UUID) (store.EmailJob, error)
	GetDKIMKey(context.Context, uuid.UUID, string) (store.DKIMKey, error)
	IsEmailSuppressed(context.Context, uuid.UUID, string) (bool, error)
	BeginEmailDelivery(context.Context, store.EmailJob, string) error
	CompleteEmailDelivery(context.Context, uuid.UUID, string) error
	FailEmailDelivery(context.Context, uuid.UUID, error) error
	GetSMSJob(context.Context, uuid.UUID) (store.SMSJob, error)
	IsSMSSuppressed(context.Context, uuid.UUID, string) (bool, error)
	HasSMSConsent(context.Context, uuid.UUID, string, string) (bool, error)
	ReserveSMSBudget(context.Context, uuid.UUID, uuid.UUID, string, float64, float64, int) error
	ReleaseSMSBudget(context.Context, uuid.UUID) error
	BeginSMSDelivery(context.Context, store.SMSJob, string, string, float64) (bool, error)
	CompleteSMSDelivery(context.Context, uuid.UUID, string) error
	FailSMSDelivery(context.Context, uuid.UUID, error) error
	ReserveSMSConfirmationBudget(context.Context, string, uuid.UUID, string, float64, float64, int) error
	ReleaseSMSConfirmationBudget(context.Context, string) error
	MarkMessageFailed(context.Context, uuid.UUID, string) error
}

type Processor struct {
	repository   Repository
	transport    email.Transport
	smsTransport sms.Transport
	config       config.Config
	logger       *slog.Logger
}

func NewProcessor(
	repository Repository,
	transport email.Transport,
	smsTransport sms.Transport,
	cfg config.Config,
	logger *slog.Logger,
) *Processor {
	return &Processor{
		repository: repository, transport: transport, smsTransport: smsTransport, config: cfg, logger: logger,
	}
}

func (p *Processor) VerifyDomain(ctx context.Context, accountID string, domainID uuid.UUID) error {
	domain, err := p.repository.GetDomain(ctx, accountID, domainID)
	if err != nil {
		return fmt.Errorf("load domain for DNS verification: %w", err)
	}
	err = emaildomain.VerifyRecords(
		ctx, netTXTResolver{}, domain.Name, domain.Selector, domain.PublicKey, domain.SPFValue,
	)
	if err != nil {
		if saveErr := p.repository.SetDomainVerificationResult(ctx, domain.ID, false, err.Error()); saveErr != nil {
			return errors.Join(err, saveErr)
		}
		return fmt.Errorf("DNS records for %s are not verified: %w", domain.Name, err)
	}
	if err := p.repository.SetDomainVerificationResult(ctx, domain.ID, true, ""); err != nil {
		return err
	}
	p.logger.InfoContext(ctx, "sending domain verified", "domain", domain.Name, "domain_id", domain.ID)
	return nil
}

func (p *Processor) DeliverNotification(ctx context.Context, messageID uuid.UUID) error {
	job, err := p.repository.GetEmailJob(ctx, messageID)
	if err != nil {
		smsJob, smsErr := p.repository.GetSMSJob(ctx, messageID)
		if smsErr == nil && contains(smsJob.Channels, "sms") && !contains(smsJob.Channels, "email") {
			return p.DeliverSMS(ctx, messageID)
		}
		return fmt.Errorf("load notification for delivery: %w", err)
	}
	if contains(job.Channels, "sms") {
		return p.failPermanently(ctx, messageID, "mixed-channel delivery requires orchestration and is not yet available")
	}
	if !contains(job.Channels, "email") {
		return p.failPermanently(ctx, messageID, "notification has no supported delivery channel")
	}
	if p.transport == nil {
		return p.failPermanently(ctx, messageID, "email transport is disabled; configure EMAIL_TRANSPORT")
	}
	suppressed, err := p.repository.IsEmailSuppressed(ctx, job.AccountID, job.Recipient)
	if err != nil {
		return fmt.Errorf("check recipient suppression: %w", err)
	}
	if suppressed {
		return p.failPermanently(ctx, messageID, "recipient is suppressed for email")
	}

	from, err := mail.ParseAddress(p.config.EmailFrom)
	if err != nil {
		return p.failPermanently(ctx, messageID, "configured EMAIL_FROM is invalid")
	}
	parts := strings.Split(from.Address, "@")
	if len(parts) != 2 {
		return p.failPermanently(ctx, messageID, "configured EMAIL_FROM has no domain")
	}
	dkimKey, err := p.repository.GetDKIMKey(ctx, job.AccountID, strings.ToLower(parts[1]))
	if err != nil {
		return p.failPermanently(ctx, messageID, "sender domain has not been verified")
	}
	privateKeyPEM, err := emaildomain.DecryptPrivateKey(p.config.DKIMEncryptionKey, dkimKey.EncryptedPrivateKey)
	if err != nil {
		return p.failPermanently(ctx, messageID, "could not decrypt sender DKIM key")
	}
	if job.TrafficClass == "" {
		job.TrafficClass = "transactional"
	}
	message, err := email.NewMessage(
		job.ID.String(), p.config.EmailFrom, job.Recipient, job.Subject, job.Body,
		job.TrafficClass, p.config.MarketingUnsubscribeURL, dkimKey.Selector, privateKeyPEM,
	)
	if err != nil {
		return p.failPermanently(ctx, messageID, err.Error())
	}
	if err := p.repository.BeginEmailDelivery(ctx, job, p.transport.Provider()); err != nil {
		return err
	}
	providerMessageID, err := p.transport.Send(ctx, message)
	if err != nil {
		if recordErr := p.repository.FailEmailDelivery(ctx, messageID, err); recordErr != nil {
			return errors.Join(err, recordErr)
		}
		var deliveryError *email.DeliveryError
		if errors.As(err, &deliveryError) && deliveryError.Temporary {
			return err
		}
		if markErr := p.repository.MarkMessageFailed(ctx, messageID, err.Error()); markErr != nil {
			return errors.Join(err, markErr)
		}
		return fmt.Errorf("%w: %v", asynq.SkipRetry, err)
	}
	if err := p.repository.CompleteEmailDelivery(ctx, messageID, providerMessageID); err != nil {
		return fmt.Errorf("persist provider-accepted email: %w", err)
	}
	p.logger.InfoContext(ctx, "email accepted by provider",
		"message_id", messageID,
		"provider", p.transport.Provider(),
		"provider_message_id", providerMessageID,
	)
	return nil
}

func (p *Processor) DeliverSMS(ctx context.Context, messageID uuid.UUID) error {
	job, err := p.repository.GetSMSJob(ctx, messageID)
	if err != nil {
		return fmt.Errorf("load notification for SMS delivery: %w", err)
	}
	if !contains(job.Channels, "sms") {
		return p.failPermanently(ctx, messageID, "notification does not request SMS")
	}
	if p.smsTransport == nil {
		return p.failPermanently(ctx, messageID, "SMS transport is disabled; configure SMS_TRANSPORT")
	}
	countryNumber, err := phonenumbers.Parse(job.Recipient, "")
	if err != nil || !phonenumbers.IsValidNumber(countryNumber) {
		return p.failPermanently(ctx, messageID, "recipient phone number is invalid")
	}
	countryCode := phonenumbers.GetRegionCodeForNumber(countryNumber)
	rate, hasRate := p.config.SMSCountryRatesUSD[countryCode]
	budget, hasBudget := p.config.SMSCountryMonthlyCapsUSD[countryCode]
	if !hasRate || !hasBudget {
		return p.failPermanently(ctx, messageID, "SMS country pricing and monthly budget are not configured")
	}
	trafficClass := job.TrafficClass
	if trafficClass == "" {
		trafficClass = "transactional"
	}
	consented, err := p.repository.HasSMSConsent(ctx, job.AccountID, job.Recipient, trafficClass)
	if err != nil {
		return fmt.Errorf("check SMS consent: %w", err)
	}
	if !consented {
		return p.failPermanently(ctx, messageID, "active SMS consent is required")
	}
	suppressed, err := p.repository.IsSMSSuppressed(ctx, job.AccountID, job.Recipient)
	if err != nil {
		return fmt.Errorf("check SMS suppression: %w", err)
	}
	if suppressed {
		return p.failPermanently(ctx, messageID, "recipient is suppressed for SMS")
	}
	segments := sms.SegmentCount(job.Body)
	estimatedCost := float64(segments) * rate
	if err := p.repository.ReserveSMSBudget(
		ctx, job.ID, job.AccountID, countryCode, estimatedCost, budget, p.config.SMSRateLimitPerMinute,
	); err != nil {
		if errors.Is(err, store.ErrSMSRateLimit) {
			return fmt.Errorf("reserve SMS rate limit: %w", err)
		}
		if errors.Is(err, store.ErrSMSCountryBudget) {
			return p.failPermanently(ctx, messageID, err.Error())
		}
		return fmt.Errorf("reserve SMS sending capacity: %w", err)
	}
	shouldSend, err := p.repository.BeginSMSDelivery(ctx, job, p.smsTransport.Provider(), countryCode, estimatedCost)
	if err != nil {
		return fmt.Errorf("begin SMS delivery: %w", err)
	}
	if !shouldSend {
		return nil
	}
	providerMessageID, err := p.smsTransport.Send(ctx, sms.Message{
		ID: job.ID.String(), To: job.Recipient, Body: job.Body, TrackStatus: true,
	})
	if err != nil {
		if recordErr := p.repository.FailSMSDelivery(ctx, messageID, err); recordErr != nil {
			return errors.Join(err, recordErr)
		}
		var deliveryError *sms.DeliveryError
		if errors.As(err, &deliveryError) && deliveryError.Temporary {
			return err
		}
		if releaseErr := p.repository.ReleaseSMSBudget(ctx, messageID); releaseErr != nil {
			return errors.Join(err, releaseErr)
		}
		if markErr := p.repository.MarkMessageFailed(ctx, messageID, err.Error()); markErr != nil {
			return errors.Join(err, markErr)
		}
		return fmt.Errorf("%w: %v", asynq.SkipRetry, err)
	}
	if err := p.repository.CompleteSMSDelivery(ctx, messageID, providerMessageID); err != nil {
		return fmt.Errorf("persist provider-accepted SMS: %w", err)
	}
	p.logger.InfoContext(ctx, "SMS accepted by provider",
		"message_id", messageID,
		"provider", p.smsTransport.Provider(),
		"provider_message_id", providerMessageID,
		"country_code", countryCode,
		"estimated_cost_usd", estimatedCost,
	)
	return nil
}

func (p *Processor) SendSMSStopConfirmation(
	ctx context.Context,
	provider, phone, eventID string,
) error {
	if p.smsTransport == nil {
		return errors.New("SMS stop confirmation transport is disabled")
	}
	if provider != p.smsTransport.Provider() {
		return fmt.Errorf("SMS opt-out webhook provider %q does not match configured provider %q",
			provider, p.smsTransport.Provider())
	}
	number, err := phonenumbers.Parse(phone, "")
	if err != nil || !phonenumbers.IsValidNumber(number) {
		return fmt.Errorf("%w: invalid SMS STOP confirmation recipient", asynq.SkipRetry)
	}
	countryCode := phonenumbers.GetRegionCodeForNumber(number)
	rate, hasRate := p.config.SMSCountryRatesUSD[countryCode]
	budget, hasBudget := p.config.SMSCountryMonthlyCapsUSD[countryCode]
	if !hasRate || !hasBudget {
		return fmt.Errorf("%w: SMS STOP confirmation country pricing and budget are not configured", asynq.SkipRetry)
	}
	accountID, err := uuid.Parse(p.config.AccountID)
	if err != nil {
		return fmt.Errorf("parse configured account ID for SMS confirmation: %w", err)
	}
	amount := float64(sms.SegmentCount(p.config.SMSStopConfirmation)) * rate
	if err := p.repository.ReserveSMSConfirmationBudget(
		ctx, eventID, accountID, countryCode, amount, budget, p.config.SMSRateLimitPerMinute,
	); err != nil {
		if errors.Is(err, store.ErrSMSCountryBudget) {
			return fmt.Errorf("%w: %v", asynq.SkipRetry, err)
		}
		return fmt.Errorf("reserve SMS STOP confirmation capacity: %w", err)
	}
	_, err = p.smsTransport.Send(ctx, sms.Message{
		ID:   "stop-confirmation-" + eventID,
		To:   phone,
		Body: p.config.SMSStopConfirmation,
	})
	if err != nil {
		var deliveryError *sms.DeliveryError
		if errors.As(err, &deliveryError) && deliveryError.Temporary {
			return fmt.Errorf("send SMS opt-out confirmation: %w", err)
		}
		if releaseErr := p.repository.ReleaseSMSConfirmationBudget(ctx, eventID); releaseErr != nil {
			return errors.Join(err, releaseErr)
		}
		if errors.As(err, &deliveryError) {
			return fmt.Errorf("%w: send SMS opt-out confirmation: %v", asynq.SkipRetry, err)
		}
		return fmt.Errorf("send SMS opt-out confirmation: %w", err)
	}
	return nil
}

func (p *Processor) failPermanently(ctx context.Context, messageID uuid.UUID, reason string) error {
	if err := p.repository.MarkMessageFailed(ctx, messageID, reason); err != nil {
		return fmt.Errorf("record permanent notification failure: %w", err)
	}
	return fmt.Errorf("%w: %s", asynq.SkipRetry, reason)
}

type netTXTResolver struct{}

func (netTXTResolver) LookupTXT(ctx context.Context, name string) ([]string, error) {
	return net.DefaultResolver.LookupTXT(ctx, name)
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
