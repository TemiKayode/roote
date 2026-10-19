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
	IsNotificationPreferred(context.Context, uuid.UUID, string, string) (bool, error)
	BeginEmailDelivery(context.Context, store.EmailJob, string) (bool, error)
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
	RecordChannelFailure(context.Context, uuid.UUID, string, string) error
	RecordChannelSkipped(context.Context, uuid.UUID, string) error
	EmitNotificationFailure(context.Context, uuid.UUID) error
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
	emailJob, emailErr := p.repository.GetEmailJob(ctx, messageID)
	smsJob, smsErr := p.repository.GetSMSJob(ctx, messageID)
	if emailErr != nil && smsErr != nil {
		return fmt.Errorf("load notification for delivery: %w", errors.Join(emailErr, smsErr))
	}
	if emailErr != nil && contains(smsJob.Channels, "email") {
		return fmt.Errorf("load email notification data: %w", emailErr)
	}
	if smsErr != nil && contains(emailJob.Channels, "sms") {
		return fmt.Errorf("load SMS notification data: %w", smsErr)
	}
	channels := emailJob.Channels
	if len(channels) == 0 {
		channels = smsJob.Channels
	}
	if !contains(channels, "email") && !contains(channels, "sms") {
		return p.failPermanently(ctx, messageID, "notification has no supported delivery channel")
	}
	routing := emailJob.Routing
	if emailErr != nil {
		routing = smsJob.Routing
	}
	strategy := routing.Strategy
	if strategy == "" {
		strategy = "explicit"
	}
	selectedChannels := append([]string(nil), channels...)
	if strategy == "preference" {
		selectedChannels = selectedChannels[:0]
		for _, channel := range channels {
			recipient := emailJob.Recipient
			if channel == "sms" {
				recipient = smsJob.Recipient
			}
			preferred, err := p.repository.IsNotificationPreferred(ctx, smsJob.AccountID, recipient, channel)
			if err != nil {
				return fmt.Errorf("check %s notification preference: %w", channel, err)
			}
			if preferred {
				selectedChannels = append(selectedChannels, channel)
			}
		}
		if len(selectedChannels) == 0 {
			for _, channel := range channels {
				if err := p.repository.RecordChannelFailure(
					ctx, messageID, channel, "channel is disabled in recipient preferences",
				); err != nil {
					return err
				}
			}
			return p.finalizeChannelError(ctx, messageID, fmt.Errorf(
				"%w: no requested notification channel is enabled in recipient preferences", asynq.SkipRetry,
			))
		}
		selected := make(map[string]struct{}, len(selectedChannels))
		for _, channel := range selectedChannels {
			selected[channel] = struct{}{}
		}
		for _, channel := range channels {
			if _, enabled := selected[channel]; !enabled {
				if err := p.repository.RecordChannelSkipped(ctx, messageID, channel); err != nil {
					return err
				}
			}
		}
	}
	if strategy == "waterfall" {
		ordered := routing.Order
		if len(ordered) == 0 {
			ordered = channels
		}
		var failures []error
		for _, channel := range ordered {
			err := p.deliverChannel(ctx, channel, messageID, emailJob)
			if err == nil {
				return nil
			}
			failures = append(failures, fmt.Errorf("%s delivery: %w", channel, err))
		}
		return p.finalizeChannelError(ctx, messageID, aggregateChannelErrors(failures))
	}
	if len(selectedChannels) == 1 {
		return p.finalizeChannelError(ctx, messageID, p.deliverChannel(ctx, selectedChannels[0], messageID, emailJob))
	}
	results := make(chan error, len(selectedChannels))
	for _, channel := range selectedChannels {
		channel := channel
		go func() { results <- p.deliverChannel(ctx, channel, messageID, emailJob) }()
	}
	var failures []error
	successes := 0
	for range selectedChannels {
		if err := <-results; err != nil {
			failures = append(failures, err)
		} else {
			successes++
		}
	}
	if len(failures) > 0 {
		aggregated := aggregateChannelErrors(failures)
		if successes > 0 && errors.Is(aggregated, asynq.SkipRetry) {
			return nil
		}
		return p.finalizeChannelError(ctx, messageID, aggregated)
	}
	return nil
}

func (p *Processor) finalizeChannelError(ctx context.Context, messageID uuid.UUID, err error) error {
	if err == nil || !errors.Is(err, asynq.SkipRetry) {
		return err
	}
	if recordErr := p.repository.EmitNotificationFailure(ctx, messageID); recordErr != nil {
		return fmt.Errorf("record terminal notification failure: %w", recordErr)
	}
	return err
}

func aggregateChannelErrors(failures []error) error {
	var retryable []error
	for _, err := range failures {
		if !errors.Is(err, asynq.SkipRetry) {
			retryable = append(retryable, err)
		}
	}
	if len(retryable) > 0 {
		return fmt.Errorf("one or more notification channels failed: %w", errors.Join(retryable...))
	}
	return fmt.Errorf("%w: all requested notification channels failed: %v",
		asynq.SkipRetry, errors.Join(failures...))
}

func (p *Processor) deliverChannel(
	ctx context.Context,
	channel string,
	messageID uuid.UUID,
	emailJob store.EmailJob,
) error {
	switch channel {
	case "email":
		return p.deliverEmail(ctx, emailJob)
	case "sms":
		return p.DeliverSMS(ctx, messageID)
	default:
		return p.failPermanently(ctx, messageID, "notification has no supported delivery channel")
	}
}

func (p *Processor) deliverEmail(ctx context.Context, job store.EmailJob) error {
	messageID := job.ID
	if p.transport == nil {
		return p.failChannelPermanently(ctx, messageID, "email", "email transport is disabled; configure EMAIL_TRANSPORT")
	}
	suppressed, err := p.repository.IsEmailSuppressed(ctx, job.AccountID, job.Recipient)
	if err != nil {
		return fmt.Errorf("check recipient suppression: %w", err)
	}
	if suppressed {
		return p.failChannelPermanently(ctx, messageID, "email", "recipient is suppressed for email")
	}
	from, err := mail.ParseAddress(p.config.EmailFrom)
	if err != nil {
		return p.failChannelPermanently(ctx, messageID, "email", "configured EMAIL_FROM is invalid")
	}
	parts := strings.Split(from.Address, "@")
	if len(parts) != 2 {
		return p.failChannelPermanently(ctx, messageID, "email", "configured EMAIL_FROM has no domain")
	}
	dkimKey, err := p.repository.GetDKIMKey(ctx, job.AccountID, strings.ToLower(parts[1]))
	if err != nil {
		return p.failChannelPermanently(ctx, messageID, "email", "sender domain has not been verified")
	}
	privateKeyPEM, err := emaildomain.DecryptPrivateKey(p.config.DKIMEncryptionKey, dkimKey.EncryptedPrivateKey)
	if err != nil {
		return p.failChannelPermanently(ctx, messageID, "email", "could not decrypt sender DKIM key")
	}
	if job.TrafficClass == "" {
		job.TrafficClass = "transactional"
	}
	message, err := email.NewMessage(
		job.ID.String(), p.config.EmailFrom, job.Recipient, job.Subject, job.Body,
		job.TrafficClass, p.config.MarketingUnsubscribeURL, dkimKey.Selector, privateKeyPEM,
	)
	if err != nil {
		return p.failChannelPermanently(ctx, messageID, "email", err.Error())
	}
	shouldSend, err := p.repository.BeginEmailDelivery(ctx, job, p.transport.Provider())
	if err != nil {
		return err
	}
	if !shouldSend {
		return nil
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
		return p.failChannelPermanently(ctx, messageID, "sms", "notification does not request SMS")
	}
	if p.smsTransport == nil {
		return p.failChannelPermanently(ctx, messageID, "sms", "SMS transport is disabled; configure SMS_TRANSPORT")
	}
	countryNumber, err := phonenumbers.Parse(job.Recipient, "")
	if err != nil || !phonenumbers.IsValidNumber(countryNumber) {
		return p.failChannelPermanently(ctx, messageID, "sms", "recipient phone number is invalid")
	}
	countryCode := phonenumbers.GetRegionCodeForNumber(countryNumber)
	rate, hasRate := p.config.SMSCountryRatesUSD[countryCode]
	budget, hasBudget := p.config.SMSCountryMonthlyCapsUSD[countryCode]
	if !hasRate || !hasBudget {
		return p.failChannelPermanently(ctx, messageID, "sms", "SMS country pricing and monthly budget are not configured")
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
		return p.failChannelPermanently(ctx, messageID, "sms", "active SMS consent is required")
	}
	suppressed, err := p.repository.IsSMSSuppressed(ctx, job.AccountID, job.Recipient)
	if err != nil {
		return fmt.Errorf("check SMS suppression: %w", err)
	}
	if suppressed {
		return p.failChannelPermanently(ctx, messageID, "sms", "recipient is suppressed for SMS")
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
			return p.failChannelPermanently(ctx, messageID, "sms", err.Error())
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

func (p *Processor) failChannelPermanently(
	ctx context.Context,
	messageID uuid.UUID,
	channel, reason string,
) error {
	if err := p.repository.RecordChannelFailure(ctx, messageID, channel, reason); err != nil {
		return fmt.Errorf("record permanent %s failure: %w", channel, err)
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
