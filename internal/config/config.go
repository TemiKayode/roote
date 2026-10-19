package config

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"math"
	"net/mail"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPPort                  int
	DatabaseURL               string
	RedisURL                  string
	AccountID                 string
	AsynqQueue                string
	LogLevel                  slog.Level
	DKIMEncryptionKey         []byte
	SendGridWebhookPublicKey  string
	EmailTransport            string
	EmailFrom                 string
	EmailSPFValue             string
	MarketingUnsubscribeURL   string
	SendGridAPIKey            string
	SendGridTransactionalPool string
	SendGridMarketingPool     string
	SMTPHost                  string
	SMTPPort                  int
	SMTPUsername              string
	SMTPPassword              string
	SMTPRequireTLS            bool
	SMSTransport              string
	SMSFrom                   string
	SMSStatusCallbackURL      string
	TwilioInboundWebhookURL   string
	SMSStopConfirmation       string
	SMSRateLimitPerMinute     int
	SMSCountryRatesUSD        map[string]float64
	SMSCountryMonthlyCapsUSD  map[string]float64
	TwilioAccountSID          string
	TwilioAuthToken           string
	TelnyxAPIKey              string
	TelnyxWebhookPublicKey    string
}

func Load() (Config, error) {
	port, err := strconv.Atoi(env("APP_PORT", "8080"))
	if err != nil || port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("APP_PORT must be an integer between 1 and 65535")
	}

	level, err := parseLogLevel(env("LOG_LEVEL", "info"))
	if err != nil {
		return Config{}, err
	}
	encryptionKey, err := decodeOptionalBase64("DKIM_ENCRYPTION_KEY", os.Getenv("DKIM_ENCRYPTION_KEY"))
	if err != nil {
		return Config{}, err
	}
	smtpPort, err := strconv.Atoi(env("SMTP_PORT", "2525"))
	if err != nil || smtpPort < 1 || smtpPort > 65535 {
		return Config{}, fmt.Errorf("SMTP_PORT must be an integer between 1 and 65535")
	}
	requireTLS, err := strconv.ParseBool(env("SMTP_REQUIRE_TLS", "true"))
	if err != nil {
		return Config{}, fmt.Errorf("SMTP_REQUIRE_TLS must be true or false")
	}
	smsRateLimit, err := strconv.Atoi(env("SMS_RATE_LIMIT_PER_MINUTE", "60"))
	if err != nil || smsRateLimit < 1 {
		return Config{}, fmt.Errorf("SMS_RATE_LIMIT_PER_MINUTE must be a positive integer")
	}
	smsRates, err := parseCountryAmounts("SMS_COUNTRY_RATES_USD", os.Getenv("SMS_COUNTRY_RATES_USD"))
	if err != nil {
		return Config{}, err
	}
	smsCaps, err := parseCountryAmounts("SMS_COUNTRY_MONTHLY_CAPS_USD", os.Getenv("SMS_COUNTRY_MONTHLY_CAPS_USD"))
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		HTTPPort:          port,
		DatabaseURL:       strings.TrimSpace(os.Getenv("DATABASE_URL")),
		RedisURL:          strings.TrimSpace(os.Getenv("REDIS_URL")),
		AccountID:         strings.TrimSpace(os.Getenv("ACCOUNT_ID")),
		AsynqQueue:        env("ASYNQ_QUEUE", "notifications"),
		LogLevel:          level,
		DKIMEncryptionKey: encryptionKey,
		SendGridWebhookPublicKey: strings.ReplaceAll(
			strings.TrimSpace(os.Getenv("SENDGRID_WEBHOOK_PUBLIC_KEY")), `\n`, "\n",
		),
		EmailTransport:            env("EMAIL_TRANSPORT", "disabled"),
		EmailFrom:                 strings.TrimSpace(os.Getenv("EMAIL_FROM")),
		EmailSPFValue:             env("EMAIL_SPF_VALUE", "v=spf1 include:sendgrid.net ~all"),
		MarketingUnsubscribeURL:   strings.TrimSpace(os.Getenv("MARKETING_UNSUBSCRIBE_URL")),
		SendGridAPIKey:            strings.TrimSpace(os.Getenv("SENDGRID_API_KEY")),
		SendGridTransactionalPool: strings.TrimSpace(os.Getenv("SENDGRID_TRANSACTIONAL_IP_POOL")),
		SendGridMarketingPool:     strings.TrimSpace(os.Getenv("SENDGRID_MARKETING_IP_POOL")),
		SMTPHost:                  strings.TrimSpace(os.Getenv("SMTP_HOST")),
		SMTPPort:                  smtpPort,
		SMTPUsername:              strings.TrimSpace(os.Getenv("SMTP_USERNAME")),
		SMTPPassword:              os.Getenv("SMTP_PASSWORD"),
		SMTPRequireTLS:            requireTLS,
		SMSTransport:              env("SMS_TRANSPORT", "disabled"),
		SMSFrom:                   strings.TrimSpace(os.Getenv("SMS_FROM")),
		SMSStatusCallbackURL:      strings.TrimSpace(os.Getenv("SMS_STATUS_CALLBACK_URL")),
		TwilioInboundWebhookURL:   strings.TrimSpace(os.Getenv("TWILIO_INBOUND_WEBHOOK_URL")),
		SMSStopConfirmation:       env("SMS_STOP_CONFIRMATION", "You have been unsubscribed and will not receive further SMS messages."),
		SMSRateLimitPerMinute:     smsRateLimit,
		SMSCountryRatesUSD:        smsRates,
		SMSCountryMonthlyCapsUSD:  smsCaps,
		TwilioAccountSID:          strings.TrimSpace(os.Getenv("TWILIO_ACCOUNT_SID")),
		TwilioAuthToken:           strings.TrimSpace(os.Getenv("TWILIO_AUTH_TOKEN")),
		TelnyxAPIKey:              strings.TrimSpace(os.Getenv("TELNYX_API_KEY")),
		TelnyxWebhookPublicKey:    strings.TrimSpace(os.Getenv("TELNYX_WEBHOOK_PUBLIC_KEY")),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.RedisURL == "" {
		return Config{}, fmt.Errorf("REDIS_URL is required")
	}
	if cfg.AccountID == "" {
		return Config{}, fmt.Errorf("ACCOUNT_ID is required")
	}
	if cfg.AsynqQueue == "" {
		return Config{}, fmt.Errorf("ASYNQ_QUEUE must not be empty")
	}
	switch cfg.EmailTransport {
	case "disabled", "sendgrid", "kumo-smtp":
	default:
		return Config{}, fmt.Errorf("EMAIL_TRANSPORT must be disabled, sendgrid, or kumo-smtp")
	}
	if cfg.EmailTransport != "disabled" && cfg.EmailFrom == "" {
		return Config{}, fmt.Errorf("EMAIL_FROM is required when email delivery is enabled")
	}
	if cfg.EmailTransport != "disabled" && len(cfg.DKIMEncryptionKey) != 32 {
		return Config{}, fmt.Errorf("DKIM_ENCRYPTION_KEY is required when email delivery is enabled")
	}
	if cfg.EmailFrom != "" {
		if _, err := mail.ParseAddress(cfg.EmailFrom); err != nil {
			return Config{}, fmt.Errorf("EMAIL_FROM must be a valid email address: %w", err)
		}
	}
	if cfg.EmailTransport == "sendgrid" && cfg.SendGridAPIKey == "" {
		return Config{}, fmt.Errorf("SENDGRID_API_KEY is required when EMAIL_TRANSPORT=sendgrid")
	}
	if cfg.EmailTransport == "kumo-smtp" && cfg.SMTPHost == "" {
		return Config{}, fmt.Errorf("SMTP_HOST is required when EMAIL_TRANSPORT=kumo-smtp")
	}
	if cfg.EmailTransport == "kumo-smtp" && cfg.SMTPUsername != "" && !cfg.SMTPRequireTLS {
		return Config{}, fmt.Errorf("SMTP_REQUIRE_TLS must be true when SMTP authentication is configured")
	}
	switch cfg.SMSTransport {
	case "disabled", "twilio", "telnyx":
	default:
		return Config{}, fmt.Errorf("SMS_TRANSPORT must be disabled, twilio, or telnyx")
	}
	if cfg.SMSTransport != "disabled" {
		if cfg.SMSFrom == "" {
			return Config{}, fmt.Errorf("SMS_FROM is required when SMS delivery is enabled")
		}
		if cfg.SMSStatusCallbackURL == "" {
			return Config{}, fmt.Errorf("SMS_STATUS_CALLBACK_URL is required when SMS delivery is enabled")
		}
		if !isPublicHTTPSURL(cfg.SMSStatusCallbackURL) {
			return Config{}, fmt.Errorf("SMS_STATUS_CALLBACK_URL must be an absolute HTTPS URL")
		}
		if len(cfg.SMSCountryRatesUSD) == 0 || len(cfg.SMSCountryMonthlyCapsUSD) == 0 {
			return Config{}, fmt.Errorf("SMS_COUNTRY_RATES_USD and SMS_COUNTRY_MONTHLY_CAPS_USD are required when SMS delivery is enabled")
		}
		if len(cfg.SMSCountryRatesUSD) != len(cfg.SMSCountryMonthlyCapsUSD) {
			return Config{}, fmt.Errorf("SMS country rates and monthly caps must define the same countries")
		}
		for country := range cfg.SMSCountryRatesUSD {
			if _, ok := cfg.SMSCountryMonthlyCapsUSD[country]; !ok {
				return Config{}, fmt.Errorf("SMS_COUNTRY_MONTHLY_CAPS_USD is missing country %s", country)
			}
		}
	}
	if cfg.SMSTransport == "twilio" && (cfg.TwilioAccountSID == "" || cfg.TwilioAuthToken == "") {
		return Config{}, fmt.Errorf("TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN are required when SMS_TRANSPORT=twilio")
	}
	if cfg.SMSTransport == "twilio" && !isPublicHTTPSURL(cfg.TwilioInboundWebhookURL) {
		return Config{}, fmt.Errorf("TWILIO_INBOUND_WEBHOOK_URL must be an absolute HTTPS URL")
	}
	if cfg.SMSTransport == "telnyx" && (cfg.TelnyxAPIKey == "" || cfg.TelnyxWebhookPublicKey == "") {
		return Config{}, fmt.Errorf("TELNYX_API_KEY and TELNYX_WEBHOOK_PUBLIC_KEY are required when SMS_TRANSPORT=telnyx")
	}
	if !strings.HasPrefix(cfg.EmailSPFValue, "v=spf1 ") || strings.ContainsAny(cfg.EmailSPFValue, "\r\n") {
		return Config{}, fmt.Errorf("EMAIL_SPF_VALUE must be a single-line SPF record starting with 'v=spf1 '")
	}
	return cfg, nil
}

func isPublicHTTPSURL(value string) bool {
	parsed, err := url.ParseRequestURI(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil
}

func parseCountryAmounts(name, value string) (map[string]float64, error) {
	result := make(map[string]float64)
	if strings.TrimSpace(value) == "" {
		return result, nil
	}
	for _, item := range strings.Split(value, ",") {
		parts := strings.Split(strings.TrimSpace(item), "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("%s entries must use COUNTRY=amount format", name)
		}
		country := strings.ToUpper(strings.TrimSpace(parts[0]))
		if len(country) != 2 || country[0] < 'A' || country[0] > 'Z' || country[1] < 'A' || country[1] > 'Z' {
			return nil, fmt.Errorf("%s country keys must be two-letter ISO codes", name)
		}
		amount, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if err != nil || math.IsNaN(amount) || math.IsInf(amount, 0) ||
			amount < 0.000001 || amount > 999999.999999 {
			return nil, fmt.Errorf("%s amount for %s must be between 0.000001 and 999999.999999 USD", name, country)
		}
		if _, duplicate := result[country]; duplicate {
			return nil, fmt.Errorf("%s contains duplicate country %s", name, country)
		}
		result[country] = amount
	}
	return result, nil
}

func decodeOptionalBase64(key, value string) ([]byte, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return nil, fmt.Errorf("%s must be standard base64: %w", key, err)
	}
	if len(decoded) != 32 {
		return nil, fmt.Errorf("%s must decode to exactly 32 bytes", key)
	}
	return decoded, nil
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("LOG_LEVEL must be debug, info, warn, or error")
	}
}
