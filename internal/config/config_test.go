package config

import "testing"

func setBaseEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("APP_PORT", "")
	t.Setenv("METRICS_PORT", "")
	t.Setenv("API_AUTH_MODE", "api_key")
	t.Setenv("MESSAGE_RETENTION_DAYS", "")
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "")
	t.Setenv("WORKER_CONCURRENCY", "")
	t.Setenv("EMAIL_WARMUP_INITIAL_LIMIT", "")
	t.Setenv("EMAIL_WARMUP_DAILY_INCREMENT", "")
	t.Setenv("EMAIL_WARMUP_MAXIMUM_LIMIT", "")
	t.Setenv("SMS_WARMUP_INITIAL_LIMIT", "")
	t.Setenv("SMS_WARMUP_DAILY_INCREMENT", "")
	t.Setenv("SMS_WARMUP_MAXIMUM_LIMIT", "")
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost/notify")
	t.Setenv("REDIS_URL", "localhost:6379")
	t.Setenv("ACCOUNT_ID", "00000000-0000-4000-8000-000000000001")
	t.Setenv("ASYNQ_QUEUE", "notifications")
	t.Setenv("LOG_LEVEL", "info")
	t.Setenv("DKIM_ENCRYPTION_KEY", "")
	t.Setenv("SENDGRID_WEBHOOK_PUBLIC_KEY", "")
	t.Setenv("EMAIL_TRANSPORT", "disabled")
	t.Setenv("EMAIL_FROM", "")
	t.Setenv("EMAIL_SPF_VALUE", "v=spf1 include:sendgrid.net ~all")
	t.Setenv("MARKETING_UNSUBSCRIBE_URL", "")
	t.Setenv("SENDGRID_API_KEY", "")
	t.Setenv("SENDGRID_TRANSACTIONAL_IP_POOL", "")
	t.Setenv("SENDGRID_MARKETING_IP_POOL", "")
	t.Setenv("SMTP_HOST", "")
	t.Setenv("SMTP_PORT", "")
	t.Setenv("SMTP_USERNAME", "")
	t.Setenv("SMTP_PASSWORD", "")
	t.Setenv("SMTP_REQUIRE_TLS", "")
	t.Setenv("SMS_TRANSPORT", "disabled")
	t.Setenv("SMS_FROM", "")
	t.Setenv("SMS_STATUS_CALLBACK_URL", "")
	t.Setenv("TWILIO_INBOUND_WEBHOOK_URL", "")
	t.Setenv("TWILIO_ACCOUNT_SID", "")
	t.Setenv("TWILIO_AUTH_TOKEN", "")
	t.Setenv("TELNYX_API_KEY", "")
	t.Setenv("TELNYX_WEBHOOK_PUBLIC_KEY", "")
	t.Setenv("SMS_RATE_LIMIT_PER_MINUTE", "")
	t.Setenv("SMS_COUNTRY_RATES_USD", "")
	t.Setenv("SMS_COUNTRY_MONTHLY_CAPS_USD", "")
	t.Setenv("SMS_STOP_CONFIRMATION", "")
}

func TestLoadRequiresCountryPricesAndCapsForSMS(t *testing.T) {
	setBaseEnvironment(t)
	t.Setenv("SMS_TRANSPORT", "twilio")
	t.Setenv("SMS_FROM", "+12025550123")
	t.Setenv("SMS_STATUS_CALLBACK_URL", "https://notify.example.org/internal/webhooks/sms/twilio")
	t.Setenv("TWILIO_INBOUND_WEBHOOK_URL", "https://notify.example.org/internal/webhooks/sms/twilio")
	t.Setenv("TWILIO_ACCOUNT_SID", "ACtest")
	t.Setenv("TWILIO_AUTH_TOKEN", "test-token")
	if _, err := Load(); err == nil {
		t.Fatal("Load() succeeded without SMS country rates and budgets")
	}
	t.Setenv("SMS_COUNTRY_RATES_USD", "US=0.01")
	t.Setenv("SMS_COUNTRY_MONTHLY_CAPS_USD", "US=100")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SMSTransport != "twilio" || cfg.SMSCountryRatesUSD["US"] != 0.01 || cfg.SMSCountryMonthlyCapsUSD["US"] != 100 {
		t.Fatalf("unexpected SMS configuration: %+v", cfg)
	}
}

func TestLoadRejectsInvalidSMSCountryConfiguration(t *testing.T) {
	setBaseEnvironment(t)
	t.Setenv("SMS_COUNTRY_RATES_USD", "USA=0.01")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted a non-ISO country code")
	}
	t.Setenv("SMS_COUNTRY_RATES_USD", "US=NaN")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted a non-finite SMS rate")
	}
}

func TestLoadRequiresDKIMKeyWhenSendingIsEnabled(t *testing.T) {
	setBaseEnvironment(t)
	t.Setenv("EMAIL_TRANSPORT", "sendgrid")
	t.Setenv("EMAIL_FROM", "alerts@example.org")
	t.Setenv("SENDGRID_API_KEY", "test-key")
	if _, err := Load(); err == nil {
		t.Fatal("Load() succeeded without DKIM_ENCRYPTION_KEY")
	}
}

func TestLoadRejectsInvalidDKIMKey(t *testing.T) {
	setBaseEnvironment(t)
	t.Setenv("DKIM_ENCRYPTION_KEY", "not-base64")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted an invalid DKIM_ENCRYPTION_KEY")
	}
}

func TestLoadAcceptsDisabledLocalConfiguration(t *testing.T) {
	setBaseEnvironment(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EmailTransport != "disabled" || cfg.HTTPPort != 8080 {
		t.Fatalf("unexpected local config: %+v", cfg)
	}
	if cfg.APIAuthMode != "api_key" || cfg.RetentionDays != 90 || cfg.WorkerConcurrency != 10 {
		t.Fatalf("unexpected M5 defaults: %+v", cfg)
	}
}

func TestLoadRejectsInvalidAPIAuthAndWarmupConfiguration(t *testing.T) {
	setBaseEnvironment(t)
	t.Setenv("API_AUTH_MODE", "none")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted an unsupported API_AUTH_MODE")
	}
	setBaseEnvironment(t)
	t.Setenv("EMAIL_WARMUP_INITIAL_LIMIT", "1000")
	t.Setenv("EMAIL_WARMUP_MAXIMUM_LIMIT", "999")
	if _, err := Load(); err == nil {
		t.Fatal("Load() accepted a warm-up maximum below its initial limit")
	}
}
