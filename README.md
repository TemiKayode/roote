# Roote

**An open-source Go notification API for transactional email and SMS.**

Roote accepts notifications over a REST API and delivers them asynchronously. It combines PostgreSQL, Redis, and Asynq with pluggable delivery providers, so an application can submit a notification once and track its progress without coupling itself to a specific email or SMS vendor.

> Roote is under active development. Review [Current limitations](IMPLEMENTATION.md#current-limitations-and-next-decisions) and complete the provider and security configuration before using it in production.

## Features

- **Asynchronous delivery:** PostgreSQL transactional outbox with Redis and Asynq workers, retries, and idempotent notification acceptance.
- **Email:** SendGrid HTTP API and SMTP transports, including SMTP-compatible MTAs such as KumoMTA; domain verification, DKIM signing, unsubscribe support, and delivery callbacks.
- **SMS:** Twilio and Telnyx transports, E.164 number validation, explicit consent, opt-out processing, delivery receipts, and country-level budgets.
- **Routing and templates:** Explicit, preference-based, or waterfall channel routing; versioned, localized templates with safe HTML rendering.
- **Customer webhooks:** Signed, durable delivery events with retries.
- **Operations:** Scoped API keys, sender warm-up limits, retention controls, audit events, health checks, and Prometheus-format metrics.
- **Tracing:** OpenTelemetry traces exported over OTLP/HTTP, including trace context propagated from API requests through queued work.

## Technology

Go 1.24+, Gin, PostgreSQL, Redis, Asynq, and OpenTelemetry.

## Quick start

Requirements: Go 1.24+ and Docker Compose.

```sh
git clone https://github.com/TemiKayode/roote.git
cd roote
cp .env.example .env
docker compose up --build -d
```

The local Compose setup binds the API to loopback and uses development authentication. Check that the API and its dependencies are ready:

```sh
curl http://localhost:8080/readyz
```

Submit a notification:

```sh
curl -i http://localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: welcome-001' \
  -d '{
    "event": "welcome",
    "channels": ["email"],
    "traffic_class": "transactional",
    "recipient": {"email": "person@example.com"},
    "content": {"subject": "Welcome", "body": "Thanks for signing up."}
  }'
```

The API responds with `202 Accepted` and a message ID. Email and SMS transports are disabled by default, so configure a provider in `.env` before expecting real delivery. See the [implementation guide](IMPLEMENTATION.md) for provider setup, API keys, consent, webhooks, operations, and deployment details.

## Security and deployment

The checked-in Compose defaults are for local loopback development only. Before deployment:

1. Set `API_AUTH_MODE=api_key`; do not expose the API with development authentication.
2. Configure provider credentials and persistent encryption keys. Keep `.env` and credentials out of version control.
3. Configure signed provider callbacks, SMS consent capture, and applicable delivery budgets and retention requirements.
4. Review the [deployment and incident guidance](IMPLEMENTATION.md#secrets-backups-deployment-and-incidents) and the [known limitations](IMPLEMENTATION.md#current-limitations-and-next-decisions).

## Development checks

```sh
go test ./...
go vet ./...
```

Store lifecycle integration tests also require `TEST_DATABASE_URL` pointing to a disposable PostgreSQL database:

```sh
go test ./internal/store -count=1
```

For the full build, configuration, API examples, and operational runbook, see [IMPLEMENTATION.md](IMPLEMENTATION.md).
