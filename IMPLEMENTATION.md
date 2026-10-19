# Notification Platform: Build Plan

## Product goal

Build a developer-facing, multi-channel notification platform for email and SMS. The service will accept asynchronous send requests, route them according to caller intent and recipient consent, record delivery state, and expose authenticated APIs and webhooks. Provider integrations remain behind channel-specific transport interfaces.

## Architecture decisions

- **Go + Gin:** HTTP API and request validation.
- **PostgreSQL + pgx + goose:** durable application data and versioned migrations.
- **Redis + Asynq:** asynchronous task processing with retries and queue isolation.
- **Transactional outbox:** the API writes a message and its outbox row in one PostgreSQL transaction. The worker relays outbox rows to Redis and marks them sent only after enqueue succeeds. This closes the database/queue dual-write gap. A deterministic Asynq task ID reduces duplicates if a process crashes between enqueue and marking; actual delivery handlers must still be idempotent.
- **Explicit interfaces:** orchestration will depend on email and SMS transport interfaces, not provider SDKs.
- **Local-first setup:** Docker Compose provides PostgreSQL, Redis, API, and worker. The API port is published on loopback only until authentication is implemented. The SMTP adapter can target a configured KumoMTA host; the MTA itself is deployed separately because its routing, signing, and TLS policy require operator-specific configuration.
- **No production claims from scaffolding:** the first milestone has no customer authentication, real delivery, consent-based routing, or provider credentials. Those are explicit later milestones, not silent fallbacks.

The Go module path is `github.com/TemiKayode/roote`.

## Implementation sequence

### Milestone 1 — Foundation (complete)

- [x] Establish the project module and local Compose services.
- [x] Create the initial PostgreSQL schema and development account bootstrap.
- [x] Add `/healthz`, `/readyz`, and `POST /v1/notifications`.
- [x] Persist accepted messages with account-scoped idempotency and an outbox row.
- [x] Relay outbox rows to Asynq; consume tasks in a logging-only worker.
- [x] Run formatting, tests, and vet; fix issues found.
- [x] Build and start the Compose stack; verify health, readiness, worker consumption, and idempotency behavior.

Acceptance: a valid request receives `202` and a stable message ID; replaying the same idempotency key and payload returns that ID without another message; reusing the key with a different payload returns `409`.

### Milestone 2 — Email and domains (implementation and local Compose verification complete)

- [x] Add domain registration, DNS record generation, AES-GCM encrypted DKIM key storage, and queued DNS verification with Asynq retries.
- [x] Define the consumer-side `EmailTransport` interface.
- [x] Add a SendGrid HTTP adapter and KumoMTA-compatible SMTP adapter. SMTP messages are composed and DKIM-signed before submission; SendGrid applies its provider-managed domain signature.
- [x] Add marketing unsubscribe headers, traffic-class IP pool selection, delivery attempts, and provider-acceptance/delivery state transitions.
- [x] Verify signed SendGrid event webhooks and process hard bounces, complaints, and delivery confirmations into email-only suppression/state updates.
- [x] Add unit tests for DNS records and checks, AES-GCM key handling, message signing, HTTP provider behavior/error classification, webhook signatures, routing, and API endpoints.
- [x] Validate migrations and email API lifecycle against a clean local Compose database.

### Milestone 3 — SMS

- [x] Define the consumer-side `SmsTransport` interface and add Twilio and Telnyx adapters.
- [x] Validate E.164 numbers with phone-number metadata and capture channel/scope-specific consent.
- [x] Reserve an account send-rate slot and a per-country monthly USD budget before provider calls.
- [x] Verify signed provider callbacks, persist delivery receipts, promptly suppress STOP/UNSUBSCRIBE senders, and queue an opt-out confirmation.
- [x] Test provider request/auth behavior, consent checks, SMS segment pricing, webhook signatures, and opt-out handling.
- [x] Apply the SMS migration and verify the local Compose API/worker lifecycle.

### Milestone 4 — Orchestration and customer webhooks

- Add explicit, preference-based, and waterfall routing with channel-specific preferences and suppression checks.
- Persist each attempt and state transition; make waterfall decisions queryable.
- Use bounded parallelism for fan-out and controlled fallback for failures/timeouts.
- Sign outbound webhook payloads with HMAC-SHA256 and retry delivery through Asynq.
- Test routing matrices, fallback, retries, and isolation between email and SMS consent/suppression.

### Milestone 5 — Operations, compliance, and hardening

- Add correlation IDs, structured logs, Prometheus metrics, readiness checks, and graceful shutdown.
- Add IP warm-up tracking/throttling, regional routing fields, and operational dashboards/queries.
- Review API-key hashing, revocation/scopes, encryption-key management, retention/deletion, and auditability.
- Exercise retry exhaustion, dead-letter operations, provider outages, throughput limits, and backpressure.
- Document deployment, secrets, backups, migrations, incident response, and provider onboarding.

## Build steps

1. Install Go 1.24+ and Docker Desktop with Compose.
2. From the repository root, copy `.env.example` to `.env`. Keep `.env` out of version control; `.gitignore` already excludes it.
3. Generate a persistent 32-byte DKIM encryption key and set `DKIM_ENCRYPTION_KEY` to its standard-base64 encoding. Protect and back it up: losing it makes stored DKIM private keys undecryptable.
4. For actual email delivery, configure `EMAIL_TRANSPORT`, `EMAIL_FROM`, and the corresponding provider credentials. `EMAIL_TRANSPORT=disabled` is the local default; queued email tasks will be marked failed rather than silently reported as delivered.
5. For SendGrid, configure domain authentication in SendGrid as well as publishing the service's DNS records. SendGrid signs the HTTP-submitted message with its provider-managed key; the generated key is used for SMTP delivery. Set `SENDGRID_WEBHOOK_PUBLIC_KEY` to the Event Webhook verification key to enable signed bounce/complaint callbacks. Set `EMAIL_SPF_VALUE` to the SPF value appropriate for the selected delivery path.
6. To enable SMS, set `SMS_TRANSPORT` to `twilio` or `telnyx`, set `SMS_FROM` and `SMS_STATUS_CALLBACK_URL`, and provide the corresponding provider credentials. Twilio additionally needs `TWILIO_INBOUND_WEBHOOK_URL`; Telnyx needs `TELNYX_WEBHOOK_PUBLIC_KEY`. Configure both `SMS_COUNTRY_RATES_USD` and `SMS_COUNTRY_MONTHLY_CAPS_USD` as comma-separated `ISO=amount` pairs (for example, `US=0.01,GB=0.05`). Rates are estimated USD per SMS segment; budgets reserve that estimate for the current UTC month. The account send limit is a fixed per-minute window. Unconfigured destination countries fail closed.
7. Configure the provider's inbound webhook to `/internal/webhooks/sms/twilio` or `/internal/webhooks/sms/telnyx`. Twilio status callbacks are generated from `SMS_STATUS_CALLBACK_URL`; set it to the matching public Twilio webhook endpoint. Telnyx delivery callbacks use that URL from each send request. Do not expose the API until customer authentication and webhook exposure controls are implemented.
8. Before sending SMS, record the recipient's explicit consent:

   ```powershell
   $consent = @{
     phone = "+14155552671"
     scope = "transactional"
     source = "signup-form"
     consented = $true
   } | ConvertTo-Json
   Invoke-RestMethod -Method Post -Uri "http://localhost:8080/v1/consents/sms" `
     -ContentType "application/json" -Body $consent
   ```

   Use `scope = "marketing"` for marketing permission or `scope = "all"` only when the captured consent explicitly covers both. `consented = false` withdraws consent and adds an SMS suppression. Inbound STOP, STOPALL, UNSUBSCRIBE, CANCEL, END, QUIT, and REVOKE commands withdraw all SMS scopes and queue a confirmation.
9. Start services with `docker compose up --build`.
10. Check `http://localhost:8080/healthz` and `http://localhost:8080/readyz`.
11. Register a domain and publish the returned records:

   ```powershell
   Invoke-RestMethod -Method Post -Uri "http://localhost:8080/v1/domains" `
     -ContentType "application/json" -Body '{"domain":"mail.example.org"}'
   ```

   Merge the SPF mechanism into an existing SPF record, if present; do not publish multiple SPF or DMARC TXT records for the same owner name. After DNS has propagated, trigger verification with `POST /v1/domains/{id}/verify`; retries run in the worker.

12. Submit a transactional email request:

   ```powershell
   $headers = @{
     "Content-Type" = "application/json"
     "Idempotency-Key" = "demo-001"
   }
   $body = @{
     event = "welcome"
     channels = @("email")
     traffic_class = "transactional"
     recipient = @{ email = "person@example.com" }
     content = @{ subject = "Welcome"; body = "Thanks for signing up." }
   } | ConvertTo-Json -Depth 5
   Invoke-RestMethod -Method Post -Uri "http://localhost:8080/v1/notifications" -Headers $headers -Body $body
   ```

13. Repeat with the same key and payload (same message ID), then change the payload but keep the key (HTTP 409).
14. Watch `docker compose logs -f worker` and check the `email_deliveries` / `sms_deliveries` tables: provider acceptance is not final delivery; signed callbacks update delivery state.
15. Run local checks with `go test ./...` and `go vet ./...`. To run the PostgreSQL-backed SMS consent, cap, rate-limit, receipt, and STOP lifecycle test, set `TEST_DATABASE_URL` to a disposable PostgreSQL database and run `go test ./internal/store -run '^TestSMSStoreControlsAndWebhooks$' -count=1`.
16. Stop the stack with `docker compose down`. PostgreSQL data is persisted in a named volume; removing it is destructive and is not part of routine shutdown.

## Current limitations and next decisions

- The development account ID is configured through `ACCOUNT_ID`; authentication and tenant isolation must be implemented before exposing this service publicly.
- Email-only and SMS-only notifications are delivered in Milestones 2 and 3. Mixed-channel requests remain explicitly unavailable until orchestration in Milestone 4.
- Customer authentication, tenant authorization, durable public webhook ingress, and outbound customer webhooks remain future milestones. Consent submitted through the development API is not independently verified; integrate authenticated consent capture before production.
- SMS country prices are operator-provided estimates and budget reservations, not reconciled provider invoices. Update rates and caps whenever provider pricing changes; current-period estimates should be reconciled operationally.
- The SMTP adapter signs messages with the stored DKIM key. For SendGrid, configure SendGrid's own domain authentication records; provider signing is not performed with the private key stored here.
- Exactly-once delivery cannot be guaranteed across a provider acceptance followed by a process crash before PostgreSQL records the response. Providers may accept a retry as a duplicate; this remains an at-least-once system.
- Configure secrets from a secret manager in deployed environments; never reuse `.env.example` credentials.
- Before production, choose the product/module path, deployment region/provider, retention policy, and key-management approach.
