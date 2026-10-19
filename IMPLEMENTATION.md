# Notification Platform: Build Plan

## Product goal

Build a developer-facing, multi-channel notification platform for email and SMS. The service will accept asynchronous send requests, route them according to caller intent and recipient consent, record delivery state, and expose authenticated APIs and webhooks. Provider integrations remain behind channel-specific transport interfaces.

## Architecture decisions

- **Go + Gin:** HTTP API and request validation.
- **PostgreSQL + pgx + goose:** durable application data and versioned migrations.
- **Redis + Asynq:** asynchronous task processing with retries and queue isolation.
- **Transactional outbox:** the API writes a message and its outbox row in one PostgreSQL transaction. The worker relays outbox rows to Redis and marks them sent only after enqueue succeeds. This closes the database/queue dual-write gap. A deterministic Asynq task ID reduces duplicates if a process crashes between enqueue and marking; actual delivery handlers must still be idempotent.
- **Explicit interfaces:** orchestration will depend on email and SMS transport interfaces, not provider SDKs.
- **Local-first setup:** Docker Compose provides PostgreSQL, Redis, API, and worker. The local Compose profile explicitly uses development authentication and publishes the API on loopback only. Deployed environments must use scoped API keys. The SMTP adapter can target a configured KumoMTA host; the MTA itself is deployed separately because its routing, signing, and TLS policy require operator-specific configuration.
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

- [x] Add explicit, preference-based, and waterfall routing with channel-specific preferences and suppression checks.
- [x] Persist channel attempts and reconcile accepted, delivered, failed, skipped, and partially delivered states.
- [x] Fan out to at most the two supported channels concurrently; waterfall routes attempt channels in order and fall back after synchronous provider failures.
- [x] Add HTTPS customer webhook registration, encrypted signing secrets, HMAC-SHA256 signatures, durable delivery records, and Asynq retries.
- [x] Test route selection, fan-out, fallback, preference handling, URL restrictions, signatures, encryption, and webhook persistence.
- [x] Apply migration 4 and verify the local Compose API/worker lifecycle.

### Milestone 5 — Operations, compliance, and hardening (implementation complete)

- [x] Add validated request IDs, structured request logs, Prometheus-format API/worker metrics, PostgreSQL/Redis readiness checks, and bounded graceful shutdown.
- [x] Add durable per-sender daily warm-up limits, configurable ramp-up defaults, regional notification metadata, and operational summaries.
- [x] Add SHA-256-hashed API keys with scopes, expiry, revocation, last-use tracking, and audit events.
- [x] Add message retention/deletion with audit records; retain consent, suppression, and audit evidence independently.
- [x] Expose archived Asynq tasks for scoped inspection/retry and persist terminal message state after notification retries are exhausted.
- [x] Document authentication, provisioning, limits, queue recovery, secret handling, backups, migrations, and incident response.
- [x] Run PostgreSQL-backed M5 lifecycle checks and deployment/runtime smoke tests before release.

Verified on 2026-10-19: all store integration tests pass against PostgreSQL 17 with migrations 1–6, and an `api_key`-mode Compose stack passed a smoke test covering key creation/revocation, scope enforcement, idempotent replay/conflict, waterfall fallback, terminal failure, dead-letter listing, audit events, worker readiness/metrics, and graceful shutdown.

### Review fixes (2026-10-19)

- `POST /v1/consents/sms` now requires the `consents:write` scope; it was previously reachable without an API key in `api_key` mode.
- SendGrid Event Webhook `custom_args` are read from top-level event properties, as SendGrid sends them. Events without a `message_id`, or for deleted/mismatched messages, are skipped individually instead of failing the whole batch; SMS status callbacks for deleted messages return `200` with `processed: 0`.
- The customer webhook relay refreshes `queued_at` on every relay. Previously a delivery waiting in a long Asynq retry backoff was reselected on every loop, starving newer deliveries.
- Waterfall routing records unattempted channels as skipped, and a waterfall whose fallback delivers is `delivered` rather than `partially_delivered`. Previously a waterfall whose first channel succeeded never reached a terminal status.
- A provider-accepted message reports `processing` until a delivery receipt arrives; it no longer regresses to `queued`.
- Deleted messages are not retried by the worker; transient DKIM-key and webhook-delivery lookup errors are retried instead of being recorded as permanent failures.
- Customer webhook URLs and resolved addresses block all special-purpose ranges, including CGNAT (`100.64.0.0/10`) and NAT64 (`64:ff9b::/96`).
- `GET /metrics` on the API requires `ops:read` in `api_key` mode.
- SPF verification checks that each required mechanism is present, so records merged with other providers or using `-all` verify.
- `ACCOUNT_ID` must be a UUID at startup. Migration 6 backfills `webhook_deliveries.source_message_id` so retention and deletion cascade to pre-M5 webhook rows, and indexes case-insensitive suppression and preference lookups.

### Roadmap progress (2026-10-19)

Phase 1 item 1 (read APIs) and the two small Phase 2 items are complete; see the product roadmap for what follows.

| Endpoint | Scope | Purpose |
| --- | --- | --- |
| `GET /v1/messages/{id}` | `messages:read` | Message status, recipient, routing, per-channel deliveries, attempts, and event timeline. Message content is not returned. |
| `GET /v1/messages?status=&recipient=&since=&until=&limit=&cursor=` | `messages:read` | Newest-first search; `recipient` is an email address or E.164 number, `since`/`until` are RFC 3339, `limit` is 1–200 (default 50); pass `next_cursor` back as `cursor`. |
| `GET /v1/webhooks` | `webhooks:read` | List endpoints, active and inactive. |
| `DELETE /v1/webhooks/{id}` | `webhooks:write` | Deactivate an endpoint; its pending deliveries are marked failed. |
| `POST /v1/webhooks/{id}/rotate-secret` | `webhooks:write` | Replace the signing secret; the new secret is returned once and the old one stops working immediately. |
| `GET /v1/domains` | `domains:read` | List sending domains with verification status and last error. |
| `GET /v1/suppressions?channel=&recipient=&limit=&cursor=` | `suppressions:read` | List suppressions, newest first. |
| `DELETE /v1/suppressions?channel=email&recipient=` | `suppressions:write` | Remove a hard-bounce suppression. Complaints (`409`) and SMS opt-outs (`400`) cannot be removed through the API; SMS opt-outs clear only with new consent. |
| `GET /v1/consents/sms?phone=` | `consents:read` | Consent history for a number, newest first. |

- **Queue isolation.** Work runs on weighted Asynq queues named `<ASYNQ_QUEUE>-transactional` (weight 6), `-webhooks` (2), `-marketing` (2), and `-system` (1, domain verification). Marketing notifications use the marketing queue; transactional notifications and STOP confirmations use the transactional queue. The worker also drains the bare `ASYNQ_QUEUE` so tasks enqueued before this change finish.
- **Batched outbox relay.** The notification and webhook relays move up to 100 rows per transaction and wake on PostgreSQL `LISTEN/NOTIFY` when work is committed, with a 5-second poll as a fallback.
- **Audited acceptance.** The `notification.accepted` audit event is written in the same transaction as the message.
- **Domain re-verification.** A verified domain stays `verified` while it is re-checked, so sending is not interrupted. A domain becomes `failed` only after its DNS verification retries are exhausted.
- **Rate-window cleanup.** The daily retention job deletes SMS rate-limit windows older than one day.
- **Breaking change:** `GET /v1/ops/summary` now returns a `queues` array (one entry per queue that has received work) instead of a single `queue` object, and dead-letter entries include their `queue`.
- Migration 7 adds indexes for message search and suppression listing.

**Templates (Phase 1 item 2).** Templates are versioned; each version is immutable and holds content per locale: `email_subject`, `email_text`, optional `email_html`, and `sms_text`. Publishing a version makes it the one sent when a notification names no version.

| Endpoint | Scope | Purpose |
| --- | --- | --- |
| `POST /v1/templates` `{"key","name"}` | `templates:write` | Register a template key (lowercase letters, digits, `.`, `_`, `-`). |
| `GET /v1/templates`, `GET /v1/templates/{key}` | `templates:read` | List templates; show one with its version history and active version. |
| `POST /v1/templates/{key}/versions` `{"default_locale","contents":{"en":{...}}}` | `templates:write` | Create the next version. Syntax is checked on create. |
| `GET /v1/templates/{key}/versions/{n}` | `templates:read` | Show a version's contents. |
| `POST /v1/templates/{key}/versions/{n}/publish` | `templates:write` | Make version `n` active. |
| `POST /v1/templates/{key}/preview` `{"version","locale","channels","data"}` | `templates:read` | Render without sending. |

A notification sends `"template": {"key", "version"?, "locale"?, "data"}` instead of `content`; sending both is a `400`. The API renders it when the request is accepted: a missing variable, an unpublished template, or a rendered subject with a line break returns `422` and nothing is queued. Locales fall back from `fr-CA` to `fr` to the version's default locale. HTML is rendered with automatic escaping; subjects, plain text, and SMS are not HTML-escaped. The stored message keeps the rendered content plus the template key, version, and locale actually used; template `data` is not stored. The idempotency hash covers the request as sent, so replaying a request after a new version is published returns the original message instead of `409`.

Direct sends can also include `content.html` (up to 500 KB) for an HTML email part and `content.sms` (up to 1,600 bytes) to send different text over SMS than `content.body`. Email is sent as `multipart/alternative` with the plain-text part first; SMTP messages are DKIM-signed over the whole message. Migration 8 adds the template tables.

**Test-mode keys (Phase 1 item 4).** `admin create-key --test` issues an `rt_test_` key. Notifications it sends are marked `test_mode` and are simulated by the worker: suppression, SMS consent, routing, and templates behave as they do live, but no provider is contacted and no sender capacity, SMS budget, or rate limit is used. A domain does not need to be verified. Test emails are accepted and then delivered, unless the recipient's domain is `bounce.test`, which simulates a hard bounce (and suppresses that address). Test SMS are accepted and then delivered. Customer webhooks fire as usual with `"test_mode": true` in the payload. Test and live messages have separate idempotency keys, and each kind of key sees only its own messages in the read APIs. The accept response includes `test_mode`. Migration 9 adds the key mode and message flag.

**Delivery reconciliation (Phase 2).** Every five minutes the worker asks Twilio or Telnyx for the status of SMS that the provider accepted more than 15 minutes ago and that have no final receipt, and applies the answer exactly as a status callback would (terminal status, customer webhook). Each message is checked at most every 15 minutes and for up to 72 hours; claims use `SKIP LOCKED`, so several workers do not poll the same message. SendGrid and SMTP offer no per-message status lookup, so email is not polled: `GET /v1/ops/summary` reports `unconfirmed_email` and `unconfirmed_sms`, the provider-accepted messages still without a receipt after 24 hours. A growing `unconfirmed_email` count usually means the SendGrid Event Webhook is not configured. Twilio `canceled`, `read`, and `scheduled` statuses are now recognized. Migration 10 adds the reconciliation timestamp and indexes.

## Build steps

1. Install Go 1.24+ and Docker Desktop with Compose.
2. From the repository root, copy `.env.example` to `.env`. Keep `.env` out of version control; `.gitignore` already excludes it.
3. Generate persistent 32-byte keys and set `DKIM_ENCRYPTION_KEY` and `WEBHOOK_ENCRYPTION_KEY` to their standard-base64 encodings. The webhook key is required to register outbound customer webhook endpoints. Protect and back up both keys: losing either makes stored secrets undecryptable.
4. For actual email delivery, configure `EMAIL_TRANSPORT`, `EMAIL_FROM`, and the corresponding provider credentials. `EMAIL_TRANSPORT=disabled` is the local default; queued email tasks will be marked failed rather than silently reported as delivered.
5. For SendGrid, configure domain authentication in SendGrid as well as publishing the service's DNS records. SendGrid signs the HTTP-submitted message with its provider-managed key; the generated key is used for SMTP delivery. Set `SENDGRID_WEBHOOK_PUBLIC_KEY` to the Event Webhook verification key to enable signed bounce/complaint callbacks. Each send includes a `message_id` custom arg, which SendGrid returns as a top-level event field. Set `EMAIL_SPF_VALUE` to the SPF value appropriate for the selected delivery path.
6. To enable SMS, set `SMS_TRANSPORT` to `twilio` or `telnyx`, set `SMS_FROM` and `SMS_STATUS_CALLBACK_URL`, and provide the corresponding provider credentials. Twilio additionally needs `TWILIO_INBOUND_WEBHOOK_URL`; Telnyx needs `TELNYX_WEBHOOK_PUBLIC_KEY`. Configure both `SMS_COUNTRY_RATES_USD` and `SMS_COUNTRY_MONTHLY_CAPS_USD` as comma-separated `ISO=amount` pairs (for example, `US=0.01,GB=0.05`). Rates are estimated USD per SMS segment; budgets reserve that estimate for the current UTC month. The account send limit is a fixed per-minute window. Unconfigured destination countries fail closed.
7. Configure the provider's inbound webhook to `/internal/webhooks/sms/twilio` or `/internal/webhooks/sms/telnyx`. Twilio status callbacks are generated from `SMS_STATUS_CALLBACK_URL`; set it to the matching public Twilio webhook endpoint. Telnyx delivery callbacks use that URL from each send request. Provider callback routes use provider signature verification and are separate from customer API-key routes.
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

   In `api_key` mode, send a bearer key with the `consents:write` scope. Use `scope = "marketing"` for marketing permission or `scope = "all"` only when the captured consent explicitly covers both. `consented = false` withdraws consent and adds an SMS suppression. Inbound STOP, STOPALL, UNSUBSCRIBE, CANCEL, END, QUIT, and REVOKE commands withdraw all SMS scopes and queue a confirmation. If the provider's own opt-out handling (for example Twilio Advanced Opt-Out) already replies and blocks the number, the service's confirmation is rejected permanently and not retried; disable one of the two confirmations to avoid a provider error.

9. Configure channel preferences with `PUT /v1/preferences`:

   ```powershell
   $preference = @{
     channel = "email"
     recipient = "person@example.com"
     enabled = $true
   } | ConvertTo-Json
   Invoke-RestMethod -Method Put -Uri "http://localhost:8080/v1/preferences" `
     -ContentType "application/json" -Body $preference
   ```

   Preferences are recipient- and channel-specific. They do not override suppression or consent checks. Use `routing.strategy = "preference"` in a notification to dispatch only through enabled requested channels. `explicit` (the default) fans out to the requested channels concurrently. `waterfall` tries requested channels in the supplied `routing.order`, falling back after a synchronous provider failure; it does not retry after a provider has accepted the message. Channels a waterfall did not need are recorded as skipped, and the message is `delivered` once any channel delivers. Message status moves `accepted` → `processing` (provider attempt in progress or accepted, awaiting a receipt) → `delivered`, `partially_delivered`, or `failed`.

10. Register an outbound customer webhook. A 32-byte signing secret is returned once; save it securely. The payload bytes are signed as HMAC-SHA256 over `timestamp + "." + raw_body`, with `X-Roote-Timestamp` and `X-Roote-Signature: sha256=<hex>` headers. Only HTTPS endpoints are accepted, redirects are not followed, and outbound connections to private, loopback, link-local, CGNAT, NAT64, documentation, and other special-purpose addresses are blocked.

   ```powershell
   $webhook = @{
     url = "https://hooks.example.com/notifications"
     events = @("notification.accepted", "notification.accepted_by_provider",
                "notification.delivered", "notification.partially_delivered",
                "notification.failed")
   } | ConvertTo-Json
   Invoke-RestMethod -Method Post -Uri "http://localhost:8080/v1/webhooks" `
     -ContentType "application/json" -Body $webhook
   ```

   Webhook attempts are persisted and retried by Asynq (up to eight retries). The examples here use the loopback-only development mode. Deployed environments must set `API_AUTH_MODE=api_key` and send a scoped bearer key to every `/v1/*` customer or operations route.

11. Start services with `docker compose up --build`.
12. Check `http://localhost:8080/healthz` and `http://localhost:8080/readyz`.
13. Register a domain and publish the returned records:

   ```powershell
   Invoke-RestMethod -Method Post -Uri "http://localhost:8080/v1/domains" `
     -ContentType "application/json" -Body '{"domain":"mail.example.org"}'
   ```

   Merge the SPF mechanism into an existing SPF record, if present; do not publish multiple SPF or DMARC TXT records for the same owner name. After DNS has propagated, trigger verification with `POST /v1/domains/{id}/verify`; retries run in the worker.

14. Submit a transactional email request:

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

15. Repeat with the same key and payload (same message ID), then change the payload but keep the key (HTTP 409).
16. Watch `docker compose logs -f worker` and inspect `delivery_attempts`, `email_deliveries`, `sms_deliveries`, and `webhook_deliveries`. Provider acceptance is not final delivery; signed provider callbacks update delivery state.
17. Run local checks with `go test ./...` and `go vet ./...`. PostgreSQL-backed SMS and orchestration/webhook store lifecycle tests require `TEST_DATABASE_URL` pointing at a disposable PostgreSQL database; run `go test ./internal/store -count=1`.
18. Stop the stack with `docker compose down`. PostgreSQL data is persisted in a named volume; removing it is destructive and is not part of routine shutdown.

## Milestone 5 operations

### Authentication and key lifecycle

`API_AUTH_MODE` defaults to `api_key` in the binary. The checked-in Compose configuration and `.env.example` deliberately choose `development` for local, loopback-only testing; this mode bypasses API-key checks and must never be used for a public deployment. Set `API_AUTH_MODE=api_key` before exposing an environment.

Provision a least-privilege key from the configured account. The plaintext token is printed once and is never stored; only its SHA-256 hash is persisted. Do not copy it into shell history, logs, tickets, or source control.

```powershell
docker compose run --rm --no-deps --entrypoint /app/admin api create-key `
  --name service-integration `
  --scopes notifications:write,domains:read `
  --expires-in 720h
```

Scopes are `notifications:write`, `messages:read`, `preferences:write`, `consents:read`, `consents:write`, `webhooks:read`, `webhooks:write`, `domains:read`, `domains:write`, `suppressions:read`, `suppressions:write`, `templates:read`, `templates:write`, `ops:read`, `ops:write`, and `data:delete`. `*` grants every scope and cannot be combined with other scopes. Expiry is optional; when set, it must be no longer than ten years. Rotate keys by creating a replacement, deploying it to the client, verifying usage, then revoking the old key:

```powershell
docker compose run --rm --no-deps --entrypoint /app/admin api revoke-key --id <key-uuid>
```

The API keys created by this CLI belong to the configured `ACCOUNT_ID`; account provisioning and multi-tenant account administration are not provided. API keys authenticate the calling service, not an end-user's consent.

### Monitoring and queue recovery

- `GET /metrics` on the API (scope `ops:read` in `api_key` mode; configure the scraper with a bearer key) and the worker's internal `METRICS_PORT` (unauthenticated; keep it on a private network) expose Prometheus text metrics. API metrics include low-cardinality method/route/status counters and latency histograms; worker metrics count completed/failed tasks by fixed task type.
- `/readyz` requires both PostgreSQL and Redis to respond. `/healthz` is a liveness check only.
- `GET /v1/ops/summary` (scope `ops:read`) reports database delivery/webhook backlogs and, per Asynq queue, depth, retry, and archive counts.
- `GET /v1/ops/dead-letters?limit=50` lists archived tasks across all queues, with each task's queue, without returning payloads or provider error strings. `POST /v1/ops/dead-letters/{task-id}/retry` (scope `ops:write`) requeues one archived task and writes an audit event. Retry only after correcting the underlying provider/configuration issue; provider acceptance followed by a crash can cause a duplicate delivery.
- Notification retries exhausted by Asynq are persisted as failed or partially delivered and emit the normal terminal customer notification event. Webhook tasks have their own durable delivery status and bounded retries. Investigate archived domain-verification tasks before manually retrying them.
- Outbox writes are durable in PostgreSQL; if Redis is unavailable, readiness fails and the worker relay retries later. Do not purge Redis to recover an outage. Inspect the queue summary and application logs, restore Redis connectivity, then retry only archived work that is safe to repeat.

### Sender warm-up, region, and retention

The first send for each `(account, channel, sender identity)` creates a durable daily capacity rule using these configurable defaults: email starts at 1,000/day and adds 1,000/day up to 100,000/day; SMS starts at 100/day and adds 100/day up to 10,000/day. SendGrid identities use the configured transactional or marketing IP pool when present; otherwise email uses the sending domain. SMS identities include provider, configured sender, and destination country. Counts are per UTC database date and include provider attempts, including transient failures. A full capacity returns the task for retry rather than sending past the limit. Operator overrides use `PUT /v1/ops/capacities` with scope `ops:write`; each override resets that identity's warm-up start date, for example:

```json
{"channel":"email","sender":"sendgrid:ip-pool:transactional-pool","initial_daily_limit":200,"daily_increment":100,"maximum_daily_limit":2000}
```

The existing SMS account minute-rate and country monthly USD budgets remain additional limits. `region` is an optional validated notification field persisted with each message; it is metadata for regional policies, not automatic cross-region routing.

`MESSAGE_RETENTION_DAYS` defaults to 90 and controls periodic deletion of old message records in batches. Message content, delivery attempts, and message events are removed; consent records, suppressions, and audit events are retained. A caller with `data:delete` can delete one message with `DELETE /v1/messages/{message-id}`. Every explicit deletion and each non-empty retention purge is audited. Set the retention period to match applicable legal/customer requirements before deployment.

### Secrets, backups, deployment, and incidents

- Supply provider credentials and the 32-byte DKIM/webhook encryption keys through a secret manager. Restrict access and preserve encrypted backups of those keys separately from the database backup. Rotating either encryption key without re-encrypting stored values makes existing stored secrets unreadable; this release does not implement online encryption-key rotation.
- Back up PostgreSQL before applying migrations or changing retention settings. Test restoration of the database and encryption keys together. The Asynq queue is Redis-backed; keep Redis persistence enabled and monitor readiness, queue latency, retry counts, and archived tasks.
- Database migrations are versioned and applied on service startup. Roll out API and worker binaries that contain the same migration set; take a backup first and avoid concurrent incompatible releases.
- For a provider outage, pause provider-side sending if possible, verify provider status/credentials, monitor queue growth and SMS budgets, and allow configured retries to resume when healthy. Avoid manually retrying tasks until the cause is fixed.
- For suspected API-key exposure, revoke the key with the admin command, provision a replacement with only required scopes, and review the `audit_events` table plus API request logs. The token itself is not recoverable from the database.
- For a customer data deletion request, use the scoped delete endpoint and verify the message is absent. Retention and explicit deletion remove message data but intentionally preserve consent/suppression and audit evidence; follow the applicable legal process if those records also require removal.

## Current limitations and next decisions

- The API is authenticated in `api_key` mode, but account provisioning and customer self-service are not implemented; API keys created by the admin CLI are bound to the configured `ACCOUNT_ID`.
- Email-only, SMS-only, and mixed-channel requests are supported. A provider's later delivery failure does not trigger waterfall fallback after that provider has already accepted the message.
- The local `development` auth mode intentionally bypasses API keys and is safe only for loopback-only development. Consent submitted by an authenticated API client is still an assertion by that client; it is not independent proof of end-user consent. Integrate verified consent capture before production.
- SMS country prices are operator-provided estimates and budget reservations, not reconciled provider invoices. Update rates and caps whenever provider pricing changes; current-period estimates should be reconciled operationally.
- The SMTP adapter signs messages with the stored DKIM key. For SendGrid, configure SendGrid's own domain authentication records; provider signing is not performed with the private key stored here.
- Exactly-once delivery cannot be guaranteed across a provider acceptance followed by a process crash before PostgreSQL records the response. Providers may accept a retry as a duplicate; this remains an at-least-once system.
- Configure secrets from a secret manager in deployed environments; never reuse `.env.example` credentials.
- Regional notification metadata is persisted but does not itself select a region or route traffic across regions. Configure and test external regional isolation before relying on it.
- Before production, choose deployment region/provider, set a retention policy, and establish managed key-rotation/recovery procedures.
