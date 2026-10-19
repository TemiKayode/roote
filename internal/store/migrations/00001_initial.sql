-- +goose Up
CREATE TABLE accounts (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    key_hash bytea NOT NULL,
    name text NOT NULL,
    scopes text[] NOT NULL DEFAULT '{}',
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX api_keys_account_idx ON api_keys (account_id);

CREATE TABLE domains (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    domain_name text NOT NULL,
    verification_status text NOT NULL DEFAULT 'pending'
        CHECK (verification_status IN ('pending', 'verified', 'failed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (account_id, domain_name)
);

CREATE TABLE dkim_keys (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    domain_id uuid NOT NULL UNIQUE REFERENCES domains(id) ON DELETE CASCADE,
    selector text NOT NULL,
    encrypted_private_key bytea NOT NULL,
    public_key text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE messages (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    idempotency_key text NOT NULL,
    event_name text NOT NULL,
    channels text[] NOT NULL CHECK (cardinality(channels) > 0),
    payload jsonb NOT NULL,
    request_hash bytea NOT NULL,
    status text NOT NULL DEFAULT 'accepted'
        CHECK (status IN ('accepted', 'queued', 'processing', 'delivered', 'failed')),
    region text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (account_id, idempotency_key)
);
CREATE INDEX messages_account_created_idx ON messages (account_id, created_at DESC);

CREATE TABLE message_events (
    id bigserial PRIMARY KEY,
    message_id uuid NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    event_type text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX message_events_message_created_idx ON message_events (message_id, created_at);

CREATE TABLE notification_preferences (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    recipient text NOT NULL,
    channel text NOT NULL CHECK (channel IN ('email', 'sms')),
    enabled boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (account_id, recipient, channel)
);

CREATE TABLE suppressions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    recipient text NOT NULL,
    channel text NOT NULL CHECK (channel IN ('email', 'sms')),
    reason text NOT NULL,
    source text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (account_id, recipient, channel)
);

CREATE TABLE consent_records (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    recipient text NOT NULL,
    channel text NOT NULL CHECK (channel IN ('email', 'sms')),
    scope text NOT NULL,
    consent_source text NOT NULL,
    consented_at timestamptz NOT NULL,
    withdrawn_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX consent_records_lookup_idx ON consent_records (account_id, recipient, channel);

CREATE TABLE webhook_endpoints (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    endpoint_url text NOT NULL,
    secret_ciphertext bytea NOT NULL,
    subscribed_events text[] NOT NULL DEFAULT '{}',
    active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE webhook_deliveries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id uuid NOT NULL REFERENCES webhook_endpoints(id) ON DELETE CASCADE,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'delivered', 'failed')),
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    last_response_code integer,
    created_at timestamptz NOT NULL DEFAULT now(),
    delivered_at timestamptz
);

CREATE TABLE outbox (
    id bigserial PRIMARY KEY,
    message_id uuid NOT NULL UNIQUE REFERENCES messages(id) ON DELETE CASCADE,
    task_type text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_pending_idx ON outbox (created_at) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox;
DROP TABLE webhook_deliveries;
DROP TABLE webhook_endpoints;
DROP TABLE consent_records;
DROP TABLE suppressions;
DROP TABLE notification_preferences;
DROP TABLE message_events;
DROP TABLE messages;
DROP TABLE dkim_keys;
DROP TABLE domains;
DROP TABLE api_keys;
DROP TABLE accounts;
