-- +goose Up
ALTER TABLE domains
    ADD COLUMN spf_value text NOT NULL DEFAULT 'v=spf1 include:sendgrid.net ~all',
    ADD COLUMN last_verification_error text,
    ADD COLUMN last_verification_at timestamptz,
    ADD COLUMN verified_at timestamptz;

CREATE TABLE email_deliveries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id uuid NOT NULL UNIQUE REFERENCES messages(id) ON DELETE CASCADE,
    provider text NOT NULL,
    provider_message_id text,
    status text NOT NULL CHECK (status IN ('sending', 'accepted', 'delivered', 'failed')),
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE email_webhook_events (
    id bigserial PRIMARY KEY,
    provider_event_id text NOT NULL UNIQUE,
    message_id uuid NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    recipient text NOT NULL,
    event_type text NOT NULL CHECK (event_type IN ('bounce', 'complaint', 'delivered')),
    created_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE email_webhook_events;
DROP TABLE email_deliveries;
ALTER TABLE domains
    DROP COLUMN verified_at,
    DROP COLUMN last_verification_at,
    DROP COLUMN last_verification_error,
    DROP COLUMN spf_value;
