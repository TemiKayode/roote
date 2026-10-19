-- +goose Up
CREATE TABLE sms_deliveries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id uuid NOT NULL UNIQUE REFERENCES messages(id) ON DELETE CASCADE,
    provider text NOT NULL,
    provider_message_id text,
    recipient text NOT NULL,
    country_code text NOT NULL,
    estimated_cost_usd numeric(12, 6) NOT NULL,
    status text NOT NULL CHECK (status IN ('sending', 'accepted', 'sent', 'delivered', 'failed', 'undelivered')),
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX sms_deliveries_provider_id_idx ON sms_deliveries (provider, provider_message_id);

CREATE TABLE sms_webhook_events (
    provider_event_id text PRIMARY KEY,
    message_id uuid NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    event_type text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sms_rate_limits (
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    window_start timestamptz NOT NULL,
    send_count integer NOT NULL,
    PRIMARY KEY (account_id, window_start)
);

CREATE TABLE sms_country_spend (
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    country_code text NOT NULL,
    period_start date NOT NULL,
    amount_reserved_usd numeric(12, 6) NOT NULL DEFAULT 0,
    PRIMARY KEY (account_id, country_code, period_start)
);

CREATE TABLE sms_cost_reservations (
    message_id uuid PRIMARY KEY REFERENCES messages(id) ON DELETE CASCADE,
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    country_code text NOT NULL,
    period_start date NOT NULL,
    amount_usd numeric(12, 6) NOT NULL,
    released boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sms_system_cost_reservations (
    reservation_id text PRIMARY KEY,
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    country_code text NOT NULL,
    period_start date NOT NULL,
    amount_usd numeric(12, 6) NOT NULL,
    released boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE sms_inbound_events (
    provider_event_id text PRIMARY KEY,
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    recipient text NOT NULL,
    event_type text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE sms_inbound_events;
DROP TABLE sms_system_cost_reservations;
DROP TABLE sms_cost_reservations;
DROP TABLE sms_country_spend;
DROP TABLE sms_rate_limits;
DROP TABLE sms_webhook_events;
DROP TABLE sms_deliveries;
