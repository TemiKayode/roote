-- +goose Up
ALTER TABLE api_keys
    ADD COLUMN expires_at timestamptz,
    ADD COLUMN last_used_at timestamptz;
CREATE UNIQUE INDEX api_keys_key_hash_idx ON api_keys (key_hash);

CREATE TABLE audit_events (
    id bigserial PRIMARY KEY,
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    api_key_id uuid REFERENCES api_keys(id) ON DELETE SET NULL,
    action text NOT NULL,
    resource_type text NOT NULL,
    resource_id text,
    request_id text,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX audit_events_account_created_idx ON audit_events (account_id, created_at DESC);

CREATE TABLE sender_capacities (
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    channel text NOT NULL CHECK (channel IN ('email', 'sms')),
    sender text NOT NULL,
    initial_daily_limit integer NOT NULL CHECK (initial_daily_limit > 0),
    daily_increment integer NOT NULL CHECK (daily_increment >= 0),
    maximum_daily_limit integer NOT NULL CHECK (maximum_daily_limit >= initial_daily_limit),
    started_at date NOT NULL DEFAULT ((now() AT TIME ZONE 'UTC')::date),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, channel, sender)
);

CREATE TABLE sender_capacity_usage (
    account_id uuid NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    channel text NOT NULL,
    sender text NOT NULL,
    usage_date date NOT NULL DEFAULT ((now() AT TIME ZONE 'UTC')::date),
    reserved integer NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    PRIMARY KEY (account_id, channel, sender, usage_date),
    FOREIGN KEY (account_id, channel, sender)
        REFERENCES sender_capacities(account_id, channel, sender) ON DELETE CASCADE
);

ALTER TABLE webhook_deliveries
    ADD COLUMN source_message_id uuid REFERENCES messages(id) ON DELETE CASCADE;
CREATE INDEX webhook_deliveries_source_message_idx ON webhook_deliveries (source_message_id);

-- +goose Down
DROP INDEX webhook_deliveries_source_message_idx;
ALTER TABLE webhook_deliveries DROP COLUMN source_message_id;
DROP TABLE sender_capacity_usage;
DROP TABLE sender_capacities;
DROP INDEX audit_events_account_created_idx;
DROP TABLE audit_events;
DROP INDEX api_keys_key_hash_idx;
ALTER TABLE api_keys DROP COLUMN last_used_at, DROP COLUMN expires_at;
