-- +goose Up
ALTER TABLE messages DROP CONSTRAINT messages_status_check;
ALTER TABLE messages ADD CONSTRAINT messages_status_check
    CHECK (status IN ('accepted', 'queued', 'processing', 'delivered', 'partially_delivered', 'failed'));

CREATE TABLE delivery_attempts (
    id bigserial PRIMARY KEY,
    message_id uuid NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
    channel text NOT NULL CHECK (channel IN ('email', 'sms')),
    attempt_number integer NOT NULL,
    status text NOT NULL CHECK (status IN ('sending', 'accepted', 'delivered', 'failed', 'skipped')),
    provider text,
    error text,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    UNIQUE (message_id, channel, attempt_number)
);
CREATE INDEX delivery_attempts_message_idx ON delivery_attempts (message_id, started_at);

ALTER TABLE webhook_deliveries
    ADD COLUMN source_id text,
    ADD COLUMN queued_at timestamptz,
    ADD COLUMN last_error text;
CREATE UNIQUE INDEX webhook_deliveries_source_idx
    ON webhook_deliveries (endpoint_id, event_type, source_id)
    WHERE source_id IS NOT NULL;
CREATE INDEX webhook_deliveries_unqueued_idx
    ON webhook_deliveries (created_at)
    WHERE status = 'pending' AND queued_at IS NULL;

-- +goose Down
DROP INDEX webhook_deliveries_unqueued_idx;
DROP INDEX webhook_deliveries_source_idx;
ALTER TABLE webhook_deliveries
    DROP COLUMN last_error,
    DROP COLUMN queued_at,
    DROP COLUMN source_id;
DROP INDEX delivery_attempts_message_idx;
DROP TABLE delivery_attempts;
UPDATE messages SET status = 'failed' WHERE status = 'partially_delivered';
ALTER TABLE messages DROP CONSTRAINT messages_status_check;
ALTER TABLE messages ADD CONSTRAINT messages_status_check
    CHECK (status IN ('accepted', 'queued', 'processing', 'delivered', 'failed'));
