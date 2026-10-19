-- +goose Up
-- Support the message search filters and keyset pagination of GET /v1/messages.
CREATE INDEX messages_account_status_created_idx ON messages (account_id, status, created_at DESC, id DESC);
CREATE INDEX messages_account_email_created_idx
    ON messages (account_id, lower(payload->'recipient'->>'email'), created_at DESC, id DESC);
CREATE INDEX messages_account_phone_created_idx
    ON messages (account_id, (payload->'recipient'->>'phone'), created_at DESC, id DESC);
CREATE INDEX suppressions_account_created_idx ON suppressions (account_id, created_at DESC, id DESC);

-- +goose Down
DROP INDEX suppressions_account_created_idx;
DROP INDEX messages_account_phone_created_idx;
DROP INDEX messages_account_email_created_idx;
DROP INDEX messages_account_status_created_idx;
