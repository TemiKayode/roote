-- +goose Up
ALTER TABLE api_keys
    ADD COLUMN mode text NOT NULL DEFAULT 'live' CHECK (mode IN ('live', 'test'));

-- Test messages are simulated by the worker and never reach a provider. Test and live
-- messages keep separate idempotency keys.
ALTER TABLE messages ADD COLUMN test_mode boolean NOT NULL DEFAULT false;
ALTER TABLE messages DROP CONSTRAINT messages_account_id_idempotency_key_key;
ALTER TABLE messages ADD CONSTRAINT messages_account_mode_idempotency_key
    UNIQUE (account_id, test_mode, idempotency_key);

-- +goose Down
ALTER TABLE messages DROP CONSTRAINT messages_account_mode_idempotency_key;
DELETE FROM messages WHERE test_mode;
ALTER TABLE messages ADD CONSTRAINT messages_account_id_idempotency_key_key
    UNIQUE (account_id, idempotency_key);
ALTER TABLE messages DROP COLUMN test_mode;
ALTER TABLE api_keys DROP COLUMN mode;
