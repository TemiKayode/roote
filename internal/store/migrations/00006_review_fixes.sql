-- +goose Up
-- Deliveries created before migration 5 have no source_message_id, so message deletion and
-- retention purges did not cascade to them. Backfill from source_id, which stores the message ID.
UPDATE webhook_deliveries d
SET source_message_id = m.id
FROM messages m
WHERE d.source_message_id IS NULL
  AND d.source_id IS NOT NULL
  AND d.source_id = m.id::text;

-- Supports the case-insensitive suppression and preference lookups used on every send.
CREATE INDEX suppressions_lower_recipient_idx ON suppressions (account_id, channel, lower(recipient));
CREATE INDEX notification_preferences_lower_recipient_idx
    ON notification_preferences (account_id, channel, lower(recipient));

-- +goose Down
DROP INDEX notification_preferences_lower_recipient_idx;
DROP INDEX suppressions_lower_recipient_idx;
