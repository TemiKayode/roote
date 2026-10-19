-- +goose Up
-- When the worker last asked the provider for this SMS's status; throttles reconciliation.
ALTER TABLE sms_deliveries ADD COLUMN last_reconciled_at timestamptz;
CREATE INDEX sms_deliveries_unconfirmed_idx ON sms_deliveries (updated_at)
    WHERE status IN ('accepted', 'sent');
CREATE INDEX email_deliveries_unconfirmed_idx ON email_deliveries (updated_at)
    WHERE status = 'accepted';

-- +goose Down
DROP INDEX email_deliveries_unconfirmed_idx;
DROP INDEX sms_deliveries_unconfirmed_idx;
ALTER TABLE sms_deliveries DROP COLUMN last_reconciled_at;
