package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// UnconfirmedSMS is a provider-accepted SMS still waiting for a delivery receipt.
type UnconfirmedSMS struct {
	MessageID         uuid.UUID
	ProviderMessageID string
	Recipient         string
}

// ClaimUnconfirmedSMS returns up to limit SMS sent through provider that were accepted more than
// 15 minutes ago, are less than 72 hours old, have no final receipt, and were not checked in the
// last 15 minutes. Claimed rows are stamped so concurrent workers skip them.
func (s *Store) ClaimUnconfirmedSMS(ctx context.Context, provider string, limit int) ([]UnconfirmedSMS, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE sms_deliveries SET last_reconciled_at = now()
		WHERE message_id IN (
			SELECT message_id FROM sms_deliveries
			WHERE provider = $1 AND status IN ('accepted', 'sent') AND provider_message_id IS NOT NULL
			  AND updated_at < now() - interval '15 minutes'
			  AND updated_at > now() - interval '72 hours'
			  AND (last_reconciled_at IS NULL OR last_reconciled_at < now() - interval '15 minutes')
			ORDER BY updated_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING message_id, provider_message_id, recipient`, provider, limit)
	if err != nil {
		return nil, fmt.Errorf("claim unconfirmed SMS: %w", err)
	}
	claimed, err := pgx.CollectRows(rows, pgx.RowToStructByPos[UnconfirmedSMS])
	if err != nil {
		return nil, fmt.Errorf("read unconfirmed SMS: %w", err)
	}
	return claimed, nil
}
