package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Reseal re-encrypts one ciphertext. It returns the new ciphertext and true, or false when
// the ciphertext is already sealed under the current key.
type Reseal func(ciphertext []byte) ([]byte, bool, error)

// ReencryptDKIMKeys re-seals every DKIM private key that is not under the current key and
// returns how many were changed. All rows change in one transaction, so a failure leaves
// every key as it was.
func (s *Store) ReencryptDKIMKeys(ctx context.Context, reseal Reseal) (int, error) {
	return s.reencrypt(ctx, "dkim_keys", "encrypted_private_key", reseal)
}

// ReencryptWebhookSecrets re-seals every customer webhook signing secret that is not under the
// current key and returns how many were changed, in one transaction.
func (s *Store) ReencryptWebhookSecrets(ctx context.Context, reseal Reseal) (int, error) {
	return s.reencrypt(ctx, "webhook_endpoints", "secret_ciphertext", reseal)
}

// reencrypt rewrites column in table. Both names are constants supplied by this package.
func (s *Store) reencrypt(ctx context.Context, table, column string, reseal Reseal) (int, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin %s re-encryption: %w", table, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, fmt.Sprintf(`SELECT id, %s FROM %s ORDER BY id FOR UPDATE`, column, table))
	if err != nil {
		return 0, fmt.Errorf("load %s ciphertexts: %w", table, err)
	}
	type sealed struct {
		ID         uuid.UUID
		Ciphertext []byte
	}
	items, err := pgx.CollectRows(rows, pgx.RowToStructByPos[sealed])
	if err != nil {
		return 0, fmt.Errorf("read %s ciphertexts: %w", table, err)
	}
	changed := 0
	for _, item := range items {
		resealed, rewrite, err := reseal(item.Ciphertext)
		if err != nil {
			return 0, fmt.Errorf("re-encrypt %s %s: %w", table, item.ID, err)
		}
		if !rewrite {
			continue
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`UPDATE %s SET %s = $2 WHERE id = $1`, table, column),
			item.ID, resealed); err != nil {
			return 0, fmt.Errorf("save re-encrypted %s %s: %w", table, item.ID, err)
		}
		changed++
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit %s re-encryption: %w", table, err)
	}
	return changed, nil
}
