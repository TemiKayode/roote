package store

import (
	"bytes"
	"testing"

	"github.com/TemiKayode/roote/internal/secret"
)

func TestReencryptMovesSecretsToTheCurrentKey(t *testing.T) {
	repository, accountID, ctx := openIntegrationStore(t)
	oldKey, newKey := bytes.Repeat([]byte{3}, 32), bytes.Repeat([]byte{4}, 32)
	sealed, err := secret.Encrypt(oldKey, []byte("signing secret"))
	if err != nil {
		t.Fatal(err)
	}
	endpointID, err := repository.CreateWebhookEndpoint(
		ctx, accountID, "https://hooks.example.net/rotate", sealed, []string{"notification.failed"},
	)
	if err != nil {
		t.Fatal(err)
	}
	keyring, err := secret.NewKeyring(newKey, oldKey)
	if err != nil {
		t.Fatal(err)
	}
	reseal := func(ciphertext []byte) ([]byte, bool, error) {
		if keyring.IsCurrent(ciphertext) {
			return nil, false, nil
		}
		plaintext, err := keyring.Decrypt(ciphertext)
		if err != nil {
			return nil, false, err
		}
		resealed, err := keyring.Encrypt(plaintext)
		return resealed, err == nil, err
	}
	if _, err := repository.ReencryptWebhookSecrets(ctx, reseal); err != nil {
		t.Fatal(err)
	}
	var stored []byte
	if err := repository.pool.QueryRow(ctx, `SELECT secret_ciphertext FROM webhook_endpoints WHERE id = $1`, endpointID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	plaintext, err := secret.Decrypt(newKey, stored)
	if err != nil || string(plaintext) != "signing secret" {
		t.Fatalf("secret under the new key = %q, error = %v", plaintext, err)
	}
	// A second pass finds everything current and changes nothing.
	changed, err := repository.ReencryptWebhookSecrets(ctx, func(ciphertext []byte) ([]byte, bool, error) {
		if !keyring.IsCurrent(ciphertext) {
			return reseal(ciphertext)
		}
		return nil, false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var stillCurrent bool
	if err := repository.pool.QueryRow(ctx, `SELECT secret_ciphertext FROM webhook_endpoints WHERE id = $1`, endpointID).Scan(&stored); err == nil {
		stillCurrent = keyring.IsCurrent(stored)
	}
	if !stillCurrent {
		t.Fatalf("second pass changed %d rows and left a non-current ciphertext", changed)
	}
}
