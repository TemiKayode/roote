package secret

import (
	"bytes"
	"testing"
)

func TestEncryptDecryptAndAuthenticate(t *testing.T) {
	key := bytes.Repeat([]byte{9}, 32)
	plaintext := []byte("customer webhook secret")
	ciphertext, err := Encrypt(key, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	decrypted, err := Decrypt(key, ciphertext)
	if err != nil || !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("Decrypt() = %q, error = %v", decrypted, err)
	}
	ciphertext[len(ciphertext)-1] ^= 0xff
	if _, err := Decrypt(key, ciphertext); err == nil {
		t.Fatal("tampered ciphertext was decrypted")
	}
}

func legacyEncrypt(t *testing.T, key, plaintext []byte) []byte {
	t.Helper()
	gcm, err := newGCM(key)
	if err != nil {
		t.Fatal(err)
	}
	nonce := bytes.Repeat([]byte{7}, gcm.NonceSize())
	return gcm.Seal(nonce, nonce, plaintext, nil)
}

func TestKeyringRotationAndLegacyCiphertexts(t *testing.T) {
	oldKey, newKey := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
	oldRing, err := NewKeyring(oldKey)
	if err != nil {
		t.Fatal(err)
	}
	sealedOld, err := oldRing.Encrypt([]byte("old secret"))
	if err != nil {
		t.Fatal(err)
	}
	legacy := legacyEncrypt(t, oldKey, []byte("legacy secret"))

	rotated, err := NewKeyring(newKey, oldKey)
	if err != nil {
		t.Fatal(err)
	}
	for name, ciphertext := range map[string][]byte{"envelope": sealedOld, "legacy": legacy} {
		if _, err := rotated.Decrypt(ciphertext); err != nil {
			t.Errorf("%s ciphertext under the previous key: %v", name, err)
		}
		if rotated.IsCurrent(ciphertext) {
			t.Errorf("%s ciphertext under the previous key reported as current", name)
		}
	}
	resealed, err := rotated.Encrypt([]byte("old secret"))
	if err != nil || !rotated.IsCurrent(resealed) {
		t.Fatalf("resealed ciphertext current = %t, error = %v", rotated.IsCurrent(resealed), err)
	}
	newOnly, err := NewKeyring(newKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newOnly.Decrypt(sealedOld); err == nil {
		t.Fatal("a keyring without the previous key decrypted its ciphertext")
	}
	if plaintext, err := newOnly.Decrypt(resealed); err != nil || string(plaintext) != "old secret" {
		t.Fatalf("resealed plaintext = %q, error = %v", plaintext, err)
	}
	if _, err := NewKeyring(newKey, []byte("short")); err == nil {
		t.Fatal("keyring accepted a short previous key")
	}
}
