// Package secret seals small secrets (DKIM private keys, webhook signing secrets) with
// AES-256-GCM under a keyring that supports key rotation.
//
// Ciphertexts written by this version are versioned envelopes:
//
//	"rk1" | fingerprint (8 bytes) | nonce (12 bytes) | sealed data
//
// The fingerprint is the first 8 bytes of SHA-256 of the key that sealed the data, so a
// keyring holding the current and previous keys can pick the right one. Earlier releases
// wrote bare nonce | sealed data; those still decrypt by trying each key in the keyring.
package secret

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
)

var envelopeMagic = []byte("rk1")

const fingerprintSize = 8

// Keyring encrypts with its current key and decrypts with the current or any previous key.
type Keyring struct {
	keys         [][]byte
	fingerprints [][]byte
}

// NewKeyring builds a keyring. Every key must be exactly 32 bytes; current is used to encrypt.
func NewKeyring(current []byte, previous ...[]byte) (*Keyring, error) {
	keyring := &Keyring{}
	for _, key := range append([][]byte{current}, previous...) {
		if len(key) != 32 {
			return nil, errors.New("encryption key must be exactly 32 bytes")
		}
		keyring.keys = append(keyring.keys, key)
		keyring.fingerprints = append(keyring.fingerprints, fingerprint(key))
	}
	return keyring, nil
}

// Encrypt seals plaintext under the current key in a versioned envelope.
func (k *Keyring) Encrypt(plaintext []byte) ([]byte, error) {
	gcm, err := newGCM(k.keys[0])
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate encryption nonce: %w", err)
	}
	envelope := make([]byte, 0, len(envelopeMagic)+fingerprintSize+len(nonce)+len(plaintext)+gcm.Overhead())
	envelope = append(envelope, envelopeMagic...)
	envelope = append(envelope, k.fingerprints[0]...)
	envelope = append(envelope, nonce...)
	return gcm.Seal(envelope, nonce, plaintext, nil), nil
}

// Decrypt opens a ciphertext sealed under any key in the keyring.
func (k *Keyring) Decrypt(ciphertext []byte) ([]byte, error) {
	if body, keyFingerprint, ok := splitEnvelope(ciphertext); ok {
		for index, candidate := range k.fingerprints {
			if bytes.Equal(candidate, keyFingerprint) {
				return open(k.keys[index], body)
			}
		}
		// Fall through: a pre-envelope ciphertext can begin with the magic bytes by chance.
	}
	for _, key := range k.keys {
		if plaintext, err := open(key, ciphertext); err == nil {
			return plaintext, nil
		}
	}
	return nil, errors.New("decrypt secret: no key in the keyring opens this ciphertext")
}

// IsCurrent reports whether ciphertext is an envelope sealed under the current key, so
// rotation can skip it.
func (k *Keyring) IsCurrent(ciphertext []byte) bool {
	_, keyFingerprint, ok := splitEnvelope(ciphertext)
	return ok && bytes.Equal(keyFingerprint, k.fingerprints[0])
}

// Encrypt seals plaintext under key.
func Encrypt(key, plaintext []byte) ([]byte, error) {
	keyring, err := NewKeyring(key)
	if err != nil {
		return nil, err
	}
	return keyring.Encrypt(plaintext)
}

// Decrypt opens a ciphertext sealed under key.
func Decrypt(key, ciphertext []byte) ([]byte, error) {
	keyring, err := NewKeyring(key)
	if err != nil {
		return nil, err
	}
	return keyring.Decrypt(ciphertext)
}

func fingerprint(key []byte) []byte {
	sum := sha256.Sum256(key)
	return sum[:fingerprintSize]
}

func splitEnvelope(ciphertext []byte) (body, keyFingerprint []byte, ok bool) {
	if len(ciphertext) < len(envelopeMagic)+fingerprintSize || !bytes.HasPrefix(ciphertext, envelopeMagic) {
		return nil, nil, false
	}
	rest := ciphertext[len(envelopeMagic):]
	return rest[fingerprintSize:], rest[:fingerprintSize], true
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM mode: %w", err)
	}
	return gcm, nil
}

func open(key, nonceAndSealed []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	if len(nonceAndSealed) < gcm.NonceSize() {
		return nil, errors.New("ciphertext is too short")
	}
	nonce, sealed := nonceAndSealed[:gcm.NonceSize()], nonceAndSealed[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, sealed, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt secret: %w", err)
	}
	return plaintext, nil
}
