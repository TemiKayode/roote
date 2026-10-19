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
