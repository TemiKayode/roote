package emaildomain

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

func TestEncryptDecryptPrivateKey(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	plaintext := []byte("private key material")
	ciphertext, err := EncryptPrivateKey(key, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ciphertext, plaintext) {
		t.Fatal("ciphertext contains plaintext")
	}
	decrypted, err := DecryptPrivateKey(key, ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("decrypted key = %q, want %q", decrypted, plaintext)
	}
	if _, err := DecryptPrivateKey(bytes.Repeat([]byte{8}, 32), ciphertext); err == nil {
		t.Fatal("decrypt with wrong key succeeded")
	}
}

func TestGenerateKeyMaterialAndRecords(t *testing.T) {
	material, err := GenerateKeyMaterial()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(material.Selector, "notify-") {
		t.Fatalf("selector = %q", material.Selector)
	}
	if _, err := base64.StdEncoding.DecodeString(material.PublicKey); err != nil {
		t.Fatalf("decode public key: %v", err)
	}
	records := Records("mail.example.org", material.Selector, material.PublicKey, "v=spf1 include:sendgrid.net ~all")
	if len(records) != 3 || records[1].Name != material.Selector+"._domainkey.mail.example.org" {
		t.Fatalf("unexpected DNS records: %+v", records)
	}
}
