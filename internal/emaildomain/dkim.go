package emaildomain

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type DNSRecord struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Value   string `json:"value"`
	Purpose string `json:"purpose"`
}

type KeyMaterial struct {
	Selector     string
	PublicKey    string
	EncryptedKey []byte
	PrivatePEM   []byte
}

func GenerateKeyMaterial() (KeyMaterial, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return KeyMaterial{}, fmt.Errorf("generate DKIM RSA key: %w", err)
	}
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return KeyMaterial{}, fmt.Errorf("marshal DKIM private key: %w", err)
	}
	publicDER := x509.MarshalPKCS1PublicKey(&privateKey.PublicKey)
	return KeyMaterial{
		Selector:  "notify-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12],
		PublicKey: base64.StdEncoding.EncodeToString(publicDER),
		PrivatePEM: pem.EncodeToMemory(&pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: privateDER,
		}),
	}, nil
}

func EncryptPrivateKey(key, plaintext []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, errors.New("DKIM encryption key must be exactly 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create DKIM encryption cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create DKIM encryption mode: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate DKIM encryption nonce: %w", err)
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func DecryptPrivateKey(key, ciphertext []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, errors.New("DKIM encryption key must be exactly 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create DKIM decryption cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create DKIM decryption mode: %w", err)
	}
	if len(ciphertext) < gcm.NonceSize() {
		return nil, errors.New("DKIM ciphertext is too short")
	}
	nonce, encrypted := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, encrypted, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt DKIM private key: %w", err)
	}
	return plaintext, nil
}

func Records(domain, selector, publicKey, spfValue string) []DNSRecord {
	return []DNSRecord{
		{
			Name:    domain,
			Type:    "TXT",
			Value:   spfValue,
			Purpose: "Authorize the configured email delivery provider to send for this domain. Merge into an existing SPF record; do not publish a second SPF record.",
		},
		{
			Name:    selector + "._domainkey." + domain,
			Type:    "TXT",
			Value:   "v=DKIM1; k=rsa; p=" + publicKey,
			Purpose: "Publish the public key used to verify DKIM signatures.",
		},
		{
			Name:    "_dmarc." + domain,
			Type:    "TXT",
			Value:   "v=DMARC1; p=none; adkim=r; aspf=r",
			Purpose: "Start DMARC monitoring with a non-enforcing policy. Update an existing _dmarc record instead of publishing a duplicate.",
		},
	}
}
