package emailwebhook

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"strconv"
	"time"
)

type SendGridVerifier struct {
	publicKey *ecdsa.PublicKey
	maxSkew   time.Duration
}

func NewSendGridVerifier(publicKeyPEM string) (*SendGridVerifier, error) {
	block, _ := pem.Decode([]byte(publicKeyPEM))
	if block == nil {
		return nil, errors.New("SendGrid webhook public key must be PEM encoded")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse SendGrid webhook public key: %w", err)
	}
	publicKey, ok := parsed.(*ecdsa.PublicKey)
	if !ok || publicKey.Curve != elliptic.P256() {
		return nil, errors.New("SendGrid webhook key must be a P-256 ECDSA public key")
	}
	return &SendGridVerifier{publicKey: publicKey, maxSkew: 5 * time.Minute}, nil
}

func (v *SendGridVerifier) Verify(payload []byte, signature, timestamp string, now time.Time) error {
	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return errors.New("invalid SendGrid signature timestamp")
	}
	signedAt := time.Unix(seconds, 0)
	if signedAt.Before(now.Add(-v.maxSkew)) || signedAt.After(now.Add(v.maxSkew)) {
		return errors.New("SendGrid signature timestamp is outside the allowed window")
	}
	signatureBytes, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return errors.New("invalid SendGrid signature encoding")
	}
	signedPayload := make([]byte, 0, len(timestamp)+len(payload))
	signedPayload = append(signedPayload, timestamp...)
	signedPayload = append(signedPayload, payload...)
	digest := sha256.Sum256(signedPayload)
	if !ecdsa.VerifyASN1(v.publicKey, digest[:], signatureBytes) {
		return errors.New("SendGrid signature verification failed")
	}
	return nil
}
