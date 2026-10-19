package emailwebhook

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"strconv"
	"testing"
	"time"
)

func TestSendGridSignatureVerification(t *testing.T) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	publicPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}))
	verifier, err := NewSendGridVerifier(publicPEM)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`[{"event":"bounce"}]`)
	now := time.Now()
	timestamp := strconv.FormatInt(now.Unix(), 10)
	signed := append([]byte(timestamp), payload...)
	digest := sha256.Sum256(signed)
	signature, err := ecdsa.SignASN1(rand.Reader, privateKey, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.Verify(payload, base64.StdEncoding.EncodeToString(signature), timestamp, now); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if err := verifier.Verify(payload, base64.StdEncoding.EncodeToString(signature), timestamp, now.Add(10*time.Minute)); err == nil {
		t.Fatal("Verify() accepted an expired timestamp")
	}
	if err := verifier.Verify(append(payload, ' '), base64.StdEncoding.EncodeToString(signature), timestamp, now); err == nil {
		t.Fatal("Verify() accepted a modified payload")
	}
}
