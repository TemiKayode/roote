package smswebhook

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"net/url"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestVerifyTwilioSignature(t *testing.T) {
	values := url.Values{
		"Body":       {"STOP"},
		"From":       {"+12025550124"},
		"MessageSid": {"SM123"},
	}
	requestURL := "https://hooks.example.org/internal/webhooks/sms/twilio"
	signature := signTwilio("test-token", requestURL, values)
	if err := VerifyTwilio("test-token", signature, requestURL, values); err != nil {
		t.Fatal(err)
	}
	values.Set("Body", "hello")
	if err := VerifyTwilio("test-token", signature, requestURL, values); err == nil {
		t.Fatal("VerifyTwilio() accepted modified form data")
	}
}

func TestVerifyTelnyxSignatureAndTimestamp(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	timestamp := strconv.FormatInt(now.Unix(), 10)
	body := []byte(`{"data":{"id":"event-1"}}`)
	payload := append([]byte(timestamp+"|"), body...)
	signature := ed25519.Sign(privateKey, payload)
	keyBase64 := base64.StdEncoding.EncodeToString(publicKey)
	signatureBase64 := base64.StdEncoding.EncodeToString(signature)
	if err := VerifyTelnyx(keyBase64, signatureBase64, timestamp, body, now); err != nil {
		t.Fatal(err)
	}
	if err := VerifyTelnyx(keyBase64, signatureBase64, timestamp, body, now.Add(6*time.Minute)); err == nil {
		t.Fatal("VerifyTelnyx() accepted a stale signature")
	}
	body[0] = ' '
	if err := VerifyTelnyx(keyBase64, signatureBase64, timestamp, body, now); err == nil {
		t.Fatal("VerifyTelnyx() accepted modified payload")
	}
}

func signTwilio(token, requestURL string, values url.Values) string {
	var payload string
	payload += requestURL
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		payload += key + values.Get(key)
	}
	mac := hmac.New(sha1.New, []byte(token))
	_, _ = mac.Write([]byte(payload))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
