package smswebhook

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const maximumTimestampAge = 5 * time.Minute

func VerifyTwilio(authToken, signature, requestURL string, values url.Values) error {
	if authToken == "" || signature == "" || requestURL == "" {
		return errors.New("Twilio webhook signature, URL, and auth token are required")
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var payload strings.Builder
	payload.WriteString(requestURL)
	for _, key := range keys {
		items := append([]string(nil), values[key]...)
		sort.Strings(items)
		for _, item := range items {
			payload.WriteString(key)
			payload.WriteString(item)
		}
	}
	mac := hmac.New(sha1.New, []byte(authToken))
	_, _ = mac.Write([]byte(payload.String()))
	expected := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if subtle.ConstantTimeCompare([]byte(expected), []byte(signature)) != 1 {
		return errors.New("Twilio webhook signature does not match")
	}
	return nil
}

func VerifyTelnyx(
	publicKeyBase64, signatureHeader, timestampHeader string,
	body []byte,
	now time.Time,
) error {
	publicKeyBytes, err := base64.StdEncoding.DecodeString(strings.TrimSpace(publicKeyBase64))
	if err != nil || len(publicKeyBytes) != ed25519.PublicKeySize {
		return errors.New("Telnyx webhook public key must be base64-encoded Ed25519")
	}
	signature, err := base64.StdEncoding.DecodeString(strings.TrimSpace(signatureHeader))
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errors.New("Telnyx webhook signature is invalid")
	}
	timestampSeconds, err := strconv.ParseInt(strings.TrimSpace(timestampHeader), 10, 64)
	if err != nil {
		return fmt.Errorf("parse Telnyx webhook Unix timestamp: %w", err)
	}
	timestamp := time.Unix(timestampSeconds, 0)
	if age := now.Sub(timestamp); age > maximumTimestampAge || age < -maximumTimestampAge {
		return errors.New("Telnyx webhook timestamp is outside the allowed window")
	}
	payload := make([]byte, 0, len(timestampHeader)+1+len(body))
	payload = append(payload, timestampHeader...)
	payload = append(payload, '|')
	payload = append(payload, body...)
	if !ed25519.Verify(ed25519.PublicKey(publicKeyBytes), payload, signature) {
		return errors.New("Telnyx webhook signature does not match")
	}
	return nil
}
