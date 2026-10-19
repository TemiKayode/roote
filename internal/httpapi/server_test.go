package httpapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/TemiKayode/roote/internal/observability"
	securesecret "github.com/TemiKayode/roote/internal/secret"
	"github.com/TemiKayode/roote/internal/store"
)

type fakeStore struct {
	id      uuid.UUID
	hash    [32]byte
	created bool
	calls   int
}

type fakeMilestoneFourStore struct {
	*fakeStore
	preferenceChannel string
	preferenceEnabled bool
	webhookURL        string
	webhookSecret     []byte
	webhookEvents     []string
}

type fakeAPIKeyStore struct {
	key store.APIKey
}

func (f *fakeAPIKeyStore) AuthenticateAPIKey(context.Context, []byte) (store.APIKey, error) {
	return f.key, nil
}

func (f *fakeMilestoneFourStore) SetNotificationPreference(
	_ context.Context, _ uuid.UUID, _ string, channel string, enabled bool,
) error {
	f.preferenceChannel, f.preferenceEnabled = channel, enabled
	return nil
}

func (f *fakeMilestoneFourStore) CreateWebhookEndpoint(
	_ context.Context,
	_ uuid.UUID,
	endpointURL string,
	encryptedSecret []byte,
	events []string,
) (uuid.UUID, error) {
	f.webhookURL = endpointURL
	f.webhookSecret = append([]byte(nil), encryptedSecret...)
	f.webhookEvents = append([]string(nil), events...)
	return uuid.New(), nil
}

type fakeSMSStore struct {
	phone         string
	scope         string
	source        string
	consented     bool
	inboundEvents int
	webhookEvents []store.SMSWebhookEvent
}

func (f *fakeSMSStore) RecordSMSConsent(
	_ context.Context, _ uuid.UUID, phone, scope, source string, consented bool,
) error {
	f.phone, f.scope, f.source, f.consented = phone, scope, source, consented
	return nil
}

func (f *fakeSMSStore) ProcessSMSInbound(
	_ context.Context, _ uuid.UUID, _, phone, text string,
) (bool, error) {
	f.phone = phone
	f.inboundEvents++
	return strings.EqualFold(strings.TrimSpace(text), "STOP"), nil
}

func (f *fakeSMSStore) ProcessSMSWebhookEvent(_ context.Context, event store.SMSWebhookEvent) (bool, error) {
	f.webhookEvents = append(f.webhookEvents, event)
	return true, nil
}

func (f *fakeStore) AcceptNotification(
	_ context.Context,
	_, _ string,
	_ store.Notification,
	request []byte,
) (uuid.UUID, bool, error) {
	f.calls++
	hash := sha256.Sum256(request)
	if f.created {
		if f.hash != hash {
			return uuid.Nil, false, store.ErrIdempotencyConflict
		}
		return f.id, true, nil
	}
	f.created = true
	f.hash = hash
	f.id = uuid.New()
	return f.id, false, nil
}

func (f *fakeStore) Ping(context.Context) error {
	return nil
}

func TestCreateNotificationIdempotency(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repository := &fakeStore{}
	router := New(slog.New(slog.NewTextHandler(io.Discard, nil)), "account", repository)
	body := `{"event":"welcome","channels":["email"],"recipient":{"email":"person@example.com"},"content":{"subject":"Welcome","body":"Hello"}}`

	first := postNotification(router, "key-1", body)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status = %d, want %d: %s", first.Code, http.StatusAccepted, first.Body)
	}
	var firstResponse struct {
		MessageID string `json:"message_id"`
		Duplicate bool   `json:"duplicate"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstResponse); err != nil {
		t.Fatalf("decode first response: %v", err)
	}
	if firstResponse.MessageID == "" || firstResponse.Duplicate {
		t.Fatalf("unexpected first response: %+v", firstResponse)
	}

	replay := postNotification(router, "key-1", body)
	if replay.Code != http.StatusAccepted {
		t.Fatalf("replay status = %d, want %d: %s", replay.Code, http.StatusAccepted, replay.Body)
	}
	var replayResponse struct {
		MessageID string `json:"message_id"`
		Duplicate bool   `json:"duplicate"`
	}
	if err := json.Unmarshal(replay.Body.Bytes(), &replayResponse); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if replayResponse.MessageID != firstResponse.MessageID || !replayResponse.Duplicate {
		t.Fatalf("replay response = %+v, want same message ID and duplicate=true", replayResponse)
	}

	conflict := postNotification(router, "key-1", `{"event":"welcome","channels":["email"],"recipient":{"email":"person@example.com"},"content":{"subject":"Welcome","body":"Changed"}}`)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, want %d: %s", conflict.Code, http.StatusConflict, conflict.Body)
	}
	if repository.calls != 3 {
		t.Fatalf("store calls = %d, want 3", repository.calls)
	}
}

func TestCreateNotificationRejectsInvalidRecipient(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repository := &fakeStore{}
	router := New(slog.New(slog.NewTextHandler(io.Discard, nil)), "account", repository)
	body := `{"event":"welcome","channels":["sms"],"recipient":{"phone":"5551234567"},"content":{"body":"Hello"}}`
	response := postNotification(router, "key-2", body)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusBadRequest, response.Body)
	}
	if repository.calls != 0 {
		t.Fatalf("invalid request reached store %d times", repository.calls)
	}
}

func TestAPIKeyAuthenticationScopesAndRequestMetrics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	accountID := uuid.New()
	metrics := observability.NewMetrics()
	authenticator := &fakeAPIKeyStore{key: store.APIKey{
		ID: uuid.New(), AccountID: accountID, Scopes: []string{"domains:read"},
	}}
	router := NewWithRuntime(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		accountID.String(),
		&fakeStore{},
		EmailFeatures{},
		SMSFeatures{},
		CustomerWebhookFeatures{},
		RuntimeFeatures{AuthMode: "api_key", APIKeys: authenticator, Metrics: metrics},
	)
	body := `{"event":"welcome","channels":["email"],"recipient":{"email":"person@example.com"},"content":{"subject":"Welcome","body":"Hello"}}`
	unauthorized := postNotification(router, "auth-1", body)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("missing API key status = %d, want %d", unauthorized.Code, http.StatusUnauthorized)
	}
	if _, err := uuid.Parse(unauthorized.Header().Get(observability.RequestIDHeader)); err != nil {
		t.Fatalf("request ID response header is invalid: %v", err)
	}

	token := "rt_live_" + strings.Repeat("a", 43)
	forbidden := postNotificationWithToken(router, "auth-2", body, token)
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("insufficient-scope status = %d, want %d: %s", forbidden.Code, http.StatusForbidden, forbidden.Body)
	}
	authenticator.key.Scopes = []string{"notifications:write"}
	accepted := postNotificationWithToken(router, "auth-3", body, token)
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("scoped request status = %d, want %d: %s", accepted.Code, http.StatusAccepted, accepted.Body)
	}
	scrape := httptest.NewRecorder()
	router.ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if scrape.Code != http.StatusOK ||
		!strings.Contains(scrape.Body.String(), `route="/v1/notifications"`) ||
		!strings.Contains(scrape.Body.String(), "roote_http_request_duration_seconds_bucket") {
		t.Fatalf("metrics response missing request counters or histograms: %s", scrape.Body)
	}
}

func postNotificationWithToken(router http.Handler, idempotencyKey, body, token string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/notifications", bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func TestValidateNotificationRoutingModesAndOrder(t *testing.T) {
	notification := store.Notification{
		Event:    "receipt",
		Channels: []string{"email", "sms"},
		Routing:  store.Routing{Strategy: "waterfall", Order: []string{"sms", "email"}},
	}
	notification.Recipient.Email = "person@example.com"
	notification.Recipient.Phone = "+14155552671"
	notification.Content.Subject = "Receipt"
	notification.Content.Body = "Your order is confirmed."
	if message := validateNotification(notification); message != "" {
		t.Fatalf("valid waterfall routing rejected: %s", message)
	}
	notification.Routing.Order = []string{"email"}
	if message := validateNotification(notification); message == "" {
		t.Fatal("incomplete waterfall channel order was accepted")
	}
	notification.Routing = store.Routing{Strategy: "random"}
	if message := validateNotification(notification); message == "" {
		t.Fatal("unsupported routing strategy was accepted")
	}
}

func TestSMSConsentRequiresValidExplicitInput(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repository := &fakeSMSStore{}
	router := NewWithFeatures(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		uuid.NewString(),
		&fakeStore{},
		EmailFeatures{},
		SMSFeatures{Store: repository},
	)
	request := httptest.NewRequest(http.MethodPost, "/v1/consents/sms",
		strings.NewReader(`{"phone":"+14155552671","scope":"marketing","source":"signup-form","consented":true}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || repository.phone != "+14155552671" ||
		repository.scope != "marketing" || repository.source != "signup-form" || !repository.consented {
		t.Fatalf("status = %d, consent = %+v: %s", response.Code, repository, response.Body)
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/consents/sms",
		strings.NewReader(`{"phone":"+14155552671","scope":"marketing","source":"signup-form"}`))
	request.Header.Set("Content-Type", "application/json")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("missing consent status = %d, want 400", response.Code)
	}
}

func TestTwilioSTOPWebhookQueuesConfirmationOnlyAfterSignatureVerification(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repository := &fakeSMSStore{}
	queued := false
	router := NewWithFeatures(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		uuid.NewString(),
		&fakeStore{},
		EmailFeatures{},
		SMSFeatures{
			Provider:                "twilio",
			Store:                   repository,
			TwilioAuthToken:         "test-auth-token",
			TwilioInboundWebhookURL: "https://hooks.example.org/internal/webhooks/sms/twilio",
			EnqueueStopConfirmation: func(_ context.Context, provider, phone, eventID string) error {
				queued = provider == "twilio" && phone == "+14155552671" && eventID == "SM-event"
				return nil
			},
		},
	)
	values := url.Values{
		"From":       {"+14155552671"},
		"MessageSid": {"SM-event"},
		"Body":       {"STOP"},
	}
	request := httptest.NewRequest(http.MethodPost, "/internal/webhooks/sms/twilio",
		strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("X-Twilio-Signature", signTwilioTest(
		"test-auth-token", "https://hooks.example.org/internal/webhooks/sms/twilio", values,
	))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !queued || repository.inboundEvents != 1 {
		t.Fatalf("status = %d, queued = %t, events = %d: %s",
			response.Code, queued, repository.inboundEvents, response.Body)
	}

	request = httptest.NewRequest(http.MethodPost, "/internal/webhooks/sms/twilio",
		strings.NewReader(values.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("X-Twilio-Signature", "invalid")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || repository.inboundEvents != 1 {
		t.Fatalf("invalid signature status = %d, events = %d", response.Code, repository.inboundEvents)
	}
}

func signTwilioTest(token, requestURL string, values url.Values) string {
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
	mac := hmac.New(sha1.New, []byte(token))
	_, _ = mac.Write([]byte(payload.String()))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func TestCreateNotificationRejectsUnknownFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repository := &fakeStore{}
	router := New(slog.New(slog.NewTextHandler(io.Discard, nil)), "account", repository)
	body := `{"event":"welcome","channels":["email"],"unexpected":"value","recipient":{"email":"person@example.com"},"content":{"subject":"Welcome","body":"Hello"}}`
	response := postNotification(router, "key-3", body)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusBadRequest, response.Body)
	}
	if repository.calls != 0 {
		t.Fatalf("invalid request reached store %d times", repository.calls)
	}
}

func TestPreferenceAndCustomerWebhookConfiguration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repository := &fakeMilestoneFourStore{fakeStore: &fakeStore{}}
	encryptionKey := bytes.Repeat([]byte{7}, 32)
	router := NewWithAllFeatures(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		uuid.NewString(),
		repository,
		EmailFeatures{},
		SMSFeatures{},
		CustomerWebhookFeatures{Store: repository, EncryptionKey: encryptionKey},
	)

	preferenceRequest := httptest.NewRequest(http.MethodPut, "/v1/preferences",
		strings.NewReader(`{"channel":"email","recipient":"Person@example.net","enabled":true}`))
	preferenceRequest.Header.Set("Content-Type", "application/json")
	preferenceResponse := httptest.NewRecorder()
	router.ServeHTTP(preferenceResponse, preferenceRequest)
	if preferenceResponse.Code != http.StatusOK || repository.preferenceChannel != "email" ||
		!repository.preferenceEnabled {
		t.Fatalf("preference status = %d, saved = %s/%t: %s",
			preferenceResponse.Code, repository.preferenceChannel, repository.preferenceEnabled,
			preferenceResponse.Body)
	}

	webhookRequest := httptest.NewRequest(http.MethodPost, "/v1/webhooks",
		strings.NewReader(`{"url":"https://hooks.example.net/events","events":["notification.accepted","notification.delivered"]}`))
	webhookRequest.Header.Set("Content-Type", "application/json")
	webhookResponse := httptest.NewRecorder()
	router.ServeHTTP(webhookResponse, webhookRequest)
	if webhookResponse.Code != http.StatusCreated || repository.webhookURL != "https://hooks.example.net/events" ||
		len(repository.webhookSecret) == 0 || len(repository.webhookEvents) != 2 {
		t.Fatalf("webhook status = %d, endpoint = %+v: %s",
			webhookResponse.Code, repository, webhookResponse.Body)
	}
	var response struct {
		SigningSecret string `json:"signing_secret"`
	}
	if err := json.Unmarshal(webhookResponse.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.SigningSecret == "" {
		t.Fatal("webhook signing secret was not returned on creation")
	}
	signingSecret, err := base64.RawURLEncoding.DecodeString(response.SigningSecret)
	if err != nil {
		t.Fatal(err)
	}
	decrypted, err := securesecret.Decrypt(encryptionKey, repository.webhookSecret)
	if err != nil || !bytes.Equal(decrypted, signingSecret) {
		t.Fatalf("persisted encrypted key does not match returned signing secret: %v", err)
	}
}

func TestCustomerWebhookRejectsInsecureDestination(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repository := &fakeMilestoneFourStore{fakeStore: &fakeStore{}}
	router := NewWithAllFeatures(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		uuid.NewString(),
		repository,
		EmailFeatures{},
		SMSFeatures{},
		CustomerWebhookFeatures{Store: repository, EncryptionKey: bytes.Repeat([]byte{1}, 32)},
	)
	request := httptest.NewRequest(http.MethodPost, "/v1/webhooks",
		strings.NewReader(`{"url":"http://hooks.example.net/events","events":["notification.accepted"]}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("insecure webhook status = %d, want 400: %s", response.Code, response.Body)
	}
}

func TestDomainRegistrationReturnsDNSRecordsAndQueuesVerification(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repository := &fakeStore{}
	domains := &fakeDomainStore{}
	queued := false
	router := NewWithEmail(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"account",
		repository,
		EmailFeatures{
			DomainStore:   domains,
			EncryptionKey: bytes.Repeat([]byte{3}, 32),
			SPFValue:      "v=spf1 include:sendgrid.net ~all",
			EnqueueDomainVerification: func(_ context.Context, _ string, domainID uuid.UUID) error {
				if domainID != domains.domain.ID {
					t.Fatalf("queued domain ID = %s", domainID)
				}
				queued = true
				return nil
			},
		},
	)

	request := httptest.NewRequest(http.MethodPost, "/v1/domains", strings.NewReader(`{"domain":"Mail.Example.org."}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("domain status = %d, want %d: %s", response.Code, http.StatusCreated, response.Body)
	}
	if !strings.Contains(response.Body.String(), "notify._domainkey.mail.example.org") {
		t.Fatalf("response missing DKIM record: %s", response.Body)
	}
	if strings.Contains(response.Body.String(), "encrypted_private_key") || domains.encryptedKey == nil {
		t.Fatal("private key was returned or not persisted")
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/domains/"+domains.domain.ID.String()+"/verify", nil)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || !queued {
		t.Fatalf("verification status = %d; queued = %t: %s", response.Code, queued, response.Body)
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/domains/"+domains.domain.ID.String()+"/records", nil)
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "v=DMARC1") {
		t.Fatalf("records status = %d: %s", response.Code, response.Body)
	}
}

func TestSendGridWebhookOnlyProcessesSignedHardEvents(t *testing.T) {
	gin.SetMode(gin.TestMode)
	domains := &fakeDomainStore{}
	router := NewWithEmail(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		"account",
		&fakeStore{},
		EmailFeatures{DomainStore: domains, WebhookVerifier: acceptingWebhookVerifier{}},
	)
	messageID := uuid.New()
	body := `[{"event":"bounce","type":"bounce","status":"5.1.1","email":"person@example.net","sg_event_id":"event-bounce","custom_args":{"message_id":"` +
		messageID.String() + `"}},{"event":"bounce","type":"blocked","status":"5.7.1","email":"other@example.net","sg_event_id":"event-blocked"},{"event":"spamreport","email":"person@example.net","sg_event_id":"event-complaint","custom_args":{"message_id":"` +
		messageID.String() + `"}}]`
	request := httptest.NewRequest(http.MethodPost, "/internal/webhooks/email", strings.NewReader(body))
	request.Header.Set("X-Twilio-Email-Event-Webhook-Signature", "valid")
	request.Header.Set("X-Twilio-Email-Event-Webhook-Timestamp", "123")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(domains.webhookEvents) != 2 {
		t.Fatalf("status = %d, recorded events = %d: %s", response.Code, len(domains.webhookEvents), response.Body)
	}
	if domains.webhookEvents[0].EventType != "bounce" || domains.webhookEvents[1].EventType != "complaint" {
		t.Fatalf("unexpected event types: %+v", domains.webhookEvents)
	}
}

type fakeDomainStore struct {
	domain        store.Domain
	encryptedKey  []byte
	webhookEvents []store.EmailWebhookEvent
}

func (f *fakeDomainStore) CreateDomain(
	_ context.Context, _, name, selector, publicKey, spf string, encrypted []byte,
) (store.Domain, error) {
	f.encryptedKey = append([]byte(nil), encrypted...)
	f.domain = store.Domain{
		ID: uuid.New(), Name: name, Selector: "notify", PublicKey: publicKey,
		SPFValue: spf, VerificationStatus: "pending",
	}
	return f.domain, nil
}

func (f *fakeDomainStore) GetDomain(_ context.Context, _ string, id uuid.UUID) (store.Domain, error) {
	if id != f.domain.ID {
		return store.Domain{}, pgx.ErrNoRows
	}
	return f.domain, nil
}

func (f *fakeDomainStore) RequestDomainVerification(_ context.Context, _ string, id uuid.UUID) error {
	if id != f.domain.ID {
		return pgx.ErrNoRows
	}
	return nil
}

func (f *fakeDomainStore) ProcessEmailWebhookEvent(_ context.Context, event store.EmailWebhookEvent) (bool, error) {
	f.webhookEvents = append(f.webhookEvents, event)
	return true, nil
}

type acceptingWebhookVerifier struct{}

func (acceptingWebhookVerifier) Verify([]byte, string, string, time.Time) error {
	return nil
}

func postNotification(router http.Handler, idempotencyKey, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/notifications", bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}
