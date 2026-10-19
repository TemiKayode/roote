package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/TemiKayode/roote/internal/store"
)

type fakeReadStore struct {
	message         store.MessageDetail
	filter          store.MessageFilter
	rotatedSecret   []byte
	suppressionErr  error
	deletedFor      string
	consentPhone    string
	deactivatedID   uuid.UUID
	messageNotFound bool
}

func (f *fakeReadStore) GetMessage(_ context.Context, _, id uuid.UUID) (store.MessageDetail, error) {
	if f.messageNotFound || id != f.message.ID {
		return store.MessageDetail{}, pgx.ErrNoRows
	}
	return f.message, nil
}

func (f *fakeReadStore) ListMessages(_ context.Context, _ uuid.UUID, filter store.MessageFilter) (store.MessagePage, error) {
	f.filter = filter
	if filter.Cursor == "bad" {
		return store.MessagePage{}, store.ErrInvalidCursor
	}
	return store.MessagePage{Messages: []store.MessageSummary{f.message.MessageSummary}, NextCursor: "next"}, nil
}

func (f *fakeReadStore) ListWebhookEndpoints(context.Context, uuid.UUID) ([]store.WebhookEndpointSummary, error) {
	return []store.WebhookEndpointSummary{{ID: uuid.New(), URL: "https://hooks.example.com/x", Active: true}}, nil
}

func (f *fakeReadStore) DeactivateWebhookEndpoint(_ context.Context, _, id uuid.UUID) error {
	f.deactivatedID = id
	return nil
}

func (f *fakeReadStore) RotateWebhookSecret(_ context.Context, _, _ uuid.UUID, secret []byte) error {
	f.rotatedSecret = secret
	return nil
}

func (f *fakeReadStore) ListDomains(context.Context, uuid.UUID) ([]store.DomainSummary, error) {
	return nil, nil
}

func (f *fakeReadStore) ListSuppressions(context.Context, uuid.UUID, store.SuppressionFilter) (store.SuppressionPage, error) {
	return store.SuppressionPage{}, nil
}

func (f *fakeReadStore) DeleteEmailSuppression(_ context.Context, _ uuid.UUID, recipient string) error {
	f.deletedFor = recipient
	return f.suppressionErr
}

func (f *fakeReadStore) ListSMSConsents(_ context.Context, _ uuid.UUID, phone string) ([]store.ConsentRecord, error) {
	f.consentPhone = phone
	return nil, nil
}

func newReadRouter(t *testing.T, reads ReadStore, auth *fakeAPIKeyStore) http.Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)
	runtime := RuntimeFeatures{Reads: reads}
	if auth != nil {
		runtime.AuthMode, runtime.APIKeys = "api_key", auth
	}
	return NewWithRuntime(
		slog.New(slog.NewTextHandler(io.Discard, nil)), auth.accountID(), &fakeStore{},
		EmailFeatures{}, SMSFeatures{},
		CustomerWebhookFeatures{EncryptionKey: make([]byte, 32)}, runtime,
	)
}

func (f *fakeAPIKeyStore) accountID() string {
	if f == nil {
		return "00000000-0000-4000-8000-000000000001"
	}
	return f.key.AccountID.String()
}

func serve(router http.Handler, method, target, token string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, nil)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func TestMessageReadsRequireMessagesReadScope(t *testing.T) {
	messageID := uuid.New()
	reads := &fakeReadStore{message: store.MessageDetail{MessageSummary: store.MessageSummary{ID: messageID, Status: "delivered"}}}
	auth := &fakeAPIKeyStore{key: store.APIKey{ID: uuid.New(), AccountID: uuid.New(), Scopes: []string{"notifications:write"}}}
	router := newReadRouter(t, reads, auth)
	token := "rt_live_" + strings.Repeat("b", 43)

	if response := serve(router, http.MethodGet, "/v1/messages/"+messageID.String(), token); response.Code != http.StatusForbidden {
		t.Fatalf("missing scope status = %d, want 403", response.Code)
	}
	auth.key.Scopes = []string{"messages:read"}
	response := serve(router, http.MethodGet, "/v1/messages/"+messageID.String(), token)
	var detail store.MessageDetail
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &detail) != nil || detail.ID != messageID {
		t.Fatalf("get message status = %d: %s", response.Code, response.Body)
	}
	if response := serve(router, http.MethodGet, "/v1/messages/"+uuid.NewString(), token); response.Code != http.StatusNotFound {
		t.Fatalf("unknown message status = %d, want 404", response.Code)
	}
}

func TestListMessagesValidatesFilters(t *testing.T) {
	reads := &fakeReadStore{}
	router := newReadRouter(t, reads, nil)
	for target, want := range map[string]int{
		"/v1/messages?status=bogus":                           http.StatusBadRequest,
		"/v1/messages?recipient=not-a-recipient":              http.StatusBadRequest,
		"/v1/messages?since=yesterday":                        http.StatusBadRequest,
		"/v1/messages?limit=500":                              http.StatusBadRequest,
		"/v1/messages?cursor=bad":                             http.StatusBadRequest,
		"/v1/messages?status=failed&recipient=%2B14155552671": http.StatusOK,
	} {
		if response := serve(router, http.MethodGet, target, ""); response.Code != want {
			t.Errorf("GET %s status = %d, want %d: %s", target, response.Code, want, response.Body)
		}
	}
	if reads.filter.Status != "failed" || reads.filter.Recipient != "+14155552671" {
		t.Fatalf("filter passed to store = %+v", reads.filter)
	}
}

func TestWebhookManagementEndpoints(t *testing.T) {
	reads := &fakeReadStore{}
	router := newReadRouter(t, reads, nil)
	endpointID := uuid.New()
	if response := serve(router, http.MethodGet, "/v1/webhooks", ""); response.Code != http.StatusOK ||
		!strings.Contains(response.Body.String(), "hooks.example.com") {
		t.Fatalf("list webhooks status = %d: %s", response.Code, response.Body)
	}
	response := serve(router, http.MethodPost, "/v1/webhooks/"+endpointID.String()+"/rotate-secret", "")
	var rotated struct {
		SigningSecret string `json:"signing_secret"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &rotated) != nil ||
		len(rotated.SigningSecret) != 43 || len(reads.rotatedSecret) == 0 {
		t.Fatalf("rotate status = %d: %s", response.Code, response.Body)
	}
	if string(reads.rotatedSecret) == rotated.SigningSecret {
		t.Fatal("rotated secret was stored unencrypted")
	}
	if response := serve(router, http.MethodDelete, "/v1/webhooks/"+endpointID.String(), ""); response.Code != http.StatusNoContent ||
		reads.deactivatedID != endpointID {
		t.Fatalf("deactivate status = %d", response.Code)
	}
}

func TestSuppressionDeletionProtectsOptOutsAndComplaints(t *testing.T) {
	reads := &fakeReadStore{}
	router := newReadRouter(t, reads, nil)
	if response := serve(router, http.MethodDelete, "/v1/suppressions?channel=sms&recipient=%2B14155552671", ""); response.Code != http.StatusBadRequest {
		t.Fatalf("SMS suppression delete status = %d, want 400", response.Code)
	}
	if response := serve(router, http.MethodDelete, "/v1/suppressions?channel=email&recipient=person@example.com", ""); response.Code != http.StatusNoContent ||
		reads.deletedFor != "person@example.com" {
		t.Fatalf("bounce delete status = %d", response.Code)
	}
	reads.suppressionErr = store.ErrSuppressionProtected
	if response := serve(router, http.MethodDelete, "/v1/suppressions?channel=email&recipient=person@example.com", ""); response.Code != http.StatusConflict {
		t.Fatalf("complaint delete status = %d, want 409", response.Code)
	}
	if response := serve(router, http.MethodGet, "/v1/consents/sms?phone=%2B14155552671", ""); response.Code != http.StatusOK ||
		reads.consentPhone != "+14155552671" {
		t.Fatalf("consent read status = %d", response.Code)
	}
}
