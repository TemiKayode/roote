package httpapi

import (
	"bytes"
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

	"github.com/TemiKayode/roote/internal/rendering"
	"github.com/TemiKayode/roote/internal/store"
)

type fakeTemplateStore struct {
	versions  map[int]store.TemplateVersion
	active    *int
	published int
}

func (f *fakeTemplateStore) CreateTemplate(_ context.Context, _ uuid.UUID, key, name string) (store.Template, error) {
	if key == "taken" {
		return store.Template{}, store.ErrTemplateExists
	}
	return store.Template{Key: key, Name: name}, nil
}

func (f *fakeTemplateStore) CreateTemplateVersion(
	_ context.Context, _ uuid.UUID, key, defaultLocale string, contents map[string]rendering.Content,
) (int, error) {
	version := len(f.versions) + 1
	f.versions[version] = store.TemplateVersion{Key: key, Version: version, DefaultLocale: defaultLocale, Contents: contents}
	return version, nil
}

func (f *fakeTemplateStore) PublishTemplateVersion(_ context.Context, _ uuid.UUID, _ string, version int) error {
	if _, ok := f.versions[version]; !ok {
		return pgx.ErrNoRows
	}
	f.active, f.published = &version, version
	return nil
}

func (f *fakeTemplateStore) ListTemplates(context.Context, uuid.UUID) ([]store.Template, error) {
	return nil, nil
}

func (f *fakeTemplateStore) GetTemplate(context.Context, uuid.UUID, string) (store.TemplateDetail, error) {
	return store.TemplateDetail{}, nil
}

func (f *fakeTemplateStore) GetTemplateVersion(_ context.Context, _ uuid.UUID, _ string, version *int) (store.TemplateVersion, error) {
	if version == nil {
		if f.active == nil {
			return store.TemplateVersion{}, store.ErrTemplateNotPublished
		}
		version = f.active
	}
	stored, ok := f.versions[*version]
	if !ok {
		return store.TemplateVersion{}, pgx.ErrNoRows
	}
	return stored, nil
}

func newTemplateRouter(t *testing.T, templates *fakeTemplateStore, notifications *fakeStore) http.Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)
	return NewWithRuntime(
		slog.New(slog.NewTextHandler(io.Discard, nil)), "00000000-0000-4000-8000-000000000001", notifications,
		EmailFeatures{}, SMSFeatures{}, CustomerWebhookFeatures{}, RuntimeFeatures{Templates: templates},
	)
}

func send(router http.Handler, method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	return recorder
}

func TestTemplateLifecycleEndpoints(t *testing.T) {
	templates := &fakeTemplateStore{versions: map[int]store.TemplateVersion{}}
	router := newTemplateRouter(t, templates, &fakeStore{})

	if response := send(router, http.MethodPost, "/v1/templates", `{"key":"welcome","name":"Welcome"}`, nil); response.Code != http.StatusCreated {
		t.Fatalf("create template status = %d: %s", response.Code, response.Body)
	}
	if response := send(router, http.MethodPost, "/v1/templates", `{"key":"taken","name":"Taken"}`, nil); response.Code != http.StatusConflict {
		t.Fatalf("duplicate template status = %d", response.Code)
	}
	if response := send(router, http.MethodPost, "/v1/templates", `{"key":"Bad Key","name":"x"}`, nil); response.Code != http.StatusBadRequest {
		t.Fatalf("invalid key status = %d", response.Code)
	}
	if response := send(router, http.MethodPost, "/v1/templates/welcome/versions",
		`{"default_locale":"en","contents":{"en":{"email_subject":"Hi {{.name"}}}`, nil); response.Code != http.StatusBadRequest {
		t.Fatalf("invalid template syntax status = %d: %s", response.Code, response.Body)
	}
	response := send(router, http.MethodPost, "/v1/templates/welcome/versions",
		`{"default_locale":"en","contents":{"en":{"email_subject":"Hi {{.name}}","email_text":"Welcome {{.name}}","sms_text":"Hi {{.name}}"},"fr":{"email_subject":"Salut {{.name}}","email_text":"Bienvenue {{.name}}"}}}`, nil)
	if response.Code != http.StatusCreated || !strings.Contains(response.Body.String(), `"version":1`) {
		t.Fatalf("create version status = %d: %s", response.Code, response.Body)
	}
	if response := send(router, http.MethodPost, "/v1/templates/welcome/versions/1/publish", "", nil); response.Code != http.StatusOK ||
		templates.published != 1 {
		t.Fatalf("publish status = %d", response.Code)
	}
	response = send(router, http.MethodPost, "/v1/templates/welcome/preview",
		`{"locale":"fr-CA","channels":["email"],"data":{"name":"Ada"}}`, nil)
	var preview struct {
		Version  int                `json:"version"`
		Rendered rendering.Rendered `json:"rendered"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &preview) != nil ||
		preview.Version != 1 || preview.Rendered.Locale != "fr" || preview.Rendered.EmailSubject != "Salut Ada" {
		t.Fatalf("preview status = %d: %s", response.Code, response.Body)
	}
}

func TestTemplatedNotificationIsRenderedAndHashedAsSent(t *testing.T) {
	version := 1
	templates := &fakeTemplateStore{active: &version, versions: map[int]store.TemplateVersion{1: {
		Key: "receipt", Version: 1, DefaultLocale: "en",
		Contents: map[string]rendering.Content{"en": {
			EmailSubject: "Order {{.order}}", EmailText: "Thanks for order {{.order}}",
			EmailHTML: "<p>Order {{.order}}</p>", SMSText: "Order {{.order}} confirmed",
		}},
	}}}
	notifications := &fakeStore{}
	router := newTemplateRouter(t, templates, notifications)
	headers := map[string]string{"Idempotency-Key": "tmpl-1"}
	body := `{"event":"receipt","channels":["email","sms"],"recipient":{"email":"p@example.com","phone":"+14155552671"},"template":{"key":"receipt","data":{"order":"A-7"}}}`

	if response := send(router, http.MethodPost, "/v1/notifications", body, headers); response.Code != http.StatusAccepted {
		t.Fatalf("templated notification status = %d: %s", response.Code, response.Body)
	}
	var stored store.Notification
	if err := json.Unmarshal(notifications.payload, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Content.Subject != "Order A-7" || stored.Content.Body != "Thanks for order A-7" ||
		stored.Content.HTML != "<p>Order A-7</p>" || stored.Content.SMS != "Order A-7 confirmed" {
		t.Fatalf("stored content = %+v", stored.Content)
	}
	if stored.Template == nil || stored.Template.Version != 1 || stored.Template.Locale != "en" || stored.Template.Data != nil {
		t.Fatalf("stored template reference = %+v", stored.Template)
	}

	// Publishing a new version must not turn a replay of the same request into a conflict.
	templates.versions[2] = templates.versions[1]
	version = 2
	if response := send(router, http.MethodPost, "/v1/notifications", body, headers); response.Code != http.StatusAccepted ||
		!strings.Contains(response.Body.String(), `"duplicate":true`) {
		t.Fatalf("replay status = %d: %s", response.Code, response.Body)
	}
}

func TestTemplatedNotificationErrors(t *testing.T) {
	version := 1
	templates := &fakeTemplateStore{versions: map[int]store.TemplateVersion{1: {
		Key: "otp", Version: 1, DefaultLocale: "en",
		Contents: map[string]rendering.Content{"en": {SMSText: "Code {{.code}}"}},
	}}}
	router := newTemplateRouter(t, templates, &fakeStore{})
	notification := func(key, template string) *httptest.ResponseRecorder {
		body := `{"event":"otp","channels":["sms"],"recipient":{"phone":"+14155552671"},` + template + `}`
		return send(router, http.MethodPost, "/v1/notifications", body, map[string]string{"Idempotency-Key": key})
	}
	if response := notification("e1", `"template":{"key":"otp","data":{"code":"1"}}`); response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("unpublished template status = %d: %s", response.Code, response.Body)
	}
	templates.active = &version
	if response := notification("e2", `"template":{"key":"otp","data":{}}`); response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("missing variable status = %d: %s", response.Code, response.Body)
	}
	if response := notification("e3", `"template":{"key":"otp","data":{"code":"1"}},"content":{"body":"x"}`); response.Code != http.StatusBadRequest {
		t.Fatalf("template plus content status = %d: %s", response.Code, response.Body)
	}
	if response := notification("e4", `"template":{"key":"otp","version":9,"data":{"code":"1"}}`); response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("unknown version status = %d: %s", response.Code, response.Body)
	}
	if response := notification("e5", `"template":{"key":"otp","data":{"code":"1"}}`); response.Code != http.StatusAccepted {
		t.Fatalf("valid templated SMS status = %d: %s", response.Code, response.Body)
	}
}
