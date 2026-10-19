package httpapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/goccy/go-yaml"
)

var ginParameter = regexp.MustCompile(`:([a-z]+)`)

func TestOpenAPISpecCoversEveryRoute(t *testing.T) {
	var spec struct {
		OpenAPI string                            `yaml:"openapi"`
		Paths   map[string]map[string]interface{} `yaml:"paths"`
	}
	if err := yaml.Unmarshal(openAPISpec, &spec); err != nil {
		t.Fatalf("openapi.yaml does not parse: %v", err)
	}
	if spec.OpenAPI != "3.1.0" {
		t.Fatalf("openapi version = %q", spec.OpenAPI)
	}
	gin.SetMode(gin.TestMode)
	router := New(slog.New(slog.NewTextHandler(io.Discard, nil)), "00000000-0000-4000-8000-000000000001", &fakeStore{})
	documented := map[string]bool{}
	for path, operations := range spec.Paths {
		for method := range operations {
			if method != "parameters" {
				documented[strings.ToUpper(method)+" "+path] = true
			}
		}
	}
	for _, route := range router.Routes() {
		if route.Path == "/openapi.yaml" {
			continue
		}
		key := route.Method + " " + ginParameter.ReplaceAllString(route.Path, "{$1}")
		if !documented[key] {
			t.Errorf("route %s is not documented in openapi.yaml", key)
		}
		delete(documented, key)
	}
	for key := range documented {
		t.Errorf("openapi.yaml documents %s, which is not a route", key)
	}
}

func TestOpenAPISpecIsServed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := New(slog.New(slog.NewTextHandler(io.Discard, nil)), "00000000-0000-4000-8000-000000000001", &fakeStore{})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil))
	if response.Code != http.StatusOK || !strings.HasPrefix(response.Body.String(), "openapi: 3.1.0") {
		t.Fatalf("GET /openapi.yaml status = %d", response.Code)
	}
}
