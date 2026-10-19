package httpapi

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/TemiKayode/roote/internal/rendering"
	"github.com/TemiKayode/roote/internal/store"
)

var templateKeyPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,99}$`)

// TemplateStore persists versioned notification templates.
type TemplateStore interface {
	CreateTemplate(context.Context, uuid.UUID, string, string) (store.Template, error)
	CreateTemplateVersion(context.Context, uuid.UUID, string, string, map[string]rendering.Content) (int, error)
	PublishTemplateVersion(context.Context, uuid.UUID, string, int) error
	ListTemplates(context.Context, uuid.UUID) ([]store.Template, error)
	GetTemplate(context.Context, uuid.UUID, string) (store.TemplateDetail, error)
	GetTemplateVersion(context.Context, uuid.UUID, string, *int) (store.TemplateVersion, error)
}

// templateContext checks that templates are configured, resolves the account, and validates
// the :key path parameter when the route has one.
func (s *Server) templateContext(c *gin.Context) (uuid.UUID, string, bool) {
	if s.templates == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "templates are unavailable"})
		return uuid.Nil, "", false
	}
	accountID, err := uuid.Parse(s.accountIDFor(c))
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "authenticated account ID is invalid", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not resolve the account"})
		return uuid.Nil, "", false
	}
	key := c.Param("key")
	if c.FullPath() != "/v1/templates" && !templateKeyPattern.MatchString(key) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "template key is invalid"})
		return uuid.Nil, "", false
	}
	return accountID, key, true
}

func (s *Server) createTemplate(c *gin.Context) {
	accountID, _, ok := s.templateContext(c)
	if !ok {
		return
	}
	var request struct {
		Key  string `json:"key"`
		Name string `json:"name"`
	}
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request body must be valid JSON with supported fields only"})
		return
	}
	name := strings.TrimSpace(request.Name)
	if !templateKeyPattern.MatchString(request.Key) || name == "" || len(name) > 200 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "key must be 1-100 lowercase letters, digits, '.', '_' or '-' and name 1-200 characters",
		})
		return
	}
	template, err := s.templates.CreateTemplate(c.Request.Context(), accountID, request.Key, name)
	if errors.Is(err, store.ErrTemplateExists) {
		c.JSON(http.StatusConflict, gin.H{"error": "template key is already in use"})
		return
	}
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not create template", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create template"})
		return
	}
	s.auditMutation(c, "template.created", "template", template.Key, nil)
	c.JSON(http.StatusCreated, template)
}

func (s *Server) listTemplates(c *gin.Context) {
	accountID, _, ok := s.templateContext(c)
	if !ok {
		return
	}
	templates, err := s.templates.ListTemplates(c.Request.Context(), accountID)
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not list templates", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not list templates"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"templates": templates})
}

func (s *Server) getTemplate(c *gin.Context) {
	accountID, key, ok := s.templateContext(c)
	if !ok {
		return
	}
	detail, err := s.templates.GetTemplate(c.Request.Context(), accountID, key)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "template not found"})
		return
	}
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not load template", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not load template"})
		return
	}
	c.JSON(http.StatusOK, detail)
}

func (s *Server) createTemplateVersion(c *gin.Context) {
	accountID, key, ok := s.templateContext(c)
	if !ok {
		return
	}
	var request struct {
		DefaultLocale string                       `json:"default_locale"`
		Contents      map[string]rendering.Content `json:"contents"`
	}
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request body must be valid JSON with supported fields only"})
		return
	}
	defaultLocale, contents, err := rendering.Validate(request.DefaultLocale, request.Contents)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	version, err := s.templates.CreateTemplateVersion(c.Request.Context(), accountID, key, defaultLocale, contents)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "template not found"})
		return
	}
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not create template version", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not create template version"})
		return
	}
	s.auditMutation(c, "template.version_created", "template", key, map[string]any{"version": version})
	c.JSON(http.StatusCreated, gin.H{"key": key, "version": version, "default_locale": defaultLocale})
}

func (s *Server) getTemplateVersion(c *gin.Context) {
	accountID, key, ok := s.templateContext(c)
	if !ok {
		return
	}
	version, ok := parseTemplateVersion(c)
	if !ok {
		return
	}
	result, err := s.templates.GetTemplateVersion(c.Request.Context(), accountID, key, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "template version not found"})
		return
	}
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not load template version", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not load template version"})
		return
	}
	c.JSON(http.StatusOK, result)
}

func (s *Server) publishTemplateVersion(c *gin.Context) {
	accountID, key, ok := s.templateContext(c)
	if !ok {
		return
	}
	version, ok := parseTemplateVersion(c)
	if !ok {
		return
	}
	err := s.templates.PublishTemplateVersion(c.Request.Context(), accountID, key, version)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "template version not found"})
		return
	}
	if err != nil {
		s.logger.ErrorContext(c.Request.Context(), "could not publish template version", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not publish template version"})
		return
	}
	s.auditMutation(c, "template.version_published", "template", key, map[string]any{"version": version})
	c.JSON(http.StatusOK, gin.H{"key": key, "active_version": version})
}

func (s *Server) previewTemplate(c *gin.Context) {
	accountID, key, ok := s.templateContext(c)
	if !ok {
		return
	}
	var request struct {
		Version  int            `json:"version"`
		Locale   string         `json:"locale"`
		Channels []string       `json:"channels"`
		Data     map[string]any `json:"data"`
	}
	if err := decodeJSON(c, &request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request body must be valid JSON with supported fields only"})
		return
	}
	if len(request.Channels) == 0 {
		request.Channels = []string{"email", "sms"}
	}
	result := s.renderTemplate(c, accountID, store.TemplateRef{
		Key: key, Version: request.Version, Locale: request.Locale, Data: request.Data,
	}, request.Channels)
	if result.status != 0 {
		c.JSON(result.status, gin.H{"error": result.message})
		return
	}
	c.JSON(http.StatusOK, gin.H{"version": result.version, "rendered": result.rendered})
}

type renderResult struct {
	rendered rendering.Rendered
	version  int
	// status and message are set when rendering failed: the HTTP status to return and an
	// error safe to show the caller.
	status  int
	message string
}

// renderTemplate renders reference for channels, resolving its version and locale.
func (s *Server) renderTemplate(
	c *gin.Context,
	accountID uuid.UUID,
	reference store.TemplateRef,
	channels []string,
) renderResult {
	if !templateKeyPattern.MatchString(reference.Key) || reference.Version < 0 {
		return renderResult{status: http.StatusBadRequest, message: "template.key or template.version is invalid"}
	}
	var version *int
	if reference.Version > 0 {
		version = &reference.Version
	}
	stored, err := s.templates.GetTemplateVersion(c.Request.Context(), accountID, reference.Key, version)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return renderResult{status: http.StatusUnprocessableEntity, message: "template or template version not found"}
	case errors.Is(err, store.ErrTemplateNotPublished):
		return renderResult{status: http.StatusUnprocessableEntity, message: "template has no published version"}
	case err != nil:
		s.logger.ErrorContext(c.Request.Context(), "could not load template", "error", err)
		return renderResult{status: http.StatusInternalServerError, message: "could not load template"}
	}
	locale, err := rendering.ResolveLocale(reference.Locale, stored.DefaultLocale, stored.Contents)
	if err != nil {
		return renderResult{status: http.StatusBadRequest, message: err.Error()}
	}
	rendered, err := rendering.Render(locale, stored.Contents[locale], channels, reference.Data)
	if err != nil {
		return renderResult{
			status: http.StatusUnprocessableEntity, message: "template could not be rendered: " + err.Error(),
		}
	}
	return renderResult{rendered: rendered, version: stored.Version}
}

func parseTemplateVersion(c *gin.Context) (int, bool) {
	version, err := strconv.Atoi(c.Param("version"))
	if err != nil || version < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "version must be a positive integer"})
		return 0, false
	}
	return version, true
}
