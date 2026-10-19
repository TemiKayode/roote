package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/TemiKayode/roote/internal/rendering"
)

var (
	// ErrTemplateExists reports a template key already used by the account.
	ErrTemplateExists = errors.New("template key is already in use")
	// ErrTemplateNotPublished reports a template with no active version to send.
	ErrTemplateNotPublished = errors.New("template has no published version")
)

type Template struct {
	Key           string    `json:"key"`
	Name          string    `json:"name"`
	ActiveVersion *int      `json:"active_version"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type TemplateVersionSummary struct {
	Version       int       `json:"version"`
	DefaultLocale string    `json:"default_locale"`
	CreatedAt     time.Time `json:"created_at"`
}

type TemplateDetail struct {
	Template
	Versions []TemplateVersionSummary `json:"versions"`
}

type TemplateVersion struct {
	Key           string                       `json:"key"`
	Version       int                          `json:"version"`
	DefaultLocale string                       `json:"default_locale"`
	Contents      map[string]rendering.Content `json:"contents"`
	CreatedAt     time.Time                    `json:"created_at"`
}

// CreateTemplate registers a template key for the account. It has no versions until one is created.
func (s *Store) CreateTemplate(ctx context.Context, accountID uuid.UUID, key, name string) (Template, error) {
	var template Template
	err := s.pool.QueryRow(ctx, `
		INSERT INTO templates (account_id, template_key, name)
		VALUES ($1, $2, $3)
		RETURNING template_key, name, active_version, created_at, updated_at`, accountID, key, name,
	).Scan(&template.Key, &template.Name, &template.ActiveVersion, &template.CreatedAt, &template.UpdatedAt)
	if err != nil {
		var pgError *pgconn.PgError
		if errors.As(err, &pgError) && pgError.Code == "23505" {
			return Template{}, ErrTemplateExists
		}
		return Template{}, fmt.Errorf("create template: %w", err)
	}
	return template, nil
}

// CreateTemplateVersion stores validated contents as the template's next version number.
func (s *Store) CreateTemplateVersion(
	ctx context.Context,
	accountID uuid.UUID,
	key, defaultLocale string,
	contents map[string]rendering.Content,
) (int, error) {
	encoded, err := json.Marshal(contents)
	if err != nil {
		return 0, fmt.Errorf("encode template contents: %w", err)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin template version: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var templateID uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT id FROM templates WHERE account_id = $1 AND template_key = $2 FOR UPDATE`,
		accountID, key,
	).Scan(&templateID); err != nil {
		return 0, fmt.Errorf("lock template: %w", err)
	}
	var version int
	if err := tx.QueryRow(ctx, `
		INSERT INTO template_versions (template_id, version, default_locale, contents)
		SELECT $1, COALESCE(MAX(version), 0) + 1, $2, $3 FROM template_versions WHERE template_id = $1
		RETURNING version`, templateID, defaultLocale, encoded,
	).Scan(&version); err != nil {
		return 0, fmt.Errorf("create template version: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE templates SET updated_at = now() WHERE id = $1`, templateID); err != nil {
		return 0, fmt.Errorf("touch template: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit template version: %w", err)
	}
	return version, nil
}

// PublishTemplateVersion makes version the one sent when a notification names no version.
func (s *Store) PublishTemplateVersion(ctx context.Context, accountID uuid.UUID, key string, version int) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE templates t SET active_version = $3, updated_at = now()
		WHERE t.account_id = $1 AND t.template_key = $2
		  AND EXISTS (SELECT 1 FROM template_versions v WHERE v.template_id = t.id AND v.version = $3)`,
		accountID, key, version)
	if err != nil {
		return fmt.Errorf("publish template version: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// ListTemplates returns the account's templates ordered by key.
func (s *Store) ListTemplates(ctx context.Context, accountID uuid.UUID) ([]Template, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT template_key, name, active_version, created_at, updated_at
		FROM templates WHERE account_id = $1 ORDER BY template_key`, accountID)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	templates, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Template])
	if err != nil {
		return nil, fmt.Errorf("read templates: %w", err)
	}
	return templates, nil
}

// GetTemplate returns a template with its version history, newest first.
func (s *Store) GetTemplate(ctx context.Context, accountID uuid.UUID, key string) (TemplateDetail, error) {
	var detail TemplateDetail
	var templateID uuid.UUID
	if err := s.pool.QueryRow(ctx, `
		SELECT id, template_key, name, active_version, created_at, updated_at
		FROM templates WHERE account_id = $1 AND template_key = $2`, accountID, key,
	).Scan(&templateID, &detail.Key, &detail.Name, &detail.ActiveVersion, &detail.CreatedAt, &detail.UpdatedAt); err != nil {
		return TemplateDetail{}, fmt.Errorf("load template: %w", err)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT version, default_locale, created_at FROM template_versions
		WHERE template_id = $1 ORDER BY version DESC`, templateID)
	if err != nil {
		return TemplateDetail{}, fmt.Errorf("list template versions: %w", err)
	}
	if detail.Versions, err = pgx.CollectRows(rows, pgx.RowToStructByPos[TemplateVersionSummary]); err != nil {
		return TemplateDetail{}, fmt.Errorf("read template versions: %w", err)
	}
	return detail, nil
}

// GetTemplateVersion returns one version's contents; a nil version means the published one.
func (s *Store) GetTemplateVersion(
	ctx context.Context,
	accountID uuid.UUID,
	key string,
	version *int,
) (TemplateVersion, error) {
	var activeVersion *int
	var templateID uuid.UUID
	if err := s.pool.QueryRow(ctx, `
		SELECT id, active_version FROM templates WHERE account_id = $1 AND template_key = $2`,
		accountID, key,
	).Scan(&templateID, &activeVersion); err != nil {
		return TemplateVersion{}, fmt.Errorf("load template: %w", err)
	}
	if version == nil {
		if activeVersion == nil {
			return TemplateVersion{}, ErrTemplateNotPublished
		}
		version = activeVersion
	}
	result := TemplateVersion{Key: key}
	var encoded []byte
	if err := s.pool.QueryRow(ctx, `
		SELECT version, default_locale, contents, created_at FROM template_versions
		WHERE template_id = $1 AND version = $2`, templateID, *version,
	).Scan(&result.Version, &result.DefaultLocale, &encoded, &result.CreatedAt); err != nil {
		return TemplateVersion{}, fmt.Errorf("load template version: %w", err)
	}
	if err := json.Unmarshal(encoded, &result.Contents); err != nil {
		return TemplateVersion{}, fmt.Errorf("decode template contents: %w", err)
	}
	return result, nil
}
