package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrations embed.FS

const migrationAdvisoryLock int64 = 824771004691823008

const (
	relayBatchSize       = 100
	relayPollInterval    = 5 * time.Second
	outboxNotifyChannel  = "roote_outbox"
	webhookNotifyChannel = "roote_webhooks"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key was already used with a different request")
	ErrSMSRateLimit        = errors.New("SMS account rate limit exceeded")
	ErrSMSCountryBudget    = errors.New("SMS country monthly budget exceeded")
	ErrSMSConsentRequired  = errors.New("active SMS consent is required")
	ErrSenderCapacity      = errors.New("sender daily warm-up capacity exceeded")
	ErrWebhookMismatch     = errors.New("provider webhook does not match the stored delivery")
)

type Store struct {
	pool *pgxpool.Pool
}

type Notification struct {
	Event        string       `json:"event"`
	Channels     []string     `json:"channels"`
	TrafficClass string       `json:"traffic_class,omitempty"`
	Region       string       `json:"region,omitempty"`
	Routing      Routing      `json:"routing,omitempty"`
	Template     *TemplateRef `json:"template,omitempty"`
	Recipient    struct {
		Email string `json:"email,omitempty"`
		Phone string `json:"phone,omitempty"`
	} `json:"recipient"`
	Content struct {
		Subject string `json:"subject,omitempty"`
		Body    string `json:"body,omitempty"`
		HTML    string `json:"html,omitempty"`
		SMS     string `json:"sms,omitempty"`
	} `json:"content"`
}

// TemplateRef names the template a notification is rendered from. In a request, Version and
// Locale are optional; the stored notification records the version and locale actually used.
// Data is used only for rendering and is not stored.
type TemplateRef struct {
	Key     string         `json:"key"`
	Version int            `json:"version,omitempty"`
	Locale  string         `json:"locale,omitempty"`
	Data    map[string]any `json:"data,omitempty"`
}

type Routing struct {
	Strategy string   `json:"strategy,omitempty"`
	Order    []string `json:"order,omitempty"`
}

type WebhookEndpoint struct {
	ID               uuid.UUID
	AccountID        uuid.UUID
	URL              string
	EncryptedSecret  []byte
	SubscribedEvents []string
	Active           bool
}

type WebhookDelivery struct {
	ID              uuid.UUID
	URL             string
	EncryptedSecret []byte
	EventType       string
	Payload         []byte
}

type Domain struct {
	ID                    uuid.UUID
	AccountID             uuid.UUID
	Name                  string
	Selector              string
	PublicKey             string
	EncryptedPrivateKey   []byte
	SPFValue              string
	VerificationStatus    string
	LastVerificationError string
}

type EmailJob struct {
	ID           uuid.UUID
	AccountID    uuid.UUID
	TestMode     bool
	Region       string
	Channels     []string
	Recipient    string
	Subject      string
	Body         string
	HTML         string
	TrafficClass string
	Routing      Routing
}

type SMSJob struct {
	ID           uuid.UUID
	AccountID    uuid.UUID
	TestMode     bool
	Region       string
	Channels     []string
	Recipient    string
	Body         string
	TrafficClass string
	Routing      Routing
}

type DKIMKey struct {
	Selector            string
	EncryptedPrivateKey []byte
}

type EmailWebhookEvent struct {
	ProviderEventID string
	MessageID       uuid.UUID
	Recipient       string
	EventType       string
}

type SMSWebhookEvent struct {
	ProviderEventID   string
	ProviderMessageID string
	Provider          string
	MessageID         uuid.UUID
	Recipient         string
	EventType         string
}

type APIKey struct {
	ID        uuid.UUID
	AccountID uuid.UUID
	Scopes    []string
	// TestMode keys create test messages, which never reach a provider.
	TestMode bool
}

type AuditEvent struct {
	AccountID  uuid.UUID
	APIKeyID   *uuid.UUID
	Action     string
	Resource   string
	ResourceID string
	RequestID  string
	Details    any
}

type OperationalSummary struct {
	MessagesByStatus map[string]int64 `json:"messages_by_status"`
	PendingOutbox    int64            `json:"pending_outbox"`
	PendingWebhooks  int64            `json:"pending_webhooks"`
	FailedWebhooks   int64            `json:"failed_webhooks"`
}

func Open(ctx context.Context, databaseURL, accountID string) (*Store, error) {
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	connectCtx, cancelConnect := context.WithTimeout(ctx, 30*time.Second)
	defer cancelConnect()
	for {
		err = pool.Ping(connectCtx)
		if err == nil {
			break
		}
		if !wait(connectCtx, 250*time.Millisecond) {
			pool.Close()
			return nil, fmt.Errorf("connect to PostgreSQL: %w", errors.Join(connectCtx.Err(), err))
		}
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("open migration connection pool: %w", err)
	}
	if err := runMigrations(ctx, db); err != nil {
		_ = db.Close()
		pool.Close()
		return nil, err
	}
	if err := db.Close(); err != nil {
		pool.Close()
		return nil, fmt.Errorf("close migration connection pool: %w", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO accounts (id, name)
		VALUES ($1, 'Local development account')
		ON CONFLICT (id) DO NOTHING`, accountID); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ensure configured account exists: %w", err)
	}
	return &Store{pool: pool}, nil
}

func runMigrations(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration lock connection: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrationAdvisoryLock); err != nil {
		_ = conn.Close()
		return fmt.Errorf("acquire migration lock: %w", err)
	}

	goose.SetBaseFS(migrations)
	migrationErr := goose.UpContext(ctx, db, "migrations")
	unlockCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, unlockErr := conn.ExecContext(unlockCtx, `SELECT pg_advisory_unlock($1)`, migrationAdvisoryLock)
	closeErr := conn.Close()
	if migrationErr != nil {
		return fmt.Errorf("apply database migrations: %w", migrationErr)
	}
	if unlockErr != nil {
		return fmt.Errorf("release migration lock: %w", unlockErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close migration lock connection: %w", closeErr)
	}
	return nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return nil
}

func (s *Store) CreateAPIKey(
	ctx context.Context,
	accountID uuid.UUID,
	name string,
	hash []byte,
	scopes []string,
	expiresAt *time.Time,
	testMode bool,
) (uuid.UUID, error) {
	var id uuid.UUID
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return uuid.Nil, fmt.Errorf("begin API key creation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := tx.QueryRow(ctx, `
			INSERT INTO api_keys (account_id, key_hash, name, scopes, expires_at, mode)
			VALUES ($1, $2, $3, $4, $5, CASE WHEN $6 THEN 'test' ELSE 'live' END)
			RETURNING id`, accountID, hash, name, scopes, expiresAt, testMode,
	).Scan(&id); err != nil {
		return uuid.Nil, fmt.Errorf("create API key: %w", err)
	}
	if err := insertAuditEvent(ctx, tx, AuditEvent{
		AccountID: accountID, Action: "api_key.created", Resource: "api_key",
		ResourceID: id.String(), Details: map[string]any{"name": name, "scopes": scopes, "test_mode": testMode},
	}); err != nil {
		return uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("commit API key creation: %w", err)
	}
	return id, nil
}

func (s *Store) RevokeAPIKey(ctx context.Context, accountID, keyID uuid.UUID) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin API key revocation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
			UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now())
			WHERE id = $1 AND account_id = $2`, keyID, accountID)
	if err != nil {
		return fmt.Errorf("revoke API key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	if err := insertAuditEvent(ctx, tx, AuditEvent{
		AccountID: accountID, Action: "api_key.revoked", Resource: "api_key",
		ResourceID: keyID.String(),
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit API key revocation: %w", err)
	}
	return nil
}

func (s *Store) AuthenticateAPIKey(ctx context.Context, hash []byte) (APIKey, error) {
	var key APIKey
	err := s.pool.QueryRow(ctx, `
			UPDATE api_keys
			SET last_used_at = now()
			WHERE key_hash = $1 AND revoked_at IS NULL
			  AND (expires_at IS NULL OR expires_at > now())
			RETURNING id, account_id, scopes, mode = 'test'`, hash,
	).Scan(&key.ID, &key.AccountID, &key.Scopes, &key.TestMode)
	if err != nil {
		return APIKey{}, fmt.Errorf("authenticate API key: %w", err)
	}
	return key, nil
}

func (s *Store) RecordAuditEvent(ctx context.Context, event AuditEvent) error {
	if err := insertAuditEvent(ctx, s.pool, event); err != nil {
		return err
	}
	return nil
}

type auditExecer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func insertAuditEvent(ctx context.Context, execer auditExecer, event AuditEvent) error {
	details, err := json.Marshal(event.Details)
	if err != nil {
		return fmt.Errorf("encode audit event details: %w", err)
	}
	_, err = execer.Exec(ctx, `
			INSERT INTO audit_events
				(account_id, api_key_id, action, resource_type, resource_id, request_id, details)
			VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), $7::jsonb)`,
		event.AccountID, event.APIKeyID, event.Action, event.Resource, event.ResourceID,
		event.RequestID, string(details),
	)
	if err != nil {
		return fmt.Errorf("record audit event %q: %w", event.Action, err)
	}
	return nil
}

func (s *Store) SetSenderCapacity(
	ctx context.Context,
	accountID uuid.UUID,
	channel, sender string,
	initialLimit, dailyIncrement, maximumLimit int,
) error {
	_, err := s.pool.Exec(ctx, `
			INSERT INTO sender_capacities
				(account_id, channel, sender, initial_daily_limit, daily_increment, maximum_daily_limit)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (account_id, channel, sender) DO UPDATE SET
				initial_daily_limit = EXCLUDED.initial_daily_limit,
				daily_increment = EXCLUDED.daily_increment,
				maximum_daily_limit = EXCLUDED.maximum_daily_limit,
				started_at = (now() AT TIME ZONE 'UTC')::date,
				updated_at = now()`,
		accountID, channel, sender, initialLimit, dailyIncrement, maximumLimit,
	)
	if err != nil {
		return fmt.Errorf("set sender capacity: %w", err)
	}
	return nil
}

func (s *Store) ReserveSenderCapacity(
	ctx context.Context,
	accountID uuid.UUID,
	channel, sender string,
	initialLimit, dailyIncrement, maximumLimit int,
) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin sender capacity reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
			INSERT INTO sender_capacities
				(account_id, channel, sender, initial_daily_limit, daily_increment, maximum_daily_limit)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (account_id, channel, sender) DO NOTHING`,
		accountID, channel, sender, initialLimit, dailyIncrement, maximumLimit,
	); err != nil {
		return fmt.Errorf("initialize sender capacity: %w", err)
	}
	var effectiveLimit int
	if err := tx.QueryRow(ctx, `
			SELECT LEAST(maximum_daily_limit::bigint,
				initial_daily_limit::bigint +
					GREATEST((now() AT TIME ZONE 'UTC')::date - started_at, 0)::bigint *
					daily_increment::bigint)::integer
			FROM sender_capacities
			WHERE account_id = $1 AND channel = $2 AND sender = $3
			FOR UPDATE`, accountID, channel, sender,
	).Scan(&effectiveLimit); err != nil {
		return fmt.Errorf("load sender warm-up capacity: %w", err)
	}
	tag, err := tx.Exec(ctx, `
			INSERT INTO sender_capacity_usage (account_id, channel, sender, usage_date, reserved)
			VALUES ($1, $2, $3, (now() AT TIME ZONE 'UTC')::date, 1)
			ON CONFLICT (account_id, channel, sender, usage_date) DO UPDATE
			SET reserved = sender_capacity_usage.reserved + 1
			WHERE sender_capacity_usage.reserved < $4`,
		accountID, channel, sender, effectiveLimit,
	)
	if err != nil {
		return fmt.Errorf("reserve sender daily capacity: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrSenderCapacity
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit sender capacity reservation: %w", err)
	}
	return nil
}

func (s *Store) ReleaseSenderCapacity(
	ctx context.Context,
	accountID uuid.UUID,
	channel, sender string,
) error {
	_, err := s.pool.Exec(ctx, `
			UPDATE sender_capacity_usage SET reserved = reserved - 1
			WHERE account_id = $1 AND channel = $2 AND sender = $3
			  AND usage_date = (now() AT TIME ZONE 'UTC')::date AND reserved > 0`,
		accountID, channel, sender,
	)
	if err != nil {
		return fmt.Errorf("release sender daily capacity: %w", err)
	}
	return nil
}

func (s *Store) OperationalSummary(ctx context.Context, accountID uuid.UUID) (OperationalSummary, error) {
	summary := OperationalSummary{
		MessagesByStatus: make(map[string]int64),
	}
	rows, err := s.pool.Query(ctx, `
			SELECT status, count(*) FROM messages
			WHERE account_id = $1 GROUP BY status`, accountID)
	if err != nil {
		return OperationalSummary{}, fmt.Errorf("query message status counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return OperationalSummary{}, fmt.Errorf("scan message status count: %w", err)
		}
		summary.MessagesByStatus[status] = count
	}
	if err := rows.Err(); err != nil {
		return OperationalSummary{}, fmt.Errorf("iterate message status counts: %w", err)
	}
	err = s.pool.QueryRow(ctx, `
			SELECT
				(SELECT count(*) FROM outbox o JOIN messages m ON m.id = o.message_id
				 WHERE m.account_id = $1 AND o.published_at IS NULL),
				(SELECT count(*) FROM webhook_deliveries d
				 JOIN webhook_endpoints e ON e.id = d.endpoint_id
				 WHERE e.account_id = $1 AND d.status = 'pending'),
				(SELECT count(*) FROM webhook_deliveries d
				 JOIN webhook_endpoints e ON e.id = d.endpoint_id
				 WHERE e.account_id = $1 AND d.status = 'failed')`,
		accountID,
	).Scan(&summary.PendingOutbox, &summary.PendingWebhooks, &summary.FailedWebhooks)
	if err != nil {
		return OperationalSummary{}, fmt.Errorf("query operational queue summary: %w", err)
	}
	return summary, nil
}

func (s *Store) DeleteMessage(ctx context.Context, accountID, messageID uuid.UUID, requestID string, keyID *uuid.UUID) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin message deletion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `DELETE FROM messages WHERE id = $1 AND account_id = $2`, messageID, accountID)
	if err != nil {
		return fmt.Errorf("delete message: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	if err := insertAuditEvent(ctx, tx, AuditEvent{
		AccountID: accountID, APIKeyID: keyID, Action: "message.deleted",
		Resource: "message", ResourceID: messageID.String(), RequestID: requestID,
	}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit message deletion: %w", err)
	}
	return nil
}

func (s *Store) PurgeExpiredMessages(ctx context.Context, accountID uuid.UUID, retentionDays int) (int64, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin message retention purge: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var deleted int64
	if err := tx.QueryRow(ctx, `
			WITH expired AS (
				SELECT id FROM messages
				WHERE account_id = $1 AND created_at < now() - make_interval(days => $2)
				ORDER BY created_at
				LIMIT 1000
				FOR UPDATE SKIP LOCKED
			), removed AS (
				DELETE FROM messages
				WHERE id IN (SELECT id FROM expired)
				RETURNING id
			)
			SELECT count(*) FROM removed`, accountID, retentionDays,
	).Scan(&deleted); err != nil {
		return 0, fmt.Errorf("purge expired messages: %w", err)
	}
	if deleted > 0 {
		if err := insertAuditEvent(ctx, tx, AuditEvent{
			AccountID: accountID, Action: "messages.retention_purged", Resource: "message",
			Details: map[string]any{"deleted_count": deleted, "retention_days": retentionDays},
		}); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit message retention purge: %w", err)
	}
	return deleted, nil
}

// PurgeExpiredRateWindows deletes SMS rate-limit windows older than a day. Each window covers
// one minute, so only the current one is ever read.
func (s *Store) PurgeExpiredRateWindows(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM sms_rate_limits WHERE window_start < now() - interval '1 day'`)
	if err != nil {
		return 0, fmt.Errorf("purge expired SMS rate windows: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (s *Store) CreateDomain(
	ctx context.Context,
	accountID, name, selector, publicKey, spfValue string,
	encryptedPrivateKey []byte,
) (Domain, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Domain{}, fmt.Errorf("begin sending domain transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	var domain Domain
	err = tx.QueryRow(ctx, `
			INSERT INTO domains (account_id, domain_name, spf_value)
			VALUES ($1, $2, $3)
			RETURNING id, account_id, domain_name, spf_value, verification_status`,
		accountID, name, spfValue,
	).Scan(&domain.ID, &domain.AccountID, &domain.Name, &domain.SPFValue, &domain.VerificationStatus)
	if err != nil {
		return Domain{}, fmt.Errorf("insert sending domain: %w", err)
	}
	_, err = tx.Exec(ctx, `
			INSERT INTO dkim_keys (domain_id, selector, encrypted_private_key, public_key)
			VALUES ($1, $2, $3, $4)`,
		domain.ID, selector, encryptedPrivateKey, publicKey,
	)
	if err != nil {
		return Domain{}, fmt.Errorf("insert DKIM key: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Domain{}, fmt.Errorf("commit sending domain transaction: %w", err)
	}
	domain.Selector = selector
	domain.PublicKey = publicKey
	return domain, nil
}

func (s *Store) GetDomain(ctx context.Context, accountID string, domainID uuid.UUID) (Domain, error) {
	var domain Domain
	err := s.pool.QueryRow(ctx, `
			SELECT d.id, d.account_id, d.domain_name, d.spf_value, d.verification_status,
			       COALESCE(d.last_verification_error, ''), k.selector, k.public_key,
			       k.encrypted_private_key
			FROM domains d
			JOIN dkim_keys k ON k.domain_id = d.id
			WHERE d.account_id = $1 AND d.id = $2`, accountID, domainID,
	).Scan(
		&domain.ID, &domain.AccountID, &domain.Name, &domain.SPFValue,
		&domain.VerificationStatus, &domain.LastVerificationError, &domain.Selector,
		&domain.PublicKey, &domain.EncryptedPrivateKey,
	)
	if err != nil {
		return Domain{}, fmt.Errorf("load sending domain: %w", err)
	}
	return domain, nil
}

func (s *Store) RequestDomainVerification(ctx context.Context, accountID string, domainID uuid.UUID) error {
	result, err := s.pool.Exec(ctx, `
			UPDATE domains
			SET verification_status = CASE WHEN verification_status = 'verified' THEN 'verified' ELSE 'pending' END,
			    last_verification_error = NULL, last_verification_at = now()
			WHERE account_id = $1 AND id = $2`, accountID, domainID)
	if err != nil {
		return fmt.Errorf("request domain verification: %w", err)
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) IsEmailSuppressed(ctx context.Context, accountID uuid.UUID, recipient string) (bool, error) {
	var suppressed bool
	err := s.pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM suppressions
				WHERE account_id = $1 AND channel = 'email' AND lower(recipient) = lower($2)
			)`, accountID, recipient).Scan(&suppressed)
	if err != nil {
		return false, fmt.Errorf("check email suppression: %w", err)
	}
	return suppressed, nil
}

func (s *Store) GetSMSJob(ctx context.Context, messageID uuid.UUID) (SMSJob, error) {
	var job SMSJob
	var payload []byte
	err := s.pool.QueryRow(ctx, `
		SELECT account_id, channels, payload, test_mode
		FROM messages WHERE id = $1`, messageID,
	).Scan(&job.AccountID, &job.Channels, &payload, &job.TestMode)
	if err != nil {
		return SMSJob{}, fmt.Errorf("load message for SMS delivery: %w", err)
	}
	var notification Notification
	if err := json.Unmarshal(payload, &notification); err != nil {
		return SMSJob{}, fmt.Errorf("decode stored SMS notification: %w", err)
	}
	job.ID = messageID
	job.Recipient = notification.Recipient.Phone
	job.Body = notification.Content.Body
	if notification.Content.SMS != "" {
		job.Body = notification.Content.SMS
	}
	job.TrafficClass = notification.TrafficClass
	job.Region = notification.Region
	job.Routing = notification.Routing
	return job, nil
}

func (s *Store) SetNotificationPreference(
	ctx context.Context,
	accountID uuid.UUID,
	recipient, channel string,
	enabled bool,
) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO notification_preferences (account_id, recipient, channel, enabled)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (account_id, recipient, channel) DO UPDATE
		SET enabled = EXCLUDED.enabled, updated_at = now()`,
		accountID, recipient, channel, enabled,
	)
	if err != nil {
		return fmt.Errorf("save notification preference: %w", err)
	}
	return nil
}

func (s *Store) IsNotificationPreferred(
	ctx context.Context,
	accountID uuid.UUID,
	recipient, channel string,
) (bool, error) {
	var enabled bool
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE((
			SELECT enabled FROM notification_preferences
			WHERE account_id = $1 AND lower(recipient) = lower($2) AND channel = $3
			ORDER BY updated_at DESC LIMIT 1
		), false)`, accountID, recipient, channel,
	).Scan(&enabled)
	if err != nil {
		return false, fmt.Errorf("load notification preference: %w", err)
	}
	return enabled, nil
}

func (s *Store) CreateWebhookEndpoint(
	ctx context.Context,
	accountID uuid.UUID,
	endpointURL string,
	encryptedSecret []byte,
	events []string,
) (uuid.UUID, error) {
	var id uuid.UUID
	err := s.pool.QueryRow(ctx, `
		INSERT INTO webhook_endpoints (account_id, endpoint_url, secret_ciphertext, subscribed_events)
		VALUES ($1, $2, $3, $4)
		RETURNING id`, accountID, endpointURL, encryptedSecret, events,
	).Scan(&id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("create customer webhook endpoint: %w", err)
	}
	return id, nil
}

func (s *Store) CreateWebhookDeliveries(
	ctx context.Context,
	tx pgx.Tx,
	messageID uuid.UUID,
	eventType string,
) error {
	var testMode bool
	if err := tx.QueryRow(ctx, `SELECT test_mode FROM messages WHERE id = $1`, messageID).Scan(&testMode); err != nil {
		return fmt.Errorf("load message mode for customer webhook: %w", err)
	}
	payload, err := json.Marshal(map[string]any{
		"id":         uuid.NewString(),
		"type":       eventType,
		"created_at": time.Now().UTC(),
		"test_mode":  testMode,
		"data":       map[string]string{"message_id": messageID.String()},
	})
	if err != nil {
		return fmt.Errorf("encode customer webhook payload: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO webhook_deliveries
			(endpoint_id, event_type, payload, source_id, source_message_id)
		SELECT id, $2, $3, $4, $1
		FROM webhook_endpoints
		WHERE account_id = (SELECT account_id FROM messages WHERE id = $1)
		  AND active = true AND $2 = ANY(subscribed_events)
		ON CONFLICT (endpoint_id, event_type, source_id) WHERE source_id IS NOT NULL DO NOTHING`,
		messageID, eventType, payload, messageID.String(),
	)
	if err != nil {
		return fmt.Errorf("queue customer webhook deliveries: %w", err)
	}
	if tag.RowsAffected() > 0 {
		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, '')`, webhookNotifyChannel); err != nil {
			return fmt.Errorf("notify webhook relay: %w", err)
		}
	}
	return nil
}

func (s *Store) EmitNotificationFailure(ctx context.Context, messageID uuid.UUID) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin notification failure event: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := emitTerminalNotificationEvent(ctx, tx, s, messageID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit notification failure event: %w", err)
	}
	return nil
}

func (s *Store) GetWebhookDelivery(ctx context.Context, deliveryID uuid.UUID) (WebhookDelivery, error) {
	var delivery WebhookDelivery
	delivery.ID = deliveryID
	err := s.pool.QueryRow(ctx, `
		SELECT e.endpoint_url, e.secret_ciphertext, d.event_type, d.payload
		FROM webhook_deliveries d
		JOIN webhook_endpoints e ON e.id = d.endpoint_id
		WHERE d.id = $1 AND e.active = true`, deliveryID,
	).Scan(&delivery.URL, &delivery.EncryptedSecret, &delivery.EventType, &delivery.Payload)
	if err != nil {
		return WebhookDelivery{}, fmt.Errorf("load customer webhook delivery: %w", err)
	}
	return delivery, nil
}

func (s *Store) RecordWebhookAttempt(
	ctx context.Context,
	deliveryID uuid.UUID,
	status string,
	responseCode int,
	deliveryError string,
) error {
	if status != "pending" && status != "delivered" && status != "failed" {
		return fmt.Errorf("unsupported customer webhook delivery status %q", status)
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE webhook_deliveries
		SET attempts = attempts + 1,
		    status = $2,
		    last_response_code = NULLIF($3, 0),
		    last_error = NULLIF($4, ''),
		    next_attempt_at = NULL,
		    delivered_at = CASE WHEN $2 = 'delivered' THEN now() ELSE delivered_at END
		WHERE id = $1`,
		deliveryID, status, responseCode, deliveryError,
	)
	if err != nil {
		return fmt.Errorf("record customer webhook attempt: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// RelayWebhookBatch enqueues up to relayBatchSize pending customer webhook deliveries and
// returns how many were enqueued. queued_at is refreshed on every relay: a stale pending
// delivery is usually still waiting in a long Asynq retry backoff, and without the refresh it
// would be reselected on every loop and starve newer deliveries.
func (s *Store) RelayWebhookBatch(
	ctx context.Context,
	enqueue func(context.Context, uuid.UUID) error,
) (int, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin webhook outbox transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id FROM webhook_deliveries
		WHERE status = 'pending'
		  AND (queued_at IS NULL OR queued_at < now() - interval '2 hours')
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED LIMIT $1`, relayBatchSize)
	if err != nil {
		return 0, fmt.Errorf("select pending webhook deliveries: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		return 0, fmt.Errorf("read pending webhook deliveries: %w", err)
	}
	relayed := make([]uuid.UUID, 0, len(ids))
	var enqueueErr error
	for _, id := range ids {
		if err := enqueue(ctx, id); err != nil {
			enqueueErr = fmt.Errorf("enqueue webhook delivery %s: %w", id, err)
			break
		}
		relayed = append(relayed, id)
	}
	if len(relayed) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE webhook_deliveries SET queued_at = now() WHERE id = ANY($1)`, relayed); err != nil {
			return 0, errors.Join(enqueueErr, fmt.Errorf("mark webhook deliveries enqueued: %w", err))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, errors.Join(enqueueErr, fmt.Errorf("commit webhook outbox transaction: %w", err))
	}
	return len(relayed), enqueueErr
}

// RunWebhookRelay relays customer webhook deliveries until ctx is cancelled.
func (s *Store) RunWebhookRelay(
	ctx context.Context,
	enqueue func(context.Context, uuid.UUID) error,
	logger *slog.Logger,
) {
	s.runRelay(ctx, webhookNotifyChannel, "webhook outbox relay", func(ctx context.Context) (int, error) {
		return s.RelayWebhookBatch(ctx, enqueue)
	}, logger)
}

func (s *Store) IsSMSSuppressed(ctx context.Context, accountID uuid.UUID, recipient string) (bool, error) {
	var suppressed bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM suppressions
			WHERE account_id = $1 AND channel = 'sms' AND recipient = $2
		)`, accountID, recipient).Scan(&suppressed)
	if err != nil {
		return false, fmt.Errorf("check SMS suppression: %w", err)
	}
	return suppressed, nil
}

func (s *Store) HasSMSConsent(ctx context.Context, accountID uuid.UUID, recipient, scope string) (bool, error) {
	var consented bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM consent_records
			WHERE account_id = $1 AND recipient = $2 AND channel = 'sms'
			  AND scope IN ($3, 'all') AND withdrawn_at IS NULL
		)`, accountID, recipient, scope).Scan(&consented)
	if err != nil {
		return false, fmt.Errorf("check SMS consent: %w", err)
	}
	return consented, nil
}

func (s *Store) RecordSMSConsent(
	ctx context.Context,
	accountID uuid.UUID,
	recipient, scope, source string,
	consented bool,
) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin SMS consent update: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if consented {
		withdrawQuery := `
			UPDATE consent_records SET withdrawn_at = now()
			WHERE account_id = $1 AND recipient = $2 AND channel = 'sms' AND withdrawn_at IS NULL
			  AND ($3 = 'all' OR scope = $3)`
		if _, err := tx.Exec(ctx, withdrawQuery, accountID, recipient, scope); err != nil {
			return fmt.Errorf("replace existing SMS consent: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO consent_records (account_id, recipient, channel, scope, consent_source, consented_at)
			VALUES ($1, $2, 'sms', $3, $4, now())`,
			accountID, recipient, scope, source,
		); err != nil {
			return fmt.Errorf("record SMS consent: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM suppressions
			WHERE account_id = $1 AND recipient = $2 AND channel = 'sms'
			  AND reason IN ('opt_out', 'stop')`, accountID, recipient,
		); err != nil {
			return fmt.Errorf("clear re-consented SMS opt-out: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE consent_records SET withdrawn_at = now()
			WHERE account_id = $1 AND recipient = $2 AND channel = 'sms' AND withdrawn_at IS NULL`,
			accountID, recipient,
		); err != nil {
			return fmt.Errorf("withdraw SMS consent: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO suppressions (account_id, recipient, channel, reason, source)
			VALUES ($1, $2, 'sms', 'opt_out', $3)
			ON CONFLICT (account_id, recipient, channel) DO UPDATE
			SET reason = 'opt_out', source = EXCLUDED.source, created_at = now()`,
			accountID, recipient, source,
		); err != nil {
			return fmt.Errorf("suppress opted-out SMS recipient: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit SMS consent update: %w", err)
	}
	return nil
}

func (s *Store) ReserveSMSBudget(
	ctx context.Context,
	jobID, accountID uuid.UUID,
	countryCode string,
	costUSD, monthlyCapUSD float64,
	rateLimitPerMinute int,
) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin SMS budget reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var released bool
	err = tx.QueryRow(ctx, `
		SELECT released FROM sms_cost_reservations WHERE message_id = $1 FOR UPDATE`, jobID,
	).Scan(&released)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("load SMS cost reservation: %w", err)
	}
	if err == nil && !released {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit existing SMS cost reservation: %w", err)
		}
		return nil
	}

	var rateCount int
	err = tx.QueryRow(ctx, `
		INSERT INTO sms_rate_limits (account_id, window_start, send_count)
		VALUES ($1, date_trunc('minute', now()), 1)
		ON CONFLICT (account_id, window_start) DO UPDATE
		SET send_count = sms_rate_limits.send_count + 1
		WHERE sms_rate_limits.send_count < $2
		RETURNING send_count`, accountID, rateLimitPerMinute,
	).Scan(&rateCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrSMSRateLimit
	}
	if err != nil {
		return fmt.Errorf("reserve SMS account rate limit: %w", err)
	}

	var periodStart time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO sms_country_spend (account_id, country_code, period_start, amount_reserved_usd)
		VALUES ($1, $2, date_trunc('month', now() AT TIME ZONE 'UTC')::date, $3)
		ON CONFLICT (account_id, country_code, period_start) DO UPDATE
		SET amount_reserved_usd = sms_country_spend.amount_reserved_usd + EXCLUDED.amount_reserved_usd
		WHERE sms_country_spend.amount_reserved_usd + EXCLUDED.amount_reserved_usd <= $4
		RETURNING period_start`, accountID, countryCode, costUSD, monthlyCapUSD,
	).Scan(&periodStart)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrSMSCountryBudget
	}
	if err != nil {
		return fmt.Errorf("reserve SMS country budget: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO sms_cost_reservations (message_id, account_id, country_code, period_start, amount_usd, released)
		VALUES ($1, $2, $3, $4, $5, false)
		ON CONFLICT (message_id) DO UPDATE
		SET account_id = EXCLUDED.account_id, country_code = EXCLUDED.country_code,
		    period_start = EXCLUDED.period_start, amount_usd = EXCLUDED.amount_usd, released = false`,
		jobID, accountID, countryCode, periodStart, costUSD,
	)
	if err != nil {
		return fmt.Errorf("save SMS cost reservation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit SMS cost reservation: %w", err)
	}
	return nil
}

func (s *Store) ReleaseSMSBudget(ctx context.Context, jobID uuid.UUID) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin SMS budget release: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var accountID uuid.UUID
	var countryCode string
	var periodStart time.Time
	var amount float64
	err = tx.QueryRow(ctx, `
		UPDATE sms_cost_reservations
		SET released = true
		WHERE message_id = $1 AND released = false
		RETURNING account_id, country_code, period_start, amount_usd`, jobID,
	).Scan(&accountID, &countryCode, &periodStart, &amount)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit empty SMS budget release: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("load SMS budget reservation to release: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sms_country_spend
		SET amount_reserved_usd = GREATEST(0, amount_reserved_usd - $4)
		WHERE account_id = $1 AND country_code = $2 AND period_start = $3`,
		accountID, countryCode, periodStart, amount,
	); err != nil {
		return fmt.Errorf("release SMS country spend: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit SMS budget release: %w", err)
	}
	return nil
}

func (s *Store) ReserveSMSConfirmationBudget(
	ctx context.Context,
	reservationID string,
	accountID uuid.UUID,
	countryCode string,
	costUSD, monthlyCapUSD float64,
	rateLimitPerMinute int,
) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin SMS confirmation budget reservation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var rateCount int
	err = tx.QueryRow(ctx, `
		INSERT INTO sms_rate_limits (account_id, window_start, send_count)
		VALUES ($1, date_trunc('minute', now()), 1)
		ON CONFLICT (account_id, window_start) DO UPDATE
		SET send_count = sms_rate_limits.send_count + 1
		WHERE sms_rate_limits.send_count < $2
		RETURNING send_count`, accountID, rateLimitPerMinute,
	).Scan(&rateCount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSMSRateLimit
		}
		return fmt.Errorf("reserve SMS confirmation rate limit: %w", err)
	}

	var released bool
	err = tx.QueryRow(ctx, `
		SELECT released FROM sms_system_cost_reservations
		WHERE reservation_id = $1 FOR UPDATE`, reservationID,
	).Scan(&released)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("load SMS confirmation cost reservation: %w", err)
	}
	if err == nil && !released {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit existing SMS confirmation reservation: %w", err)
		}
		return nil
	}

	var periodStart time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO sms_country_spend (account_id, country_code, period_start, amount_reserved_usd)
		VALUES ($1, $2, date_trunc('month', now() AT TIME ZONE 'UTC')::date, $3)
		ON CONFLICT (account_id, country_code, period_start) DO UPDATE
		SET amount_reserved_usd = sms_country_spend.amount_reserved_usd + EXCLUDED.amount_reserved_usd
		WHERE sms_country_spend.amount_reserved_usd + EXCLUDED.amount_reserved_usd <= $4
		RETURNING period_start`, accountID, countryCode, costUSD, monthlyCapUSD,
	).Scan(&periodStart)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrSMSCountryBudget
	}
	if err != nil {
		return fmt.Errorf("reserve SMS confirmation country budget: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO sms_system_cost_reservations
			(reservation_id, account_id, country_code, period_start, amount_usd, released)
		VALUES ($1, $2, $3, $4, $5, false)
		ON CONFLICT (reservation_id) DO UPDATE
		SET account_id = EXCLUDED.account_id, country_code = EXCLUDED.country_code,
		    period_start = EXCLUDED.period_start, amount_usd = EXCLUDED.amount_usd, released = false`,
		reservationID, accountID, countryCode, periodStart, costUSD,
	); err != nil {
		return fmt.Errorf("save SMS confirmation cost reservation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit SMS confirmation budget reservation: %w", err)
	}
	return nil
}

func (s *Store) ReleaseSMSConfirmationBudget(ctx context.Context, reservationID string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin SMS confirmation budget release: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var accountID uuid.UUID
	var countryCode string
	var periodStart time.Time
	var amount float64
	err = tx.QueryRow(ctx, `
		UPDATE sms_system_cost_reservations SET released = true
		WHERE reservation_id = $1 AND released = false
		RETURNING account_id, country_code, period_start, amount_usd`, reservationID,
	).Scan(&accountID, &countryCode, &periodStart, &amount)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit empty SMS confirmation budget release: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("load SMS confirmation reservation to release: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE sms_country_spend
		SET amount_reserved_usd = GREATEST(0, amount_reserved_usd - $4)
		WHERE account_id = $1 AND country_code = $2 AND period_start = $3`,
		accountID, countryCode, periodStart, amount,
	); err != nil {
		return fmt.Errorf("release SMS confirmation country spend: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit SMS confirmation budget release: %w", err)
	}
	return nil
}

func (s *Store) BeginSMSDelivery(
	ctx context.Context,
	job SMSJob,
	provider, countryCode string,
	estimatedCostUSD float64,
) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin SMS delivery: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT id FROM messages WHERE id = $1 FOR UPDATE`, job.ID); err != nil {
		return false, fmt.Errorf("lock message before SMS attempt: %w", err)
	}

	var shouldSend bool
	err = tx.QueryRow(ctx, `
		INSERT INTO sms_deliveries (message_id, provider, recipient, country_code, estimated_cost_usd, status)
		VALUES ($1, $2, $3, $4, $5, 'sending')
		ON CONFLICT (message_id) DO UPDATE
		SET provider = EXCLUDED.provider, recipient = EXCLUDED.recipient,
		    country_code = EXCLUDED.country_code, estimated_cost_usd = EXCLUDED.estimated_cost_usd,
		    status = 'sending', error = NULL, updated_at = now()
		WHERE sms_deliveries.status NOT IN ('accepted', 'sent', 'delivered')
		RETURNING true`, job.ID, provider, job.Recipient, countryCode, estimatedCostUSD,
	).Scan(&shouldSend)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit existing SMS delivery state: %w", err)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("record SMS delivery start: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO delivery_attempts (message_id, channel, attempt_number, status, provider)
		SELECT $1, 'sms', COALESCE(MAX(attempt_number), 0) + 1, 'sending', $2
		FROM delivery_attempts WHERE message_id = $1 AND channel = 'sms'`,
		job.ID, provider,
	); err != nil {
		return false, fmt.Errorf("record SMS delivery attempt: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE messages SET status = 'processing', updated_at = now() WHERE id = $1`, job.ID); err != nil {
		return false, fmt.Errorf("mark SMS message processing: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit SMS delivery start: %w", err)
	}
	return true, nil
}

func (s *Store) CompleteSMSDelivery(ctx context.Context, messageID uuid.UUID, providerMessageID string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin SMS delivery completion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT id FROM messages WHERE id = $1 FOR UPDATE`, messageID); err != nil {
		return fmt.Errorf("lock message before SMS completion: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE sms_deliveries
		SET status = CASE WHEN status = 'sending' THEN 'accepted' ELSE status END,
		    provider_message_id = NULLIF($2, ''), error = NULL, updated_at = now()
		WHERE message_id = $1`, messageID, providerMessageID)
	if err != nil {
		return fmt.Errorf("record provider-accepted SMS: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.New("SMS delivery record does not exist")
	}
	if err := reconcileMessageStatus(ctx, tx, messageID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO message_events (message_id, event_type, details)
		VALUES ($1, 'sms.accepted_by_provider', jsonb_build_object('provider_message_id', $2::text))`,
		messageID, providerMessageID,
	); err != nil {
		return fmt.Errorf("record SMS acceptance event: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE delivery_attempts SET status = 'accepted', finished_at = now()
		WHERE id = (
			SELECT id FROM delivery_attempts WHERE message_id = $1 AND channel = 'sms'
			ORDER BY attempt_number DESC LIMIT 1
		)`, messageID); err != nil {
		return fmt.Errorf("complete SMS delivery attempt: %w", err)
	}
	if err := s.CreateWebhookDeliveries(ctx, tx, messageID, "notification.accepted_by_provider"); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit SMS delivery completion: %w", err)
	}
	return nil
}

func (s *Store) FailSMSDelivery(ctx context.Context, messageID uuid.UUID, deliveryError error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin failed SMS delivery attempt: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		UPDATE sms_deliveries SET status = 'failed', error = $2, updated_at = now()
		WHERE message_id = $1`, messageID, deliveryError.Error()); err != nil {
		return fmt.Errorf("record failed SMS delivery: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE delivery_attempts SET status = 'failed', error = $2, finished_at = now()
		WHERE id = (
			SELECT id FROM delivery_attempts WHERE message_id = $1 AND channel = 'sms'
			ORDER BY attempt_number DESC LIMIT 1
		)`, messageID, deliveryError.Error()); err != nil {
		return fmt.Errorf("record failed SMS delivery attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit failed SMS delivery attempt: %w", err)
	}
	return nil
}

func (s *Store) ProcessSMSWebhookEvent(ctx context.Context, event SMSWebhookEvent) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin SMS webhook transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT id FROM messages WHERE id = $1 FOR UPDATE`, event.MessageID); err != nil {
		return false, fmt.Errorf("lock message before SMS webhook: %w", err)
	}

	var recipient, provider, providerMessageID string
	if err := tx.QueryRow(ctx, `
		SELECT recipient, provider, provider_message_id
		FROM sms_deliveries WHERE message_id = $1`, event.MessageID,
	).Scan(&recipient, &provider, &providerMessageID); err != nil {
		return false, fmt.Errorf("load SMS delivery for webhook: %w", err)
	}
	if recipient != event.Recipient {
		return false, fmt.Errorf("%w: SMS recipient differs", ErrWebhookMismatch)
	}
	if provider != event.Provider || providerMessageID == "" || providerMessageID != event.ProviderMessageID {
		return false, fmt.Errorf("%w: SMS provider message differs", ErrWebhookMismatch)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO sms_webhook_events (provider_event_id, message_id, event_type)
		VALUES ($1, $2, $3) ON CONFLICT (provider_event_id) DO NOTHING`,
		event.ProviderEventID, event.MessageID, event.EventType,
	)
	if err != nil {
		return false, fmt.Errorf("record SMS webhook event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit duplicate SMS webhook: %w", err)
		}
		return false, nil
	}
	switch event.EventType {
	case "sent", "accepted":
		if _, err := tx.Exec(ctx, `
			UPDATE sms_deliveries SET status = 'sent', updated_at = now()
			WHERE message_id = $1 AND status NOT IN ('delivered', 'failed', 'undelivered')`, event.MessageID,
		); err != nil {
			return false, fmt.Errorf("mark SMS sent: %w", err)
		}
	case "delivered":
		deliveryTag, err := tx.Exec(ctx, `
			UPDATE sms_deliveries SET status = 'delivered', error = NULL, updated_at = now()
			WHERE message_id = $1
			  AND EXISTS (SELECT 1 FROM messages WHERE id = $1 AND status != 'failed')
			  AND status != 'undelivered'`, event.MessageID)
		if err != nil {
			return false, fmt.Errorf("mark SMS delivered: %w", err)
		}
		if deliveryTag.RowsAffected() > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE delivery_attempts SET status = 'delivered', finished_at = now()
				WHERE id = (
					SELECT id FROM delivery_attempts WHERE message_id = $1 AND channel = 'sms'
					ORDER BY attempt_number DESC LIMIT 1
				)`, event.MessageID); err != nil {
				return false, fmt.Errorf("complete SMS delivery attempt: %w", err)
			}
			if err := reconcileMessageStatus(ctx, tx, event.MessageID); err != nil {
				return false, err
			}
			if err := emitTerminalNotificationEvent(ctx, tx, s, event.MessageID); err != nil {
				return false, err
			}
		}
	case "failed", "undelivered":
		deliveryTag, err := tx.Exec(ctx, `
			UPDATE sms_deliveries SET status = 'undelivered', error = $2, updated_at = now()
			WHERE message_id = $1 AND status != 'delivered'`, event.MessageID, event.EventType)
		if err != nil {
			return false, fmt.Errorf("mark SMS undelivered: %w", err)
		}
		if deliveryTag.RowsAffected() > 0 {
			if _, err := tx.Exec(ctx, `
				UPDATE delivery_attempts SET status = 'failed', error = $2, finished_at = now()
				WHERE id = (
					SELECT id FROM delivery_attempts WHERE message_id = $1 AND channel = 'sms'
					ORDER BY attempt_number DESC LIMIT 1
				)`, event.MessageID, event.EventType); err != nil {
				return false, fmt.Errorf("mark SMS delivery attempt failed: %w", err)
			}
			if err := reconcileMessageStatus(ctx, tx, event.MessageID); err != nil {
				return false, err
			}
			if err := emitTerminalNotificationEvent(ctx, tx, s, event.MessageID); err != nil {
				return false, err
			}
		}
	default:
		return false, fmt.Errorf("unsupported SMS webhook event type %q", event.EventType)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO message_events (message_id, event_type, details)
		VALUES ($1, $2, jsonb_build_object('provider_event_id', $3::text))`,
		event.MessageID, "sms."+event.EventType, event.ProviderEventID,
	); err != nil {
		return false, fmt.Errorf("record SMS webhook state event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit SMS webhook event: %w", err)
	}
	return true, nil
}

func (s *Store) ProcessSMSInbound(
	ctx context.Context,
	accountID uuid.UUID,
	providerEventID, recipient, text string,
) (bool, error) {
	optOut := isSMSOptOut(text)
	eventType := "inbound"
	if optOut {
		eventType = "opt_out"
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin inbound SMS transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		INSERT INTO sms_inbound_events (provider_event_id, account_id, recipient, event_type)
		VALUES ($1, $2, $3, $4) ON CONFLICT (provider_event_id) DO NOTHING`,
		providerEventID, accountID, recipient, eventType,
	)
	if err != nil {
		return false, fmt.Errorf("record inbound SMS event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var existingType string
		if err := tx.QueryRow(ctx, `
			SELECT event_type FROM sms_inbound_events WHERE provider_event_id = $1`,
			providerEventID,
		).Scan(&existingType); err != nil {
			return false, fmt.Errorf("load duplicate inbound SMS event: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit duplicate inbound SMS event: %w", err)
		}
		return existingType == "opt_out", nil
	}
	if optOut {
		if _, err := tx.Exec(ctx, `
			UPDATE consent_records SET withdrawn_at = now()
			WHERE account_id = $1 AND recipient = $2 AND channel = 'sms' AND withdrawn_at IS NULL`,
			accountID, recipient,
		); err != nil {
			return false, fmt.Errorf("withdraw consent after SMS opt-out: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO suppressions (account_id, recipient, channel, reason, source)
			VALUES ($1, $2, 'sms', 'stop', 'sms_inbound')
			ON CONFLICT (account_id, recipient, channel) DO UPDATE
			SET reason = 'stop', source = 'sms_inbound', created_at = now()`,
			accountID, recipient,
		); err != nil {
			return false, fmt.Errorf("suppress SMS opt-out: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit inbound SMS event: %w", err)
	}
	return optOut, nil
}

func isSMSOptOut(text string) bool {
	switch strings.ToUpper(strings.TrimSpace(text)) {
	case "STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT", "REVOKE":
		return true
	default:
		return false
	}
}

func (s *Store) MarkMessageFailed(ctx context.Context, messageID uuid.UUID, reason string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin failed message update: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	if _, err := tx.Exec(ctx, `
			INSERT INTO message_events (message_id, event_type, details)
			VALUES ($1, 'delivery.failed', jsonb_build_object('reason', $2::text))`, messageID, reason); err != nil {
		return fmt.Errorf("record message failure event: %w", err)
	}
	if err := reconcileMessageStatus(ctx, tx, messageID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit failed message update: %w", err)
	}
	return nil
}

func (s *Store) FailMessageRetriesExhausted(
	ctx context.Context,
	messageID uuid.UUID,
	retries, maximumRetries int,
) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin retry exhaustion update: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var lockedID uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT id FROM messages WHERE id = $1 FOR UPDATE`, messageID,
	).Scan(&lockedID); err != nil {
		return fmt.Errorf("lock message after retry exhaustion: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE messages
		SET status = CASE WHEN
			EXISTS (SELECT 1 FROM delivery_attempts WHERE message_id = $1 AND status = 'delivered')
			OR EXISTS (SELECT 1 FROM email_deliveries WHERE message_id = $1 AND status = 'delivered')
			OR EXISTS (SELECT 1 FROM sms_deliveries WHERE message_id = $1 AND status = 'delivered')
			THEN 'partially_delivered' ELSE 'failed' END,
			updated_at = now()
		WHERE id = $1`, messageID); err != nil {
		return fmt.Errorf("mark retry-exhausted message terminal: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO message_events (message_id, event_type, details)
		VALUES ($1, 'delivery.retry_exhausted',
			jsonb_build_object('retry_count', $2::integer, 'max_retries', $3::integer))`,
		messageID, retries, maximumRetries,
	); err != nil {
		return fmt.Errorf("record notification retry exhaustion: %w", err)
	}
	if err := emitTerminalNotificationEvent(ctx, tx, s, messageID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit retry exhaustion update: %w", err)
	}
	return nil
}

func (s *Store) RecordChannelFailure(
	ctx context.Context,
	messageID uuid.UUID,
	channel, reason string,
) error {
	if channel != "email" && channel != "sms" {
		return fmt.Errorf("unsupported delivery channel %q", channel)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin failed channel attempt: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT id FROM messages WHERE id = $1 FOR UPDATE`, messageID); err != nil {
		return fmt.Errorf("lock message before failed channel attempt: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO delivery_attempts (message_id, channel, attempt_number, status, error, finished_at)
		SELECT $1, $2, COALESCE(MAX(attempt_number), 0) + 1, 'failed', $3, now()
		FROM delivery_attempts WHERE message_id = $1 AND channel = $2`,
		messageID, channel, reason,
	); err != nil {
		return fmt.Errorf("record failed %s channel attempt: %w", channel, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO message_events (message_id, event_type, details)
		VALUES ($1, 'delivery.attempt_failed', jsonb_build_object('channel', $2::text, 'reason', $3::text))`,
		messageID, channel, reason,
	); err != nil {
		return fmt.Errorf("record failed %s channel event: %w", channel, err)
	}
	if err := reconcileMessageStatus(ctx, tx, messageID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit failed channel attempt: %w", err)
	}
	return nil
}

func (s *Store) RecordChannelSkipped(ctx context.Context, messageID uuid.UUID, channel string) error {
	if channel != "email" && channel != "sms" {
		return fmt.Errorf("unsupported delivery channel %q", channel)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin skipped channel attempt: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT id FROM messages WHERE id = $1 FOR UPDATE`, messageID); err != nil {
		return fmt.Errorf("lock message before skipped channel attempt: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO delivery_attempts (message_id, channel, attempt_number, status, finished_at)
		SELECT $1, $2, COALESCE(MAX(attempt_number), 0) + 1, 'skipped', now()
		FROM delivery_attempts WHERE message_id = $1 AND channel = $2`,
		messageID, channel,
	); err != nil {
		return fmt.Errorf("record skipped %s channel attempt: %w", channel, err)
	}
	if err := reconcileMessageStatus(ctx, tx, messageID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit skipped channel attempt: %w", err)
	}
	return nil
}

func emitTerminalNotificationEvent(
	ctx context.Context,
	tx pgx.Tx,
	repository *Store,
	messageID uuid.UUID,
) error {
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM messages WHERE id = $1`, messageID).Scan(&status); err != nil {
		return fmt.Errorf("load reconciled notification status: %w", err)
	}
	eventType := ""
	switch status {
	case "delivered":
		eventType = "notification.delivered"
	case "partially_delivered":
		eventType = "notification.partially_delivered"
	case "failed":
		eventType = "notification.failed"
	default:
		return nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO message_events (message_id, event_type, details)
		VALUES ($1, $2, '{}'::jsonb)`, messageID, eventType); err != nil {
		return fmt.Errorf("record terminal notification event: %w", err)
	}
	if err := repository.CreateWebhookDeliveries(ctx, tx, messageID, eventType); err != nil {
		return err
	}
	return nil
}

func reconcileMessageStatus(ctx context.Context, tx pgx.Tx, messageID uuid.UUID) error {
	_, err := tx.Exec(ctx, `
		UPDATE messages AS m
		SET status = (
			SELECT CASE
				-- A waterfall succeeds when any channel delivers once every channel is settled;
				-- earlier synchronous failures are the reason the fallback ran.
				WHEN m.payload->'routing'->>'strategy' = 'waterfall'
					AND COUNT(*) FILTER (WHERE channel_status = 'delivered') > 0
					AND COUNT(*) FILTER (WHERE channel_status IN ('delivered', 'failed', 'undelivered', 'skipped')) = COUNT(*)
					THEN 'delivered'
				WHEN COUNT(*) FILTER (WHERE channel_status != 'skipped') > 0
					AND COUNT(*) FILTER (WHERE channel_status IN ('delivered', 'skipped')) = COUNT(*)
					THEN 'delivered'
				WHEN COUNT(*) FILTER (WHERE channel_status IN ('delivered', 'failed', 'undelivered', 'skipped')) = COUNT(*)
					AND COUNT(*) FILTER (WHERE channel_status = 'delivered') > 0
					THEN 'partially_delivered'
				WHEN COUNT(*) FILTER (WHERE channel_status IN ('failed', 'undelivered', 'skipped')) = COUNT(*)
					AND COUNT(*) FILTER (WHERE channel_status = 'failed' OR channel_status = 'undelivered') > 0
					THEN 'failed'
				WHEN BOOL_OR(channel_status IN ('sending', 'accepted')) THEN 'processing'
				ELSE 'queued'
			END
			FROM (
				SELECT requested.channel,
					CASE requested.channel
						WHEN 'email' THEN COALESCE(
							(SELECT status FROM email_deliveries WHERE message_id = m.id),
							(SELECT status FROM delivery_attempts WHERE message_id = m.id AND channel = 'email'
							 ORDER BY attempt_number DESC LIMIT 1))
						WHEN 'sms' THEN COALESCE(
							(SELECT CASE status WHEN 'sent' THEN 'accepted' WHEN 'undelivered' THEN 'failed' ELSE status END
							 FROM sms_deliveries WHERE message_id = m.id),
							(SELECT status FROM delivery_attempts WHERE message_id = m.id AND channel = 'sms'
							 ORDER BY attempt_number DESC LIMIT 1))
					END AS channel_status
				FROM unnest(m.channels) AS requested(channel)
			) AS channel_states
		),
		updated_at = now()
		WHERE m.id = $1`, messageID)
	if err != nil {
		return fmt.Errorf("reconcile notification delivery status: %w", err)
	}
	return nil
}

// SetDomainVerificationResult records a DNS check. A failed check that will be retried keeps
// the domain's current status, so re-checking a verified domain does not interrupt sending;
// only a final failure (retries exhausted) marks the domain failed.
func (s *Store) SetDomainVerificationResult(
	ctx context.Context,
	domainID uuid.UUID,
	verified, final bool,
	verificationError string,
) error {
	result, err := s.pool.Exec(ctx, `
			UPDATE domains
			SET verification_status = CASE
			        WHEN $2 THEN 'verified'
			        WHEN $3 THEN 'failed'
			        ELSE verification_status
			    END,
			    verified_at = CASE
			        WHEN $2 THEN COALESCE(verified_at, now())
			        WHEN $3 THEN NULL
			        ELSE verified_at
			    END,
			    last_verification_error = NULLIF($4, ''),
			    last_verification_at = now(),
			    updated_at = now()
			WHERE id = $1`, domainID, verified, final, verificationError)
	if err != nil {
		return fmt.Errorf("save domain verification result: %w", err)
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) GetEmailJob(ctx context.Context, messageID uuid.UUID) (EmailJob, error) {
	var job EmailJob
	var payload []byte
	var channels []string
	err := s.pool.QueryRow(ctx, `
			SELECT account_id, channels, payload, test_mode
			FROM messages
			WHERE id = $1`, messageID,
	).Scan(&job.AccountID, &channels, &payload, &job.TestMode)
	if err != nil {
		return EmailJob{}, fmt.Errorf("load message for email delivery: %w", err)
	}
	if !contains(channels, "email") {
		return EmailJob{}, errors.New("message does not request email delivery")
	}
	var notification Notification
	if err := json.Unmarshal(payload, &notification); err != nil {
		return EmailJob{}, fmt.Errorf("decode stored notification payload: %w", err)
	}
	job.ID = messageID
	job.Channels = channels
	job.Recipient = notification.Recipient.Email
	job.Subject = notification.Content.Subject
	job.Body = notification.Content.Body
	job.HTML = notification.Content.HTML
	job.TrafficClass = notification.TrafficClass
	job.Region = notification.Region
	job.Routing = notification.Routing
	return job, nil
}

func (s *Store) GetDKIMKey(ctx context.Context, accountID uuid.UUID, domainName string) (DKIMKey, error) {
	var key DKIMKey
	err := s.pool.QueryRow(ctx, `
			SELECT k.selector, k.encrypted_private_key
			FROM domains d
			JOIN dkim_keys k ON k.domain_id = d.id
			WHERE d.account_id = $1 AND lower(d.domain_name) = lower($2)
			  AND d.verification_status = 'verified'`, accountID, domainName,
	).Scan(&key.Selector, &key.EncryptedPrivateKey)
	if err != nil {
		return DKIMKey{}, fmt.Errorf("load verified DKIM key for %s: %w", domainName, err)
	}
	return key, nil
}

func (s *Store) BeginEmailDelivery(ctx context.Context, job EmailJob, provider string) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin email delivery: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	if _, err := tx.Exec(ctx, `SELECT id FROM messages WHERE id = $1 FOR UPDATE`, job.ID); err != nil {
		return false, fmt.Errorf("lock message before email attempt: %w", err)
	}
	var existingStatus string
	err = tx.QueryRow(ctx, `
		SELECT status FROM email_deliveries WHERE message_id = $1 FOR UPDATE`, job.ID,
	).Scan(&existingStatus)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("load existing email delivery: %w", err)
	}
	if err == nil && (existingStatus == "accepted" || existingStatus == "delivered") {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit existing email delivery state: %w", err)
		}
		return false, nil
	}
	if _, err := tx.Exec(ctx, `
			INSERT INTO email_deliveries (message_id, provider, status)
			VALUES ($1, $2, 'sending')
			ON CONFLICT (message_id) DO UPDATE
			SET provider = EXCLUDED.provider, status = 'sending', error = NULL, updated_at = now()`,
		job.ID, provider,
	); err != nil {
		return false, fmt.Errorf("record email delivery start: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO delivery_attempts (message_id, channel, attempt_number, status, provider)
		SELECT $1, 'email', COALESCE(MAX(attempt_number), 0) + 1, 'sending', $2
		FROM delivery_attempts WHERE message_id = $1 AND channel = 'email'`,
		job.ID, provider,
	); err != nil {
		return false, fmt.Errorf("record email delivery attempt: %w", err)
	}
	if _, err := tx.Exec(ctx, `
			UPDATE messages SET status = 'processing', updated_at = now() WHERE id = $1`, job.ID); err != nil {
		return false, fmt.Errorf("update message to processing: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit email delivery start: %w", err)
	}
	return true, nil
}

func (s *Store) CompleteEmailDelivery(ctx context.Context, messageID uuid.UUID, providerMessageID string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin email delivery completion: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	if _, err := tx.Exec(ctx, `SELECT id FROM messages WHERE id = $1 FOR UPDATE`, messageID); err != nil {
		return fmt.Errorf("lock message before email completion: %w", err)
	}
	tag, err := tx.Exec(ctx, `
			UPDATE email_deliveries
			SET status = CASE WHEN status = 'sending' THEN 'accepted' ELSE status END,
			    provider_message_id = NULLIF($2, ''), error = NULL, updated_at = now()
			WHERE message_id = $1`, messageID, providerMessageID)
	if err != nil {
		return fmt.Errorf("record delivered email: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.New("email delivery record does not exist")
	}
	if err := reconcileMessageStatus(ctx, tx, messageID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
			INSERT INTO message_events (message_id, event_type, details)
			VALUES ($1, 'email.accepted_by_provider', jsonb_build_object('provider_message_id', $2::text))`,
		messageID, providerMessageID,
	); err != nil {
		return fmt.Errorf("record email delivery event: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE delivery_attempts SET status = 'accepted', finished_at = now()
		WHERE id = (
			SELECT id FROM delivery_attempts WHERE message_id = $1 AND channel = 'email'
			ORDER BY attempt_number DESC LIMIT 1
		)`, messageID); err != nil {
		return fmt.Errorf("complete email delivery attempt: %w", err)
	}
	if err := s.CreateWebhookDeliveries(ctx, tx, messageID, "notification.accepted_by_provider"); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit email delivery completion: %w", err)
	}
	return nil
}

func (s *Store) FailEmailDelivery(ctx context.Context, messageID uuid.UUID, deliveryError error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin failed email delivery attempt: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
			UPDATE email_deliveries SET status = 'failed', error = $2, updated_at = now()
			WHERE message_id = $1`, messageID, deliveryError.Error()); err != nil {
		return fmt.Errorf("record failed email delivery: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE delivery_attempts SET status = 'failed', error = $2, finished_at = now()
		WHERE id = (
			SELECT id FROM delivery_attempts WHERE message_id = $1 AND channel = 'email'
			ORDER BY attempt_number DESC LIMIT 1
		)`, messageID, deliveryError.Error()); err != nil {
		return fmt.Errorf("record failed email delivery attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit failed email delivery attempt: %w", err)
	}
	return nil
}

func (s *Store) ProcessEmailWebhookEvent(ctx context.Context, event EmailWebhookEvent) (bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin email webhook transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	if _, err := tx.Exec(ctx, `SELECT id FROM messages WHERE id = $1 FOR UPDATE`, event.MessageID); err != nil {
		return false, fmt.Errorf("lock message before email webhook: %w", err)
	}

	var accountID uuid.UUID
	var payload []byte
	if err := tx.QueryRow(ctx, `
			SELECT account_id, payload FROM messages WHERE id = $1`, event.MessageID,
	).Scan(&accountID, &payload); err != nil {
		return false, fmt.Errorf("load message for email webhook: %w", err)
	}
	var notification Notification
	if err := json.Unmarshal(payload, &notification); err != nil {
		return false, fmt.Errorf("decode message for email webhook: %w", err)
	}
	if !contains(notification.Channels, "email") ||
		!stringsEqualFold(notification.Recipient.Email, event.Recipient) {
		return false, fmt.Errorf("%w: email recipient differs", ErrWebhookMismatch)
	}

	tag, err := tx.Exec(ctx, `
			INSERT INTO email_webhook_events (provider_event_id, message_id, recipient, event_type)
			VALUES ($1, $2, lower($3), $4)
			ON CONFLICT (provider_event_id) DO NOTHING`,
		event.ProviderEventID, event.MessageID, event.Recipient, event.EventType,
	)
	if err != nil {
		return false, fmt.Errorf("record email webhook event: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit duplicate webhook event: %w", err)
		}
		return false, nil
	}
	if _, err := tx.Exec(ctx, `
			INSERT INTO message_events (message_id, event_type, details)
			VALUES ($1, $2, jsonb_build_object('provider_event_id', $3::text, 'recipient', lower($4)))`,
		event.MessageID, "email."+event.EventType, event.ProviderEventID, event.Recipient,
	); err != nil {
		return false, fmt.Errorf("record email webhook state event: %w", err)
	}
	if event.EventType == "bounce" || event.EventType == "complaint" {
		reason := "hard_bounce"
		if event.EventType == "complaint" {
			reason = "complaint"
		}
		if _, err := tx.Exec(ctx, `
				INSERT INTO suppressions (account_id, recipient, channel, reason, source)
				VALUES ($1, lower($2), 'email', $3, 'sendgrid_webhook')
				ON CONFLICT (account_id, recipient, channel) DO NOTHING`,
			accountID, event.Recipient, reason,
		); err != nil {
			return false, fmt.Errorf("suppress email recipient: %w", err)
		}
	}
	if event.EventType == "bounce" {
		deliveryTag, err := tx.Exec(ctx, `
				UPDATE email_deliveries SET status = 'failed', error = 'hard bounce', updated_at = now()
				WHERE message_id = $1 AND status != 'delivered'`, event.MessageID)
		if err != nil {
			return false, fmt.Errorf("mark bounced email delivery: %w", err)
		}
		if deliveryTag.RowsAffected() > 0 {
			if _, err := tx.Exec(ctx, `
			UPDATE delivery_attempts SET status = 'failed', error = 'hard bounce', finished_at = now()
			WHERE id = (
				SELECT id FROM delivery_attempts WHERE message_id = $1 AND channel = 'email'
				ORDER BY attempt_number DESC LIMIT 1
			)`, event.MessageID); err != nil {
				return false, fmt.Errorf("mark bounced email attempt: %w", err)
			}
			if err := reconcileMessageStatus(ctx, tx, event.MessageID); err != nil {
				return false, err
			}
			if err := emitTerminalNotificationEvent(ctx, tx, s, event.MessageID); err != nil {
				return false, err
			}
		}
	}
	if event.EventType == "delivered" {
		deliveryTag, err := tx.Exec(ctx, `
				UPDATE email_deliveries SET status = 'delivered', updated_at = now()
				WHERE message_id = $1 AND status != 'failed'`, event.MessageID)
		if err != nil {
			return false, fmt.Errorf("mark email delivered: %w", err)
		}
		if deliveryTag.RowsAffected() > 0 {
			if _, err := tx.Exec(ctx, `
			UPDATE delivery_attempts SET status = 'delivered', finished_at = now()
			WHERE id = (
				SELECT id FROM delivery_attempts WHERE message_id = $1 AND channel = 'email'
				ORDER BY attempt_number DESC LIMIT 1
			)`, event.MessageID); err != nil {
				return false, fmt.Errorf("complete email delivery attempt: %w", err)
			}
			if err := reconcileMessageStatus(ctx, tx, event.MessageID); err != nil {
				return false, err
			}
			if err := emitTerminalNotificationEvent(ctx, tx, s, event.MessageID); err != nil {
				return false, err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit email webhook event: %w", err)
	}
	return true, nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func stringsEqualFold(left, right string) bool {
	return strings.EqualFold(strings.TrimSpace(left), strings.TrimSpace(right))
}

func (s *Store) AcceptNotification(
	ctx context.Context,
	accountID, idempotencyKey string,
	notification Notification,
	request []byte,
) (uuid.UUID, bool, error) {
	return s.Accept(ctx, Acceptance{
		AccountID: accountID, IdempotencyKey: idempotencyKey, Notification: notification, Payload: request,
	})
}

// Acceptance is a validated notification ready to persist.
type Acceptance struct {
	AccountID      string
	IdempotencyKey string
	Notification   Notification
	// Payload is the notification as stored (for a template, already rendered).
	Payload []byte
	// RequestHash identifies the request as the client sent it, so a replay matches even if
	// the template's published version changed since. Nil hashes Payload.
	RequestHash []byte
	// TestMode messages are simulated by the worker and never reach a provider. Test and live
	// messages have separate idempotency keys.
	TestMode bool
	// Audit, when non-nil, is written in the same transaction as a newly accepted message
	// with ResourceID set to the message ID. Idempotent replays are not audited again.
	Audit *AuditEvent
}

// Accept persists a notification with its outbox row and accepted event, or returns the
// existing message for an idempotent replay.
func (s *Store) Accept(ctx context.Context, acceptance Acceptance) (uuid.UUID, bool, error) {
	accountID, idempotencyKey := acceptance.AccountID, acceptance.IdempotencyKey
	notification, request, audit := acceptance.Notification, acceptance.Payload, acceptance.Audit
	requestHash := acceptance.RequestHash
	if requestHash == nil {
		sum := sha256.Sum256(request)
		requestHash = sum[:]
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("begin notification transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var messageID uuid.UUID
	err = tx.QueryRow(ctx, `
		INSERT INTO messages
			(account_id, idempotency_key, event_name, channels, payload, request_hash, region, test_mode)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8)
		ON CONFLICT (account_id, test_mode, idempotency_key) DO NOTHING
		RETURNING id`, accountID, idempotencyKey, notification.Event, notification.Channels,
		request, requestHash, notification.Region, acceptance.TestMode).Scan(&messageID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, fmt.Errorf("insert message: %w", err)
	}

	if errors.Is(err, pgx.ErrNoRows) {
		var existingHash []byte
		if err := tx.QueryRow(ctx, `
			SELECT id, request_hash
			FROM messages
			WHERE account_id = $1 AND test_mode = $2 AND idempotency_key = $3`,
			accountID, acceptance.TestMode, idempotencyKey,
		).Scan(&messageID, &existingHash); err != nil {
			return uuid.Nil, false, fmt.Errorf("load idempotent message: %w", err)
		}
		if !equalBytes(existingHash, requestHash) {
			return uuid.Nil, false, ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, false, fmt.Errorf("commit idempotent lookup: %w", err)
		}
		return messageID, true, nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO message_events (message_id, event_type, details)
		VALUES ($1, 'accepted', '{}'::jsonb)`, messageID); err != nil {
		return uuid.Nil, false, fmt.Errorf("record accepted message event: %w", err)
	}
	if err := s.CreateWebhookDeliveries(ctx, tx, messageID, "notification.accepted"); err != nil {
		return uuid.Nil, false, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox (message_id, task_type)
		VALUES ($1, 'notification:deliver')`, messageID); err != nil {
		return uuid.Nil, false, fmt.Errorf("insert notification outbox event: %w", err)
	}
	if audit != nil {
		event := *audit
		event.ResourceID = messageID.String()
		if err := insertAuditEvent(ctx, tx, event); err != nil {
			return uuid.Nil, false, err
		}
	}
	// Delivered on commit; wakes the worker's relay without waiting for its poll.
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, '')`, outboxNotifyChannel); err != nil {
		return uuid.Nil, false, fmt.Errorf("notify outbox relay: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, false, fmt.Errorf("commit notification: %w", err)
	}
	return messageID, false, nil
}

// RelayOutboxBatch enqueues up to relayBatchSize unpublished notification outbox rows, in
// the order they were accepted, and returns how many were enqueued. Rows are marked published
// only after their enqueue succeeds; on an enqueue error the rows already enqueued are still
// marked and the error is returned.
func (s *Store) RelayOutboxBatch(
	ctx context.Context,
	enqueue func(ctx context.Context, messageID uuid.UUID, trafficClass string) error,
) (int, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin outbox relay transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT o.message_id, COALESCE(m.payload->>'traffic_class', '')
		FROM outbox o
		JOIN messages m ON m.id = o.message_id
		WHERE o.published_at IS NULL
		ORDER BY o.created_at
		FOR UPDATE OF o SKIP LOCKED
		LIMIT $1`, relayBatchSize)
	if err != nil {
		return 0, fmt.Errorf("select outbox rows: %w", err)
	}
	type pending struct {
		MessageID    uuid.UUID
		TrafficClass string
	}
	batch, err := pgx.CollectRows(rows, pgx.RowToStructByPos[pending])
	if err != nil {
		return 0, fmt.Errorf("read outbox rows: %w", err)
	}
	published := make([]uuid.UUID, 0, len(batch))
	var enqueueErr error
	for _, row := range batch {
		if err := enqueue(ctx, row.MessageID, row.TrafficClass); err != nil {
			enqueueErr = fmt.Errorf("enqueue message %s: %w", row.MessageID, err)
			break
		}
		published = append(published, row.MessageID)
	}
	if len(published) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE outbox SET published_at = now()
			WHERE message_id = ANY($1) AND published_at IS NULL`, published); err != nil {
			return 0, errors.Join(enqueueErr, fmt.Errorf("mark outbox rows published: %w", err))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, errors.Join(enqueueErr, fmt.Errorf("commit outbox relay: %w", err))
	}
	return len(published), enqueueErr
}

// RunOutboxRelay relays accepted notifications to the queue until ctx is cancelled.
func (s *Store) RunOutboxRelay(
	ctx context.Context,
	enqueue func(ctx context.Context, messageID uuid.UUID, trafficClass string) error,
	logger *slog.Logger,
) {
	s.runRelay(ctx, outboxNotifyChannel, "outbox relay", func(ctx context.Context) (int, error) {
		return s.RelayOutboxBatch(ctx, enqueue)
	}, logger)
}

// runRelay drains work in batches, then sleeps until a PostgreSQL NOTIFY on channel says new
// work was committed, or until relayPollInterval passes. The poll covers missed notifications,
// a lost listener connection, and stale webhook deliveries that need re-relaying.
func (s *Store) runRelay(
	ctx context.Context,
	channel, name string,
	relay func(context.Context) (int, error),
	logger *slog.Logger,
) {
	var listener *pgxpool.Conn
	defer func() {
		if listener != nil {
			listener.Release()
		}
	}()
	for ctx.Err() == nil {
		if listener == nil {
			listener = s.listen(ctx, channel, name, logger)
		}
		relayed, err := relay(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error(name+" failed", "error", err)
			if !wait(ctx, time.Second) {
				return
			}
			continue
		}
		if relayed == relayBatchSize {
			continue
		}
		if listener == nil {
			if !wait(ctx, relayPollInterval) {
				return
			}
			continue
		}
		waitCtx, cancel := context.WithTimeout(ctx, relayPollInterval)
		err = listener.Conn().PgConn().WaitForNotification(waitCtx)
		cancel()
		if err != nil && !pgconn.Timeout(err) && ctx.Err() == nil {
			logger.Warn(name+" lost its notification listener; polling until it reconnects", "error", err)
			listener.Release()
			listener = nil
		}
	}
}

func (s *Store) listen(ctx context.Context, channel, name string, logger *slog.Logger) *pgxpool.Conn {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		if ctx.Err() == nil {
			logger.Warn(name+" could not acquire a notification listener", "error", err)
		}
		return nil
	}
	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		if ctx.Err() == nil {
			logger.Warn(name+" could not listen for notifications", "error", err)
		}
		conn.Release()
		return nil
	}
	return conn
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var diff byte
	for i := range left {
		diff |= left[i] ^ right[i]
	}
	return diff == 0
}
