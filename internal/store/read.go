package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrInvalidCursor reports a pagination cursor that was not issued by this service.
var ErrInvalidCursor = errors.New("pagination cursor is invalid")

// ErrSuppressionProtected reports a suppression that the API must not remove: complaints
// and SMS opt-outs are the recipient's own choice and are cleared only by new consent.
var ErrSuppressionProtected = errors.New("suppression can only be cleared by the recipient")

const maxPageSize = 200

type MessageSummary struct {
	ID           uuid.UUID `json:"id"`
	Event        string    `json:"event"`
	Channels     []string  `json:"channels"`
	Status       string    `json:"status"`
	TrafficClass string    `json:"traffic_class"`
	Region       string    `json:"region,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type MessageRecipient struct {
	Email string `json:"email,omitempty"`
	Phone string `json:"phone,omitempty"`
}

type ChannelDelivery struct {
	Channel           string    `json:"channel"`
	Provider          string    `json:"provider"`
	ProviderMessageID string    `json:"provider_message_id,omitempty"`
	Status            string    `json:"status"`
	Error             string    `json:"error,omitempty"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type DeliveryAttempt struct {
	Channel       string     `json:"channel"`
	AttemptNumber int        `json:"attempt_number"`
	Status        string     `json:"status"`
	Provider      string     `json:"provider,omitempty"`
	Error         string     `json:"error,omitempty"`
	StartedAt     time.Time  `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
}

type MessageEvent struct {
	Type      string          `json:"type"`
	Details   json.RawMessage `json:"details"`
	CreatedAt time.Time       `json:"created_at"`
}

// MessageDetail is a message with its delivery state and timeline. Message content is not
// returned: it is retained only for delivery and is removed by retention.
type MessageDetail struct {
	MessageSummary
	Routing    Routing           `json:"routing"`
	Recipient  MessageRecipient  `json:"recipient"`
	Deliveries []ChannelDelivery `json:"deliveries"`
	Attempts   []DeliveryAttempt `json:"attempts"`
	Events     []MessageEvent    `json:"events"`
}

type MessageFilter struct {
	Status    string
	Recipient string
	Since     *time.Time
	Until     *time.Time
	Limit     int
	Cursor    string
	TestMode  bool
}

type MessagePage struct {
	Messages   []MessageSummary `json:"messages"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

type WebhookEndpointSummary struct {
	ID        uuid.UUID `json:"id"`
	URL       string    `json:"url"`
	Events    []string  `json:"events"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
}

type DomainSummary struct {
	ID                    uuid.UUID  `json:"id"`
	Name                  string     `json:"domain"`
	VerificationStatus    string     `json:"verification_status"`
	LastVerificationError string     `json:"last_verification_error,omitempty"`
	VerifiedAt            *time.Time `json:"verified_at,omitempty"`
	CreatedAt             time.Time  `json:"created_at"`
}

type Suppression struct {
	ID        uuid.UUID `json:"-"`
	Channel   string    `json:"channel"`
	Recipient string    `json:"recipient"`
	Reason    string    `json:"reason"`
	Source    string    `json:"source"`
	CreatedAt time.Time `json:"created_at"`
}

type SuppressionFilter struct {
	Channel   string
	Recipient string
	Limit     int
	Cursor    string
}

type SuppressionPage struct {
	Suppressions []Suppression `json:"suppressions"`
	NextCursor   string        `json:"next_cursor,omitempty"`
}

type ConsentRecord struct {
	Scope       string     `json:"scope"`
	Source      string     `json:"source"`
	ConsentedAt time.Time  `json:"consented_at"`
	WithdrawnAt *time.Time `json:"withdrawn_at,omitempty"`
}

// GetMessage returns one message of the account with its deliveries, attempts, and events.
// Test-mode callers see only test messages and live callers only live ones.
func (s *Store) GetMessage(ctx context.Context, accountID, messageID uuid.UUID, testMode bool) (MessageDetail, error) {
	var detail MessageDetail
	var payload []byte
	err := s.pool.QueryRow(ctx, `
		SELECT id, event_name, channels, status, COALESCE(payload->>'traffic_class', ''),
		       COALESCE(region, ''), created_at, updated_at, payload
		FROM messages WHERE account_id = $1 AND id = $2 AND test_mode = $3`, accountID, messageID, testMode,
	).Scan(&detail.ID, &detail.Event, &detail.Channels, &detail.Status, &detail.TrafficClass,
		&detail.Region, &detail.CreatedAt, &detail.UpdatedAt, &payload)
	if err != nil {
		return MessageDetail{}, fmt.Errorf("load message: %w", err)
	}
	var notification Notification
	if err := json.Unmarshal(payload, &notification); err != nil {
		return MessageDetail{}, fmt.Errorf("decode stored message: %w", err)
	}
	detail.Routing = notification.Routing
	detail.Recipient = MessageRecipient{Email: notification.Recipient.Email, Phone: notification.Recipient.Phone}

	rows, err := s.pool.Query(ctx, `
		SELECT 'email', provider, COALESCE(provider_message_id, ''), status, COALESCE(error, ''), updated_at
		FROM email_deliveries WHERE message_id = $1
		UNION ALL
		SELECT 'sms', provider, COALESCE(provider_message_id, ''), status, COALESCE(error, ''), updated_at
		FROM sms_deliveries WHERE message_id = $1`, messageID)
	if err != nil {
		return MessageDetail{}, fmt.Errorf("load message deliveries: %w", err)
	}
	if detail.Deliveries, err = pgx.CollectRows(rows, pgx.RowToStructByPos[ChannelDelivery]); err != nil {
		return MessageDetail{}, fmt.Errorf("read message deliveries: %w", err)
	}

	rows, err = s.pool.Query(ctx, `
		SELECT channel, attempt_number, status, COALESCE(provider, ''), COALESCE(error, ''), started_at, finished_at
		FROM delivery_attempts WHERE message_id = $1
		ORDER BY started_at, id`, messageID)
	if err != nil {
		return MessageDetail{}, fmt.Errorf("load delivery attempts: %w", err)
	}
	if detail.Attempts, err = pgx.CollectRows(rows, pgx.RowToStructByPos[DeliveryAttempt]); err != nil {
		return MessageDetail{}, fmt.Errorf("read delivery attempts: %w", err)
	}

	rows, err = s.pool.Query(ctx, `
		SELECT event_type, details, created_at FROM message_events
		WHERE message_id = $1 ORDER BY created_at, id`, messageID)
	if err != nil {
		return MessageDetail{}, fmt.Errorf("load message events: %w", err)
	}
	if detail.Events, err = pgx.CollectRows(rows, pgx.RowToStructByPos[MessageEvent]); err != nil {
		return MessageDetail{}, fmt.Errorf("read message events: %w", err)
	}
	return detail, nil
}

// ListMessages returns the account's messages, newest first, filtered and paginated.
func (s *Store) ListMessages(ctx context.Context, accountID uuid.UUID, filter MessageFilter) (MessagePage, error) {
	query := queryBuilder{args: []any{accountID, filter.TestMode}}
	query.sql.WriteString(`
		SELECT id, event_name, channels, status, COALESCE(payload->>'traffic_class', ''),
		       COALESCE(region, ''), created_at, updated_at
		FROM messages WHERE account_id = $1 AND test_mode = $2`)
	if filter.Status != "" {
		query.where("status = %s", filter.Status)
	}
	if recipient := strings.TrimSpace(filter.Recipient); recipient != "" {
		if strings.Contains(recipient, "@") {
			query.where("lower(payload->'recipient'->>'email') = lower(%s)", recipient)
		} else {
			query.where("payload->'recipient'->>'phone' = %s", recipient)
		}
	}
	if filter.Since != nil {
		query.where("created_at >= %s", *filter.Since)
	}
	if filter.Until != nil {
		query.where("created_at < %s", *filter.Until)
	}
	if err := query.after(filter.Cursor); err != nil {
		return MessagePage{}, err
	}
	limit := pageSize(filter.Limit)
	query.page(limit)

	rows, err := s.pool.Query(ctx, query.sql.String(), query.args...)
	if err != nil {
		return MessagePage{}, fmt.Errorf("list messages: %w", err)
	}
	messages, err := pgx.CollectRows(rows, pgx.RowToStructByPos[MessageSummary])
	if err != nil {
		return MessagePage{}, fmt.Errorf("read messages: %w", err)
	}
	page := MessagePage{Messages: messages}
	if len(messages) > limit {
		page.Messages = messages[:limit]
		last := page.Messages[limit-1]
		page.NextCursor = encodeCursor(last.CreatedAt, last.ID)
	}
	return page, nil
}

// ListWebhookEndpoints returns the account's customer webhook endpoints, newest first.
func (s *Store) ListWebhookEndpoints(ctx context.Context, accountID uuid.UUID) ([]WebhookEndpointSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, endpoint_url, subscribed_events, active, created_at
		FROM webhook_endpoints WHERE account_id = $1
		ORDER BY created_at DESC, id DESC`, accountID)
	if err != nil {
		return nil, fmt.Errorf("list customer webhook endpoints: %w", err)
	}
	endpoints, err := pgx.CollectRows(rows, pgx.RowToStructByPos[WebhookEndpointSummary])
	if err != nil {
		return nil, fmt.Errorf("read customer webhook endpoints: %w", err)
	}
	return endpoints, nil
}

// DeactivateWebhookEndpoint stops deliveries to an endpoint. Pending deliveries to it are
// marked failed when the worker next picks them up.
func (s *Store) DeactivateWebhookEndpoint(ctx context.Context, accountID, endpointID uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE webhook_endpoints SET active = false
		WHERE account_id = $1 AND id = $2`, accountID, endpointID)
	if err != nil {
		return fmt.Errorf("deactivate customer webhook endpoint: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// RotateWebhookSecret replaces an active endpoint's encrypted signing secret.
func (s *Store) RotateWebhookSecret(ctx context.Context, accountID, endpointID uuid.UUID, encryptedSecret []byte) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE webhook_endpoints SET secret_ciphertext = $3
		WHERE account_id = $1 AND id = $2 AND active = true`, accountID, endpointID, encryptedSecret)
	if err != nil {
		return fmt.Errorf("rotate customer webhook secret: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// ListDomains returns the account's sending domains, newest first.
func (s *Store) ListDomains(ctx context.Context, accountID uuid.UUID) ([]DomainSummary, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, domain_name, verification_status, COALESCE(last_verification_error, ''),
		       verified_at, created_at
		FROM domains WHERE account_id = $1
		ORDER BY created_at DESC, id DESC`, accountID)
	if err != nil {
		return nil, fmt.Errorf("list sending domains: %w", err)
	}
	domains, err := pgx.CollectRows(rows, pgx.RowToStructByPos[DomainSummary])
	if err != nil {
		return nil, fmt.Errorf("read sending domains: %w", err)
	}
	return domains, nil
}

// ListSuppressions returns the account's suppressions, newest first, filtered and paginated.
func (s *Store) ListSuppressions(ctx context.Context, accountID uuid.UUID, filter SuppressionFilter) (SuppressionPage, error) {
	query := queryBuilder{args: []any{accountID}}
	query.sql.WriteString(`
		SELECT id, channel, recipient, reason, source, created_at
		FROM suppressions WHERE account_id = $1`)
	if filter.Channel != "" {
		query.where("channel = %s", filter.Channel)
	}
	if recipient := strings.TrimSpace(filter.Recipient); recipient != "" {
		query.where("lower(recipient) = lower(%s)", recipient)
	}
	if err := query.after(filter.Cursor); err != nil {
		return SuppressionPage{}, err
	}
	limit := pageSize(filter.Limit)
	query.page(limit)

	rows, err := s.pool.Query(ctx, query.sql.String(), query.args...)
	if err != nil {
		return SuppressionPage{}, fmt.Errorf("list suppressions: %w", err)
	}
	suppressions, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Suppression])
	if err != nil {
		return SuppressionPage{}, fmt.Errorf("read suppressions: %w", err)
	}
	page := SuppressionPage{Suppressions: suppressions}
	if len(suppressions) > limit {
		page.Suppressions = suppressions[:limit]
		last := page.Suppressions[limit-1]
		page.NextCursor = encodeCursor(last.CreatedAt, last.ID)
	}
	return page, nil
}

// DeleteEmailSuppression removes a hard-bounce email suppression, for example after the
// recipient's mailbox is fixed. Complaints are protected: the recipient asked not to be mailed.
func (s *Store) DeleteEmailSuppression(ctx context.Context, accountID uuid.UUID, recipient string) error {
	var reason string
	err := s.pool.QueryRow(ctx, `
		DELETE FROM suppressions
		WHERE account_id = $1 AND channel = 'email' AND lower(recipient) = lower($2)
		  AND reason = 'hard_bounce'
		RETURNING reason`, accountID, recipient,
	).Scan(&reason)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("delete email suppression: %w", err)
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM suppressions
			WHERE account_id = $1 AND channel = 'email' AND lower(recipient) = lower($2)
		)`, accountID, recipient,
	).Scan(&exists); err != nil {
		return fmt.Errorf("check email suppression: %w", err)
	}
	if exists {
		return ErrSuppressionProtected
	}
	return pgx.ErrNoRows
}

// ListSMSConsents returns the consent history for a phone number, newest first.
func (s *Store) ListSMSConsents(ctx context.Context, accountID uuid.UUID, phone string) ([]ConsentRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT scope, consent_source, consented_at, withdrawn_at
		FROM consent_records
		WHERE account_id = $1 AND recipient = $2 AND channel = 'sms'
		ORDER BY consented_at DESC, id DESC`, accountID, phone)
	if err != nil {
		return nil, fmt.Errorf("list SMS consents: %w", err)
	}
	records, err := pgx.CollectRows(rows, pgx.RowToStructByPos[ConsentRecord])
	if err != nil {
		return nil, fmt.Errorf("read SMS consents: %w", err)
	}
	return records, nil
}

// queryBuilder appends filters with positional arguments and keyset pagination ordered by
// (created_at, id) descending.
type queryBuilder struct {
	sql  strings.Builder
	args []any
}

func (q *queryBuilder) where(condition string, value any) {
	q.args = append(q.args, value)
	q.sql.WriteString(" AND ")
	q.sql.WriteString(fmt.Sprintf(condition, "$"+strconv.Itoa(len(q.args))))
}

func (q *queryBuilder) after(cursor string) error {
	if cursor == "" {
		return nil
	}
	createdAt, id, err := decodeCursor(cursor)
	if err != nil {
		return err
	}
	q.args = append(q.args, createdAt, id)
	fmt.Fprintf(&q.sql, " AND (created_at, id) < ($%d, $%d)", len(q.args)-1, len(q.args))
	return nil
}

func (q *queryBuilder) page(limit int) {
	q.args = append(q.args, limit+1)
	fmt.Fprintf(&q.sql, " ORDER BY created_at DESC, id DESC LIMIT $%d", len(q.args))
}

func pageSize(limit int) int {
	if limit < 1 {
		return 50
	}
	return min(limit, maxPageSize)
}

func encodeCursor(createdAt time.Time, id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString(
		[]byte(strconv.FormatInt(createdAt.UnixMicro(), 10) + ":" + id.String()),
	)
}

func decodeCursor(cursor string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	micros, idText, found := strings.Cut(string(raw), ":")
	if !found {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	value, err := strconv.ParseInt(micros, 10, 64)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	id, err := uuid.Parse(idText)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrInvalidCursor
	}
	return time.UnixMicro(value).UTC(), id, nil
}
