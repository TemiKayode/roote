package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"go.opentelemetry.io/otel/attribute"

	"github.com/TemiKayode/roote/internal/observability"
)

const TaskNotificationDelivery = "notification:deliver"
const TaskDomainVerification = "domain:verify"
const TaskSMSStopConfirmation = "sms:stop-confirm"
const TaskWebhookDelivery = "webhook:deliver"

// Work is split into queues per class so a marketing send or a burst of webhook retries
// cannot delay transactional messages. Each queue name is the configured base plus a suffix.
const (
	ClassTransactional = "transactional"
	ClassMarketing     = "marketing"
	ClassWebhooks      = "webhooks"
	ClassSystem        = "system"
)

// queueWeights are Asynq's weighted-priority shares: a worker picks transactional work
// six times as often as system work, but no queue is starved.
var queueWeights = []struct {
	class  string
	weight int
}{
	{ClassTransactional, 6},
	{ClassWebhooks, 2},
	{ClassMarketing, 2},
	{ClassSystem, 1},
}

// QueueName returns the Asynq queue for a work class.
func QueueName(base, class string) string {
	return base + "-" + class
}

// Queues returns the worker's queue weights. The bare base queue is included so tasks
// enqueued before queues were split still drain.
func Queues(base string) map[string]int {
	queues := map[string]int{base: 1}
	for _, queue := range queueWeights {
		queues[QueueName(base, queue.class)] = queue.weight
	}
	return queues
}

// QueueNames lists every queue the worker consumes, most important first.
func QueueNames(base string) []string {
	names := make([]string, 0, len(queueWeights)+1)
	for _, queue := range queueWeights {
		names = append(names, QueueName(base, queue.class))
	}
	return append(names, base)
}

func notificationQueue(base, trafficClass string) string {
	if trafficClass == "marketing" {
		return QueueName(base, ClassMarketing)
	}
	return QueueName(base, ClassTransactional)
}

type notificationPayload struct {
	MessageID   string `json:"message_id"`
	TraceParent string `json:"trace_parent,omitempty"`
}

type domainVerificationPayload struct {
	AccountID string `json:"account_id"`
	DomainID  string `json:"domain_id"`
}

type smsStopConfirmationPayload struct {
	Provider string `json:"provider"`
	Phone    string `json:"phone"`
	EventID  string `json:"event_id"`
}

type webhookDeliveryPayload struct {
	DeliveryID string `json:"delivery_id"`
}

func EnqueueNotification(
	ctx context.Context,
	client *asynq.Client,
	baseQueue string,
	messageID uuid.UUID,
	trafficClass, traceParent string,
) error {
	payload, err := json.Marshal(notificationPayload{MessageID: messageID.String(), TraceParent: traceParent})
	if err != nil {
		return fmt.Errorf("marshal notification task: %w", err)
	}
	task := asynq.NewTask(TaskNotificationDelivery, payload)
	_, err = client.EnqueueContext(ctx, task,
		asynq.Queue(notificationQueue(baseQueue, trafficClass)),
		asynq.TaskID(messageID.String()),
		asynq.Unique(24*time.Hour),
	)
	if err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) && !errors.Is(err, asynq.ErrDuplicateTask) {
		return fmt.Errorf("enqueue notification task: %w", err)
	}
	return nil
}

func EnqueueDomainVerification(
	ctx context.Context,
	client *asynq.Client,
	baseQueue, accountID string,
	domainID uuid.UUID,
) error {
	payload, err := json.Marshal(domainVerificationPayload{AccountID: accountID, DomainID: domainID.String()})
	if err != nil {
		return fmt.Errorf("marshal domain verification task: %w", err)
	}
	_, err = client.EnqueueContext(ctx, asynq.NewTask(TaskDomainVerification, payload),
		asynq.Queue(QueueName(baseQueue, ClassSystem)),
		asynq.TaskID("verify-"+domainID.String()),
		asynq.Unique(5*time.Minute),
	)
	if err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) && !errors.Is(err, asynq.ErrDuplicateTask) {
		return fmt.Errorf("enqueue domain verification task: %w", err)
	}
	return nil
}

func EnqueueSMSStopConfirmation(
	ctx context.Context,
	client *asynq.Client,
	baseQueue, provider, phone, eventID string,
) error {
	payload, err := json.Marshal(smsStopConfirmationPayload{
		Provider: provider,
		Phone:    phone,
		EventID:  eventID,
	})
	if err != nil {
		return fmt.Errorf("marshal SMS stop confirmation task: %w", err)
	}
	taskIDHash := sha256.Sum256([]byte(provider + ":" + eventID))
	_, err = client.EnqueueContext(ctx, asynq.NewTask(TaskSMSStopConfirmation, payload),
		asynq.Queue(QueueName(baseQueue, ClassTransactional)),
		asynq.TaskID("sms-stop-"+hex.EncodeToString(taskIDHash[:])),
		asynq.Unique(24*time.Hour),
	)
	if err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) && !errors.Is(err, asynq.ErrDuplicateTask) {
		return fmt.Errorf("enqueue SMS stop confirmation: %w", err)
	}
	return nil
}

func EnqueueWebhookDelivery(
	ctx context.Context,
	client *asynq.Client,
	baseQueue string,
	deliveryID uuid.UUID,
) error {
	payload, err := json.Marshal(webhookDeliveryPayload{DeliveryID: deliveryID.String()})
	if err != nil {
		return fmt.Errorf("marshal webhook delivery task: %w", err)
	}
	_, err = client.EnqueueContext(ctx, asynq.NewTask(TaskWebhookDelivery, payload),
		asynq.Queue(QueueName(baseQueue, ClassWebhooks)),
		asynq.TaskID("webhook-"+deliveryID.String()),
		asynq.MaxRetry(8),
		asynq.Unique(24*time.Hour),
	)
	if err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) && !errors.Is(err, asynq.ErrDuplicateTask) {
		return fmt.Errorf("enqueue webhook delivery: %w", err)
	}
	return nil
}

type NotificationHandler func(context.Context, uuid.UUID) error
type DomainVerificationHandler func(context.Context, string, uuid.UUID) error
type SMSStopConfirmationHandler func(context.Context, string, string, string) error
type WebhookDeliveryHandler func(context.Context, uuid.UUID) error

func NewServeMux(
	logger *slog.Logger,
	handleNotification NotificationHandler,
	handleDomainVerification DomainVerificationHandler,
	handleSMSStopConfirmation SMSStopConfirmationHandler,
	handleWebhookDelivery WebhookDeliveryHandler,
) *asynq.ServeMux {
	mux := asynq.NewServeMux()
	mux.HandleFunc(TaskNotificationDelivery, func(ctx context.Context, task *asynq.Task) error {
		var payload notificationPayload
		if err := json.Unmarshal(task.Payload(), &payload); err != nil {
			return fmt.Errorf("decode notification task: %w", err)
		}
		if _, err := uuid.Parse(payload.MessageID); err != nil {
			return fmt.Errorf("invalid message ID in task: %w", err)
		}
		messageID, _ := uuid.Parse(payload.MessageID)
		if handleNotification == nil {
			return fmt.Errorf("notification handler is not configured")
		}
		ctx, span := observability.StartTaskSpan(ctx, TaskNotificationDelivery, payload.TraceParent)
		span.SetAttributes(attribute.String("roote.message_id", messageID.String()))
		err := handleNotification(ctx, messageID)
		observability.EndSpan(span, err)
		return err
	})
	mux.HandleFunc(TaskDomainVerification, func(ctx context.Context, task *asynq.Task) error {
		var payload domainVerificationPayload
		if err := json.Unmarshal(task.Payload(), &payload); err != nil {
			return fmt.Errorf("decode domain verification task: %w", err)
		}
		domainID, err := uuid.Parse(payload.DomainID)
		if err != nil {
			return fmt.Errorf("invalid domain ID in task: %w", err)
		}
		if _, err := uuid.Parse(payload.AccountID); err != nil {
			return fmt.Errorf("invalid account ID in task: %w", err)
		}
		if handleDomainVerification == nil {
			return fmt.Errorf("domain verification handler is not configured")
		}
		logger.InfoContext(ctx, "domain DNS verification started", "domain_id", domainID)
		ctx, span := observability.StartTaskSpan(ctx, TaskDomainVerification, "")
		err = handleDomainVerification(ctx, payload.AccountID, domainID)
		observability.EndSpan(span, err)
		return err
	})
	mux.HandleFunc(TaskSMSStopConfirmation, func(ctx context.Context, task *asynq.Task) error {
		var payload smsStopConfirmationPayload
		if err := json.Unmarshal(task.Payload(), &payload); err != nil {
			return fmt.Errorf("decode SMS stop confirmation task: %w", err)
		}
		if payload.Provider == "" || payload.Phone == "" || payload.EventID == "" {
			return errors.New("invalid SMS stop confirmation task")
		}
		if handleSMSStopConfirmation == nil {
			return errors.New("SMS stop confirmation handler is not configured")
		}
		logger.InfoContext(ctx, "sending SMS opt-out confirmation", "provider", payload.Provider)
		ctx, span := observability.StartTaskSpan(ctx, TaskSMSStopConfirmation, "")
		err := handleSMSStopConfirmation(ctx, payload.Provider, payload.Phone, payload.EventID)
		observability.EndSpan(span, err)
		return err
	})
	mux.HandleFunc(TaskWebhookDelivery, func(ctx context.Context, task *asynq.Task) error {
		var payload webhookDeliveryPayload
		if err := json.Unmarshal(task.Payload(), &payload); err != nil {
			return fmt.Errorf("decode webhook delivery task: %w", err)
		}
		deliveryID, err := uuid.Parse(payload.DeliveryID)
		if err != nil {
			return fmt.Errorf("invalid webhook delivery ID in task: %w", err)
		}
		if handleWebhookDelivery == nil {
			return errors.New("webhook delivery handler is not configured")
		}
		ctx, span := observability.StartTaskSpan(ctx, TaskWebhookDelivery, "")
		span.SetAttributes(attribute.String("roote.webhook_delivery_id", deliveryID.String()))
		err = handleWebhookDelivery(ctx, deliveryID)
		observability.EndSpan(span, err)
		return err
	})
	return mux
}
