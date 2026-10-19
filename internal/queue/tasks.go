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
)

const TaskNotificationDelivery = "notification:deliver"
const TaskDomainVerification = "domain:verify"
const TaskSMSStopConfirmation = "sms:stop-confirm"

type notificationPayload struct {
	MessageID string `json:"message_id"`
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

func EnqueueNotification(ctx context.Context, client *asynq.Client, queueName string, messageID uuid.UUID) error {
	payload, err := json.Marshal(notificationPayload{MessageID: messageID.String()})
	if err != nil {
		return fmt.Errorf("marshal notification task: %w", err)
	}
	task := asynq.NewTask(TaskNotificationDelivery, payload)
	_, err = client.EnqueueContext(ctx, task,
		asynq.Queue(queueName),
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
	queueName, accountID string,
	domainID uuid.UUID,
) error {
	payload, err := json.Marshal(domainVerificationPayload{AccountID: accountID, DomainID: domainID.String()})
	if err != nil {
		return fmt.Errorf("marshal domain verification task: %w", err)
	}
	_, err = client.EnqueueContext(ctx, asynq.NewTask(TaskDomainVerification, payload),
		asynq.Queue(queueName),
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
	queueName, provider, phone, eventID string,
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
		asynq.Queue(queueName),
		asynq.TaskID("sms-stop-"+hex.EncodeToString(taskIDHash[:])),
		asynq.Unique(24*time.Hour),
	)
	if err != nil && !errors.Is(err, asynq.ErrTaskIDConflict) && !errors.Is(err, asynq.ErrDuplicateTask) {
		return fmt.Errorf("enqueue SMS stop confirmation: %w", err)
	}
	return nil
}

type NotificationHandler func(context.Context, uuid.UUID) error
type DomainVerificationHandler func(context.Context, string, uuid.UUID) error
type SMSStopConfirmationHandler func(context.Context, string, string, string) error

func NewServeMux(
	logger *slog.Logger,
	handleNotification NotificationHandler,
	handleDomainVerification DomainVerificationHandler,
	handleSMSStopConfirmation SMSStopConfirmationHandler,
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
		return handleNotification(ctx, messageID)
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
		return handleDomainVerification(ctx, payload.AccountID, domainID)
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
		return handleSMSStopConfirmation(ctx, payload.Provider, payload.Phone, payload.EventID)
	})
	return mux
}
