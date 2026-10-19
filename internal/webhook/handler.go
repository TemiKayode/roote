package webhook

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"

	"github.com/TemiKayode/roote/internal/store"
)

type DeliveryRepository interface {
	GetWebhookDelivery(context.Context, uuid.UUID) (store.WebhookDelivery, error)
	RecordWebhookAttempt(context.Context, uuid.UUID, string, int, string) error
}

type DeliverySender interface {
	Send(context.Context, store.WebhookDelivery) (int, error)
}

type Handler struct {
	repository DeliveryRepository
	sender     DeliverySender
}

func NewHandler(repository DeliveryRepository, sender DeliverySender) *Handler {
	return &Handler{repository: repository, sender: sender}
}

func (h *Handler) Handle(
	ctx context.Context,
	deliveryID uuid.UUID,
	retryCount, maxRetries int,
) error {
	delivery, err := h.repository.GetWebhookDelivery(ctx, deliveryID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("load customer webhook delivery: %w", err)
		}
		// The delivery or its message was deleted, or the endpoint was deactivated.
		if recordErr := h.repository.RecordWebhookAttempt(
			ctx, deliveryID, "failed", 0, err.Error(),
		); recordErr != nil && !errors.Is(recordErr, pgx.ErrNoRows) {
			return errors.Join(err, recordErr)
		}
		return fmt.Errorf("%w: %v", asynq.SkipRetry, err)
	}
	responseCode, sendErr := h.sender.Send(ctx, delivery)
	status, lastError := "delivered", ""
	if sendErr != nil {
		status, lastError = "pending", sendErr.Error()
		if retryCount >= maxRetries {
			status = "failed"
		}
	}
	if recordErr := h.repository.RecordWebhookAttempt(
		ctx, deliveryID, status, responseCode, lastError,
	); recordErr != nil {
		return errors.Join(sendErr, recordErr)
	}
	if sendErr == nil {
		return nil
	}
	if retryCount >= maxRetries {
		return fmt.Errorf("%w: customer webhook retry limit reached: %v", asynq.SkipRetry, sendErr)
	}
	return sendErr
}
