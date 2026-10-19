package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"github.com/TemiKayode/roote/internal/config"
	"github.com/TemiKayode/roote/internal/emailwebhook"
	"github.com/TemiKayode/roote/internal/httpapi"
	"github.com/TemiKayode/roote/internal/queue"
	"github.com/TemiKayode/roote/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("API stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	database, err := store.Open(ctx, cfg.DatabaseURL, cfg.AccountID)
	if err != nil {
		return fmt.Errorf("open application store: %w", err)
	}
	defer database.Close()
	queueClient := asynq.NewClient(asynq.RedisClientOpt{Addr: cfg.RedisURL})
	defer queueClient.Close()

	var webhookVerifier httpapi.WebhookVerifier
	if cfg.SendGridWebhookPublicKey != "" {
		webhookVerifier, err = emailwebhook.NewSendGridVerifier(cfg.SendGridWebhookPublicKey)
		if err != nil {
			return fmt.Errorf("configure SendGrid webhook verification: %w", err)
		}
	}

	server := &http.Server{
		Addr: fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler: httpapi.NewWithAllFeatures(logger, cfg.AccountID, database, httpapi.EmailFeatures{
			DomainStore:   database,
			EncryptionKey: cfg.DKIMEncryptionKey,
			SPFValue:      cfg.EmailSPFValue,
			EnqueueDomainVerification: func(ctx context.Context, accountID string, domainID uuid.UUID) error {
				return queue.EnqueueDomainVerification(ctx, queueClient, cfg.AsynqQueue, accountID, domainID)
			},
			WebhookVerifier: webhookVerifier,
		}, httpapi.SMSFeatures{
			Provider:                cfg.SMSTransport,
			Store:                   database,
			TwilioAuthToken:         cfg.TwilioAuthToken,
			TwilioInboundWebhookURL: cfg.TwilioInboundWebhookURL,
			SMSStatusCallbackURL:    cfg.SMSStatusCallbackURL,
			TelnyxWebhookPublicKey:  cfg.TelnyxWebhookPublicKey,
			EnqueueStopConfirmation: func(ctx context.Context, provider, phone, eventID string) error {
				return queue.EnqueueSMSStopConfirmation(ctx, queueClient, cfg.AsynqQueue, provider, phone, eventID)
			},
		}, httpapi.CustomerWebhookFeatures{
			Store:         database,
			EncryptionKey: cfg.WebhookEncryptionKey,
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("API listening", "port", cfg.HTTPPort)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down HTTP server: %w", err)
		}
	}
	return nil
}
