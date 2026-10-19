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
	"github.com/redis/go-redis/v9"

	"github.com/TemiKayode/roote/internal/config"
	"github.com/TemiKayode/roote/internal/delivery"
	"github.com/TemiKayode/roote/internal/observability"
	"github.com/TemiKayode/roote/internal/queue"
	"github.com/TemiKayode/roote/internal/store"
	"github.com/TemiKayode/roote/internal/transport/email"
	"github.com/TemiKayode/roote/internal/transport/sms"
	"github.com/TemiKayode/roote/internal/webhook"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker stopped", "error", err)
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

	emailTransport, err := newEmailTransport(cfg)
	if err != nil {
		return fmt.Errorf("configure email transport: %w", err)
	}
	smsTransport, err := newSMSTransport(cfg)
	if err != nil {
		return fmt.Errorf("configure SMS transport: %w", err)
	}
	processor := delivery.NewProcessor(database, emailTransport, smsTransport, cfg, logger)
	webhookSender := webhook.NewSender(cfg.WebhookEncryptionKey, cfg.WebhookPreviousKeys...)
	webhookHandler := webhook.NewHandler(database, webhookSender)

	redis := asynq.RedisClientOpt{Addr: cfg.RedisURL}
	client := asynq.NewClient(redis)
	defer client.Close()
	redisClient := redisv9Client(cfg.RedisURL)
	defer redisClient.Close()
	metrics := observability.NewMetrics()
	server := asynq.NewServer(redis, asynq.Config{
		Queues:          queue.Queues(cfg.AsynqQueue),
		Concurrency:     cfg.WorkerConcurrency,
		ShutdownTimeout: time.Duration(cfg.ShutdownTimeoutSeconds) * time.Second,
		Logger:          slogAsynqLogger{logger: logger},
	})
	if err := server.Start(queue.NewServeMux(
		logger,
		func(ctx context.Context, messageID uuid.UUID) error {
			return observeTask(metrics, queue.TaskNotificationDelivery, func() error {
				retries, _ := asynq.GetRetryCount(ctx)
				maximumRetries, _ := asynq.GetMaxRetry(ctx)
				return processor.HandleNotificationTask(ctx, messageID, retries, maximumRetries)
			})
		},
		func(ctx context.Context, accountID string, domainID uuid.UUID) error {
			return observeTask(metrics, queue.TaskDomainVerification, func() error {
				retries, _ := asynq.GetRetryCount(ctx)
				maximumRetries, _ := asynq.GetMaxRetry(ctx)
				return processor.VerifyDomain(ctx, accountID, domainID, retries, maximumRetries)
			})
		},
		func(ctx context.Context, provider, phone, eventID string) error {
			return observeTask(metrics, queue.TaskSMSStopConfirmation, func() error {
				return processor.SendSMSStopConfirmation(ctx, provider, phone, eventID)
			})
		},
		func(ctx context.Context, deliveryID uuid.UUID) error {
			retries, _ := asynq.GetRetryCount(ctx)
			maxRetries, _ := asynq.GetMaxRetry(ctx)
			return observeTask(metrics, queue.TaskWebhookDelivery, func() error {
				return webhookHandler.Handle(ctx, deliveryID, retries, maxRetries)
			})
		},
	)); err != nil {
		return fmt.Errorf("start Asynq server: %w", err)
	}
	go database.RunOutboxRelay(ctx, func(ctx context.Context, messageID uuid.UUID, trafficClass string) error {
		return queue.EnqueueNotification(ctx, client, cfg.AsynqQueue, messageID, trafficClass)
	}, logger)
	go database.RunWebhookRelay(ctx, func(ctx context.Context, deliveryID uuid.UUID) error {
		return queue.EnqueueWebhookDelivery(ctx, client, cfg.AsynqQueue, deliveryID)
	}, logger)
	accountID, err := uuid.Parse(cfg.AccountID)
	if err != nil {
		return fmt.Errorf("parse configured account ID: %w", err)
	}
	go runRetention(ctx, database, accountID, cfg.RetentionDays, logger)
	go runSMSReconciliation(ctx, processor, logger)
	metricsMux := http.NewServeMux()
	metricsMux.HandleFunc("/metrics", metrics.Handler)
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	metricsMux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		readyCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := database.Ping(readyCtx); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		if err := redisClient.Ping(readyCtx).Err(); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	metricsServer := &http.Server{
		Addr: fmt.Sprintf(":%d", cfg.MetricsPort), Handler: metricsMux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20,
	}
	metricsErrors := make(chan error, 1)
	go func() {
		metricsErrors <- metricsServer.ListenAndServe()
	}()

	logger.Info("worker started", "queue", cfg.AsynqQueue)
	var metricsServerErr error
	select {
	case <-ctx.Done():
	case serveErr := <-metricsErrors:
		if !errors.Is(serveErr, http.ErrServerClosed) {
			metricsServerErr = fmt.Errorf("serve worker metrics: %w", serveErr)
			logger.Error("worker metrics server stopped", "error", serveErr)
			stop()
		}
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(
		context.Background(), time.Duration(cfg.ShutdownTimeoutSeconds)*time.Second,
	)
	defer cancelShutdown()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("could not shut down worker metrics server cleanly", "error", err)
	}
	shutdownComplete := make(chan struct{})
	go func() {
		server.Shutdown()
		close(shutdownComplete)
	}()
	select {
	case <-shutdownComplete:
	case <-shutdownCtx.Done():
		return errors.Join(metricsServerErr, fmt.Errorf("worker shutdown timed out: %w", shutdownCtx.Err()))
	}
	logger.Info("worker stopped")
	return metricsServerErr
}

func redisv9Client(address string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: address})
}

func observeTask(metrics *observability.Metrics, name string, handler func() error) error {
	err := handler()
	result := "success"
	if err != nil {
		result = "failure"
	}
	metrics.ObserveTask(name, result)
	return err
}

func runRetention(
	ctx context.Context,
	database *store.Store,
	accountID uuid.UUID,
	retentionDays int,
	logger *slog.Logger,
) {
	run := func() {
		for {
			deleted, err := database.PurgeExpiredMessages(ctx, accountID, retentionDays)
			if err != nil {
				if ctx.Err() == nil {
					logger.ErrorContext(ctx, "message retention purge failed", "error", err)
				}
				return
			}
			if deleted > 0 {
				logger.InfoContext(ctx, "expired messages purged", "count", deleted)
			}
			if deleted < 1000 {
				break
			}
		}
		if _, err := database.PurgeExpiredRateWindows(ctx); err != nil && ctx.Err() == nil {
			logger.ErrorContext(ctx, "SMS rate window purge failed", "error", err)
		}
	}
	run()
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run()
		}
	}
}

// runSMSReconciliation polls the SMS provider every five minutes for messages whose delivery
// receipt is overdue, draining a backlog in consecutive batches.
func runSMSReconciliation(ctx context.Context, processor *delivery.Processor, logger *slog.Logger) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for ctx.Err() == nil {
			applied, err := processor.ReconcileSMS(ctx)
			if err != nil {
				if ctx.Err() == nil {
					logger.ErrorContext(ctx, "SMS reconciliation failed", "error", err)
				}
				break
			}
			if applied > 0 {
				logger.InfoContext(ctx, "reconciled SMS delivery statuses", "count", applied)
			}
			if applied == 0 {
				break
			}
		}
	}
}

func newSMSTransport(cfg config.Config) (sms.Transport, error) {
	switch cfg.SMSTransport {
	case "disabled":
		return nil, nil
	case "twilio":
		return sms.NewTwilioTransport(
			cfg.TwilioAccountSID, cfg.TwilioAuthToken, cfg.SMSFrom, cfg.SMSStatusCallbackURL,
		)
	case "telnyx":
		return sms.NewTelnyxTransport(cfg.TelnyxAPIKey, cfg.SMSFrom, cfg.SMSStatusCallbackURL)
	default:
		return nil, fmt.Errorf("unsupported SMS transport %q", cfg.SMSTransport)
	}
}

func newEmailTransport(cfg config.Config) (email.Transport, error) {
	switch cfg.EmailTransport {
	case "disabled":
		return nil, nil
	case "sendgrid":
		return email.NewSendGridTransport(
			cfg.SendGridAPIKey, cfg.SendGridTransactionalPool, cfg.SendGridMarketingPool,
		)
	case "kumo-smtp":
		return email.NewSMTPTransport(
			cfg.SMTPHost, cfg.SMTPPort, cfg.SMTPUsername, cfg.SMTPPassword, cfg.SMTPRequireTLS,
		)
	default:
		return nil, fmt.Errorf("unsupported email transport %q", cfg.EmailTransport)
	}
}

type slogAsynqLogger struct {
	logger *slog.Logger
}

func (l slogAsynqLogger) Debug(args ...interface{}) {
	l.logger.Debug(fmt.Sprint(args...))
}

func (l slogAsynqLogger) Info(args ...interface{}) {
	l.logger.Info(fmt.Sprint(args...))
}

func (l slogAsynqLogger) Warn(args ...interface{}) {
	l.logger.Warn(fmt.Sprint(args...))
}

func (l slogAsynqLogger) Error(args ...interface{}) {
	l.logger.Error(fmt.Sprint(args...))
}

func (l slogAsynqLogger) Fatal(args ...interface{}) {
	l.logger.Error(fmt.Sprint(args...))
}

func (l slogAsynqLogger) Debugf(format string, args ...interface{}) {
	l.logger.Debug(fmt.Sprintf(format, args...))
}

func (l slogAsynqLogger) Infof(format string, args ...interface{}) {
	l.logger.Info(fmt.Sprintf(format, args...))
}

func (l slogAsynqLogger) Warnf(format string, args ...interface{}) {
	l.logger.Warn(fmt.Sprintf(format, args...))
}

func (l slogAsynqLogger) Errorf(format string, args ...interface{}) {
	l.logger.Error(fmt.Sprintf(format, args...))
}

func (l slogAsynqLogger) Fatalf(format string, args ...interface{}) {
	l.logger.Error(fmt.Sprintf(format, args...))
}
