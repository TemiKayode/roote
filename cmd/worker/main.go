package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"github.com/TemiKayode/roote/internal/config"
	"github.com/TemiKayode/roote/internal/delivery"
	"github.com/TemiKayode/roote/internal/queue"
	"github.com/TemiKayode/roote/internal/store"
	"github.com/TemiKayode/roote/internal/transport/email"
	"github.com/TemiKayode/roote/internal/transport/sms"
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

	redis := asynq.RedisClientOpt{Addr: cfg.RedisURL}
	client := asynq.NewClient(redis)
	defer client.Close()
	server := asynq.NewServer(redis, asynq.Config{
		Queues: map[string]int{cfg.AsynqQueue: 1},
		Logger: slogAsynqLogger{logger: logger},
	})
	if err := server.Start(queue.NewServeMux(
		logger, processor.DeliverNotification, processor.VerifyDomain, processor.SendSMSStopConfirmation,
	)); err != nil {
		return fmt.Errorf("start Asynq server: %w", err)
	}
	go database.RunOutboxRelay(ctx, func(ctx context.Context, messageID uuid.UUID) error {
		return queue.EnqueueNotification(ctx, client, cfg.AsynqQueue, messageID)
	}, logger)

	logger.Info("worker started", "queue", cfg.AsynqQueue)
	<-ctx.Done()
	server.Shutdown()
	logger.Info("worker stopped")
	return nil
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
