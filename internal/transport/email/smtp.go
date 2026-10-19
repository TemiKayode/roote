package email

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/smtp"
	"net/textproto"
	"strconv"
	"strings"
	"time"
)

type SMTPTransport struct {
	host       string
	port       int
	username   string
	password   string
	requireTLS bool
	timeout    time.Duration
}

func NewSMTPTransport(host string, port int, username, password string, requireTLS bool) (*SMTPTransport, error) {
	if strings.TrimSpace(host) == "" || port < 1 || port > 65535 {
		return nil, errors.New("SMTP host and a valid port are required")
	}
	if (username == "") != (password == "") {
		return nil, errors.New("SMTP username and password must both be set or both be empty")
	}
	return &SMTPTransport{
		host:       host,
		port:       port,
		username:   username,
		password:   password,
		requireTLS: requireTLS,
		timeout:    15 * time.Second,
	}, nil
}

func (t *SMTPTransport) Provider() string {
	return "kumo-smtp"
}

func (t *SMTPTransport) Send(ctx context.Context, message Message) (string, error) {
	rawMessage, err := composeAndSign(message)
	if err != nil {
		return "", &DeliveryError{Err: err}
	}
	address := net.JoinHostPort(t.host, strconv.Itoa(t.port))
	dialer := net.Dialer{Timeout: t.timeout}
	connection, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return "", &DeliveryError{Err: fmt.Errorf("connect to SMTP server: %w", err), Temporary: true}
	}
	defer connection.Close()
	stopOnCancel := context.AfterFunc(ctx, func() {
		_ = connection.Close()
	})
	defer stopOnCancel()
	deadline := time.Now().Add(t.timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return "", &DeliveryError{Err: fmt.Errorf("set SMTP deadline: %w", err), Temporary: true}
	}

	client, err := smtp.NewClient(connection, t.host)
	if err != nil {
		return "", &DeliveryError{Err: fmt.Errorf("start SMTP session: %w", err), Temporary: true}
	}
	defer client.Close()
	if t.requireTLS {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return "", &DeliveryError{Err: errors.New("SMTP server does not offer required STARTTLS")}
		}
		if err := client.StartTLS(&tls.Config{ServerName: t.host, MinVersion: tls.VersionTLS12}); err != nil {
			return "", &DeliveryError{Err: fmt.Errorf("start SMTP TLS: %w", err), Temporary: true}
		}
	}
	if t.username != "" {
		if !clientSupportsAuth(client) {
			return "", &DeliveryError{Err: errors.New("SMTP server does not advertise AUTH")}
		}
		auth := smtp.PlainAuth("", t.username, t.password, t.host)
		if err := client.Auth(auth); err != nil {
			return "", &DeliveryError{Err: fmt.Errorf("authenticate to SMTP server: %w", err)}
		}
	}
	if err := client.Mail(message.From); err != nil {
		return "", classifySMTPError("set SMTP sender", err)
	}
	if err := client.Rcpt(message.To); err != nil {
		return "", classifySMTPError("set SMTP recipient", err)
	}
	writer, err := client.Data()
	if err != nil {
		return "", classifySMTPError("start SMTP message data", err)
	}
	if _, err := writer.Write(rawMessage); err != nil {
		_ = writer.Close()
		return "", &DeliveryError{Err: fmt.Errorf("write SMTP message: %w", err), Temporary: true}
	}
	if err := writer.Close(); err != nil {
		return "", classifySMTPError("finish SMTP message data", err)
	}
	if err := client.Quit(); err != nil {
		return "", classifySMTPError("close SMTP session", err)
	}
	return "", nil
}

func clientSupportsAuth(client *smtp.Client) bool {
	ok, mechanisms := client.Extension("AUTH")
	return ok && len(strings.Fields(mechanisms)) > 0
}

func classifySMTPError(operation string, err error) error {
	var smtpError *textproto.Error
	temporary := errors.As(err, &smtpError) && smtpError.Code >= 400 && smtpError.Code < 500
	if smtpError == nil {
		temporary = true
	}
	return &DeliveryError{Err: fmt.Errorf("%s: %w", operation, err), Temporary: temporary}
}
