package email

import (
	"context"
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	stdmail "net/mail"
	"net/url"
	"strings"

	"github.com/emersion/go-msgauth/dkim"
	gomail "github.com/wneessen/go-mail"
)

type Message struct {
	ID                string
	From              string
	To                string
	Subject           string
	Body              string
	HTML              string
	TrafficClass      string
	UnsubscribeURL    string
	DKIMDomain        string
	DKIMSelector      string
	DKIMPrivateKeyPEM []byte
}

type Transport interface {
	Send(context.Context, Message) (string, error)
	Provider() string
}

type DeliveryError struct {
	Err       error
	Temporary bool
}

func (e *DeliveryError) Error() string {
	return e.Err.Error()
}

func (e *DeliveryError) Unwrap() error {
	return e.Err
}

func NewMessage(
	id, from, to, subject, body, trafficClass, unsubscribeURL, selector string,
	privateKeyPEM []byte,
) (Message, error) {
	fromAddress, err := stdmail.ParseAddress(from)
	if err != nil {
		return Message{}, fmt.Errorf("parse sender address: %w", err)
	}
	toAddress, err := stdmail.ParseAddress(to)
	if err != nil {
		return Message{}, fmt.Errorf("parse recipient address: %w", err)
	}
	if strings.ContainsAny(subject, "\r\n") {
		return Message{}, errors.New("email subject must not contain line breaks")
	}
	parts := strings.Split(fromAddress.Address, "@")
	if len(parts) != 2 {
		return Message{}, errors.New("sender address must have a domain")
	}
	domain := strings.ToLower(parts[1])
	if domain == "" || selector == "" || len(privateKeyPEM) == 0 {
		return Message{}, errors.New("verified sender domain and DKIM key are required")
	}
	if trafficClass != "" && trafficClass != "transactional" && trafficClass != "marketing" {
		return Message{}, errors.New("traffic class must be transactional or marketing")
	}
	if trafficClass == "marketing" && unsubscribeURL == "" {
		return Message{}, errors.New("marketing email requires an unsubscribe URL")
	}
	if trafficClass == "marketing" {
		parsedURL, err := url.ParseRequestURI(unsubscribeURL)
		if err != nil || parsedURL.Scheme != "https" || parsedURL.Host == "" || parsedURL.User != nil {
			return Message{}, errors.New("marketing unsubscribe URL must be an absolute HTTPS URL")
		}
	}
	return Message{
		ID:                id,
		From:              fromAddress.Address,
		To:                toAddress.Address,
		Subject:           subject,
		Body:              body,
		TrafficClass:      trafficClass,
		UnsubscribeURL:    unsubscribeURL,
		DKIMDomain:        domain,
		DKIMSelector:      selector,
		DKIMPrivateKeyPEM: privateKeyPEM,
	}, nil
}

func composeAndSign(message Message) ([]byte, error) {
	privateKeyBlock, _ := pem.Decode(message.DKIMPrivateKeyPEM)
	if privateKeyBlock == nil {
		return nil, errors.New("DKIM private key is not PEM encoded")
	}
	privateKey, err := x509.ParsePKCS8PrivateKey(privateKeyBlock.Bytes)
	if err != nil {
		if rsaKey, rsaErr := x509.ParsePKCS1PrivateKey(privateKeyBlock.Bytes); rsaErr == nil {
			privateKey = rsaKey
		} else {
			return nil, fmt.Errorf("parse DKIM private key: %w", err)
		}
	}
	signer, ok := privateKey.(crypto.Signer)
	if !ok {
		return nil, errors.New("DKIM private key does not support signing")
	}

	msg := gomail.NewMsg()
	if err := msg.From(message.From); err != nil {
		return nil, fmt.Errorf("set email sender: %w", err)
	}
	if err := msg.To(message.To); err != nil {
		return nil, fmt.Errorf("set email recipient: %w", err)
	}
	msg.Subject(message.Subject)
	msg.SetGenHeader(gomail.Header("X-Notification-Message-ID"), message.ID)
	if message.TrafficClass != "" {
		msg.SetGenHeader(gomail.Header("X-Notification-Traffic-Class"), message.TrafficClass)
	}
	msg.SetBodyString(gomail.TypeTextPlain, message.Body)
	if message.HTML != "" {
		msg.AddAlternativeString(gomail.TypeTextHTML, message.HTML)
	}
	msg.SetMessageID()
	headerKeys := []string{"From", "To", "Subject", "Date", "Message-ID", "MIME-Version", "Content-Type", "X-Notification-Message-ID"}
	if message.TrafficClass == "marketing" {
		msg.SetGenHeader(gomail.HeaderListUnsubscribe, "<"+message.UnsubscribeURL+">")
		msg.SetGenHeader(gomail.HeaderListUnsubscribePost, "List-Unsubscribe=One-Click")
		headerKeys = append(headerKeys, "List-Unsubscribe", "List-Unsubscribe-Post")
	}
	var raw strings.Builder
	if _, err := msg.WriteTo(&raw); err != nil {
		return nil, fmt.Errorf("compose email message: %w", err)
	}
	var signed strings.Builder
	if err := dkim.Sign(&signed, strings.NewReader(raw.String()), &dkim.SignOptions{
		Domain:                 message.DKIMDomain,
		Selector:               message.DKIMSelector,
		Signer:                 signer,
		HeaderCanonicalization: dkim.CanonicalizationRelaxed,
		BodyCanonicalization:   dkim.CanonicalizationRelaxed,
		HeaderKeys:             headerKeys,
	}); err != nil {
		return nil, fmt.Errorf("sign email with DKIM: %w", err)
	}
	return []byte(signed.String()), nil
}

func ValidateMessage(message Message) error {
	if _, err := stdmail.ParseAddress(message.From); err != nil {
		return fmt.Errorf("invalid sender: %w", err)
	}
	if _, err := stdmail.ParseAddress(message.To); err != nil {
		return fmt.Errorf("invalid recipient: %w", err)
	}
	if message.DKIMDomain == "" {
		return errors.New("DKIM signing domain is required")
	}
	return nil
}
