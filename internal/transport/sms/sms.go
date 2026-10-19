package sms

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf16"
)

var e164Pattern = regexp.MustCompile(`^\+[1-9][0-9]{7,14}$`)

type Message struct {
	ID          string
	To          string
	Body        string
	TrackStatus bool
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

func ValidateMessage(message Message) error {
	if strings.TrimSpace(message.ID) == "" {
		return errors.New("SMS message ID is required")
	}
	if !e164Pattern.MatchString(message.To) {
		return errors.New("SMS recipient must be an E.164 number")
	}
	if strings.TrimSpace(message.Body) == "" {
		return errors.New("SMS body must not be empty")
	}
	return nil
}

func SegmentCount(body string) int {
	gsmLength := 0
	gsm := true
	for _, r := range body {
		if isGSM7Basic(r) {
			gsmLength++
		} else if isGSM7Extended(r) {
			gsmLength += 2
		} else {
			gsm = false
			break
		}
	}
	if gsm {
		if gsmLength <= 160 {
			return 1
		}
		return (gsmLength + 152) / 153
	}

	utf16Length := len(utf16.Encode([]rune(body)))
	if utf16Length <= 70 {
		return 1
	}
	return (utf16Length + 66) / 67
}

func isGSM7Basic(r rune) bool {
	if r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
		return true
	}
	return strings.ContainsRune("@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./:;<=>?¡ÄÖÑÜ§¿äöñüà", r)
}

func isGSM7Extended(r rune) bool {
	return strings.ContainsRune("^{}\\[~]|€", r)
}

func ValidateTransportResult(providerMessageID string) error {
	if strings.TrimSpace(providerMessageID) == "" {
		return fmt.Errorf("SMS provider returned an empty message ID")
	}
	return nil
}
