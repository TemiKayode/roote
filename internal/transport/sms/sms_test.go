package sms

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestSegmentCountAccountsForGSMAndUnicodeEncoding(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "single GSM", body: strings.Repeat("a", 160), want: 1},
		{name: "concatenated GSM", body: strings.Repeat("a", 161), want: 2},
		{name: "extended GSM characters", body: strings.Repeat("^", 81), want: 2},
		{name: "single UCS-2", body: strings.Repeat("界", 70), want: 1},
		{name: "concatenated UCS-2", body: strings.Repeat("😀", 36), want: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := SegmentCount(test.body); got != test.want {
				t.Fatalf("SegmentCount() = %d, want %d", got, test.want)
			}
		})
	}
}

func TestTwilioTransportSendsFormAndReturnsSID(t *testing.T) {
	transport, err := NewTwilioTransport("AC123", "secret", "+12025550123", "https://hooks.example.org/sms")
	if err != nil {
		t.Fatal(err)
	}
	transport.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Host != "api.twilio.com" {
			t.Fatalf("unexpected Twilio request: %s %s", request.Method, request.URL)
		}
		username, password, ok := request.BasicAuth()
		if !ok || username != "AC123" || password != "secret" {
			t.Fatal("Twilio request is missing Basic authentication")
		}
		if err := request.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if request.Form.Get("To") != "+12025550124" || request.Form.Get("Body") != "Hello" ||
			request.Form.Get("StatusCallback") != "https://hooks.example.org/sms?message_id=message-1" {
			t.Fatalf("unexpected Twilio form: %v", request.Form)
		}
		return response(http.StatusCreated, `{"sid":"SM123"}`), nil
	})}
	id, err := transport.Send(context.Background(), Message{
		ID: "message-1", To: "+12025550124", Body: "Hello", TrackStatus: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if id != "SM123" {
		t.Fatalf("provider ID = %q, want SM123", id)
	}
}

func TestTelnyxTransportUsesBearerTokenAndReturnsID(t *testing.T) {
	transport, err := NewTelnyxTransport("key", "+12025550123", "")
	if err != nil {
		t.Fatal(err)
	}
	transport.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Authorization") != "Bearer key" {
			t.Fatal("Telnyx request is missing bearer authentication")
		}
		var body struct {
			To   string `json:"to"`
			Text string `json:"text"`
		}
		if err := jsonNewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.To != "+12025550124" || body.Text != "Hello" {
			t.Fatalf("unexpected Telnyx body: %+v", body)
		}
		return response(http.StatusAccepted, `{"data":{"id":"telnyx-id"}}`), nil
	})}
	id, err := transport.Send(context.Background(), Message{ID: "message-1", To: "+12025550124", Body: "Hello"})
	if err != nil {
		t.Fatal(err)
	}
	if id != "telnyx-id" {
		t.Fatalf("provider ID = %q, want telnyx-id", id)
	}
}

func TestTransportClassifiesProviderErrors(t *testing.T) {
	transport, err := NewTwilioTransport("AC123", "secret", "+12025550123", "")
	if err != nil {
		t.Fatal(err)
	}
	transport.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusTooManyRequests, ""), nil
	})}
	_, err = transport.Send(context.Background(), Message{ID: "message-1", To: "+12025550124", Body: "Hello"})
	deliveryError, ok := err.(*DeliveryError)
	if !ok || !deliveryError.Temporary {
		t.Fatalf("error = %#v, want temporary delivery error", err)
	}

	transport.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusBadRequest, ""), nil
	})}
	_, err = transport.Send(context.Background(), Message{ID: "message-1", To: "+12025550124", Body: "Hello"})
	deliveryError, ok = err.(*DeliveryError)
	if !ok || deliveryError.Temporary {
		t.Fatalf("error = %#v, want permanent delivery error", err)
	}
}

func response(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func jsonNewDecoder(body io.Reader) interface{ Decode(any) error } {
	return json.NewDecoder(body)
}

func TestTwilioCallbackURLPreservesExistingQuery(t *testing.T) {
	transport, err := NewTwilioTransport("AC123", "secret", "+12025550123", "https://hooks.example.org/sms?source=twilio")
	if err != nil {
		t.Fatal(err)
	}
	transport.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if err := request.ParseForm(); err != nil {
			t.Fatal(err)
		}
		callback, err := url.Parse(request.Form.Get("StatusCallback"))
		if err != nil {
			t.Fatal(err)
		}
		if callback.Query().Get("source") != "twilio" || callback.Query().Get("message_id") != "m-1" {
			t.Fatalf("unexpected callback URL: %s", callback)
		}
		return response(http.StatusCreated, `{"sid":"SM123"}`), nil
	})}
	if _, err := transport.Send(context.Background(), Message{
		ID: "m-1", To: "+12025550124", Body: "Hello", TrackStatus: true,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestProviderStatusLookups(t *testing.T) {
	twilio, err := NewTwilioTransport("AC123", "token", "+15005550006", "")
	if err != nil {
		t.Fatal(err)
	}
	twilio.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet ||
			request.URL.String() != "https://api.twilio.com/2010-04-01/Accounts/AC123/Messages/SM1.json" {
			t.Errorf("Twilio status request = %s %s", request.Method, request.URL)
		}
		if user, password, ok := request.BasicAuth(); !ok || user != "AC123" || password != "token" {
			t.Error("Twilio status request is not authenticated")
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"status":"undelivered"}`))}, nil
	})}
	status, ok, err := twilio.Status(context.Background(), "SM1")
	if err != nil || !ok || status != "undelivered" {
		t.Fatalf("Twilio status = %q, %t, %v", status, ok, err)
	}

	telnyx, err := NewTelnyxTransport("key", "+15005550006", "")
	if err != nil {
		t.Fatal(err)
	}
	telnyx.client = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.String() != "https://api.telnyx.com/v2/messages/msg-1" ||
			request.Header.Get("Authorization") != "Bearer key" {
			t.Errorf("Telnyx status request = %s %s", request.URL, request.Header.Get("Authorization"))
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(
			`{"data":{"to":[{"status":"delivered"}]}}`))}, nil
	})}
	status, ok, err = telnyx.Status(context.Background(), "msg-1")
	if err != nil || !ok || status != "delivered" {
		t.Fatalf("Telnyx status = %q, %t, %v", status, ok, err)
	}
	telnyx.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	if _, _, err := telnyx.Status(context.Background(), "missing"); err == nil {
		t.Fatal("Telnyx status for a 404 response returned no error")
	}
}
