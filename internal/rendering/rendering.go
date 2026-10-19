// Package rendering validates and renders notification templates.
//
// Subjects, plain text, and SMS bodies use text/template; HTML uses html/template, which
// escapes template data for the HTML context. Every template runs with missingkey=error, so
// a notification that omits a variable fails when it is accepted rather than sending a gap.
package rendering

import (
	"bytes"
	"errors"
	"fmt"
	htmltemplate "html/template"
	"strings"
	texttemplate "text/template"

	"golang.org/x/text/language"
)

const (
	maxSubjectBytes = 255
	maxTextBytes    = 100_000
	maxHTMLBytes    = 500_000
	maxSMSBytes     = 1_600
	maxLocales      = 50
)

// Content is one locale's template source.
type Content struct {
	EmailSubject string `json:"email_subject,omitempty"`
	EmailText    string `json:"email_text,omitempty"`
	EmailHTML    string `json:"email_html,omitempty"`
	SMSText      string `json:"sms_text,omitempty"`
}

// Rendered is a template rendered for one locale.
type Rendered struct {
	Locale       string `json:"locale"`
	EmailSubject string `json:"email_subject,omitempty"`
	EmailText    string `json:"email_text,omitempty"`
	EmailHTML    string `json:"email_html,omitempty"`
	SMSText      string `json:"sms_text,omitempty"`
}

// NormalizeLocale returns the canonical BCP 47 form of a locale tag.
func NormalizeLocale(locale string) (string, error) {
	tag, err := language.Parse(strings.TrimSpace(locale))
	if err != nil || tag == language.Und {
		return "", fmt.Errorf("locale %q is not a valid BCP 47 language tag", locale)
	}
	return tag.String(), nil
}

// Validate checks a template version's contents and returns them keyed by canonical locale.
func Validate(defaultLocale string, contents map[string]Content) (string, map[string]Content, error) {
	if len(contents) == 0 || len(contents) > maxLocales {
		return "", nil, fmt.Errorf("contents must define between 1 and %d locales", maxLocales)
	}
	normalized := make(map[string]Content, len(contents))
	for locale, content := range contents {
		canonical, err := NormalizeLocale(locale)
		if err != nil {
			return "", nil, err
		}
		if _, duplicate := normalized[canonical]; duplicate {
			return "", nil, fmt.Errorf("locale %s is defined more than once", canonical)
		}
		if err := validateContent(content); err != nil {
			return "", nil, fmt.Errorf("locale %s: %w", canonical, err)
		}
		normalized[canonical] = content
	}
	canonicalDefault, err := NormalizeLocale(defaultLocale)
	if err != nil {
		return "", nil, fmt.Errorf("default_locale: %w", err)
	}
	if _, ok := normalized[canonicalDefault]; !ok {
		return "", nil, fmt.Errorf("default_locale %s has no contents", canonicalDefault)
	}
	return canonicalDefault, normalized, nil
}

func validateContent(content Content) error {
	hasEmail := content.EmailSubject != "" || content.EmailText != "" || content.EmailHTML != ""
	if !hasEmail && content.SMSText == "" {
		return errors.New("define email content, SMS content, or both")
	}
	if hasEmail && (strings.TrimSpace(content.EmailSubject) == "" || strings.TrimSpace(content.EmailText) == "") {
		return errors.New("email content needs email_subject and email_text; email_html is optional")
	}
	if len(content.EmailSubject) > maxSubjectBytes*4 || len(content.EmailText) > maxTextBytes ||
		len(content.EmailHTML) > maxHTMLBytes || len(content.SMSText) > maxSMSBytes*4 {
		return errors.New("template source exceeds the size limit")
	}
	for name, source := range map[string]string{
		"email_subject": content.EmailSubject, "email_text": content.EmailText, "sms_text": content.SMSText,
	} {
		if _, err := parseText(name, source); err != nil {
			return err
		}
	}
	if _, err := parseHTML(content.EmailHTML); err != nil {
		return err
	}
	return nil
}

// ResolveLocale picks the content for requested: an exact match, then the bare language
// ("fr-CA" falls back to "fr"), then the default locale.
func ResolveLocale(requested, defaultLocale string, contents map[string]Content) (string, error) {
	if strings.TrimSpace(requested) == "" {
		return defaultLocale, nil
	}
	tag, err := language.Parse(requested)
	if err != nil {
		return "", fmt.Errorf("locale %q is not a valid BCP 47 language tag", requested)
	}
	if _, ok := contents[tag.String()]; ok {
		return tag.String(), nil
	}
	base, _ := tag.Base()
	if _, ok := contents[base.String()]; ok {
		return base.String(), nil
	}
	return defaultLocale, nil
}

// Render renders the content for locale with data. channels limits rendering to the
// requested channels, and each requested channel must have content in that locale.
func Render(locale string, content Content, channels []string, data map[string]any) (Rendered, error) {
	rendered := Rendered{Locale: locale}
	for _, channel := range channels {
		switch channel {
		case "email":
			if content.EmailSubject == "" {
				return Rendered{}, fmt.Errorf("template has no email content for locale %s", locale)
			}
			var err error
			if rendered.EmailSubject, err = execText("email_subject", content.EmailSubject, data); err != nil {
				return Rendered{}, err
			}
			rendered.EmailSubject = strings.TrimSpace(rendered.EmailSubject)
			if rendered.EmailSubject == "" || strings.ContainsAny(rendered.EmailSubject, "\r\n") ||
				len(rendered.EmailSubject) > maxSubjectBytes {
				return Rendered{}, errors.New("rendered email subject must be one line of 1 to 255 bytes")
			}
			if rendered.EmailText, err = execText("email_text", content.EmailText, data); err != nil {
				return Rendered{}, err
			}
			if strings.TrimSpace(rendered.EmailText) == "" || len(rendered.EmailText) > maxTextBytes {
				return Rendered{}, fmt.Errorf("rendered email text must be 1 to %d bytes", maxTextBytes)
			}
			if content.EmailHTML != "" {
				if rendered.EmailHTML, err = execHTML(content.EmailHTML, data); err != nil {
					return Rendered{}, err
				}
				if len(rendered.EmailHTML) > maxHTMLBytes {
					return Rendered{}, fmt.Errorf("rendered email HTML exceeds %d bytes", maxHTMLBytes)
				}
			}
		case "sms":
			if content.SMSText == "" {
				return Rendered{}, fmt.Errorf("template has no SMS content for locale %s", locale)
			}
			text, err := execText("sms_text", content.SMSText, data)
			if err != nil {
				return Rendered{}, err
			}
			rendered.SMSText = strings.TrimSpace(text)
			if rendered.SMSText == "" || len(rendered.SMSText) > maxSMSBytes {
				return Rendered{}, fmt.Errorf("rendered SMS text must be 1 to %d bytes", maxSMSBytes)
			}
		default:
			return Rendered{}, fmt.Errorf("unsupported channel %q", channel)
		}
	}
	return rendered, nil
}

func parseText(name, source string) (*texttemplate.Template, error) {
	parsed, err := texttemplate.New(name).Option("missingkey=error").Parse(source)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return parsed, nil
}

func parseHTML(source string) (*htmltemplate.Template, error) {
	parsed, err := htmltemplate.New("email_html").Option("missingkey=error").Parse(source)
	if err != nil {
		return nil, fmt.Errorf("email_html: %w", err)
	}
	return parsed, nil
}

func execText(name, source string, data map[string]any) (string, error) {
	parsed, err := parseText(name, source)
	if err != nil {
		return "", err
	}
	var output bytes.Buffer
	if err := parsed.Execute(&limitedWriter{buffer: &output, limit: maxHTMLBytes}, data); err != nil {
		return "", fmt.Errorf("render %s: %w", name, err)
	}
	return output.String(), nil
}

func execHTML(source string, data map[string]any) (string, error) {
	parsed, err := parseHTML(source)
	if err != nil {
		return "", err
	}
	var output bytes.Buffer
	if err := parsed.Execute(&limitedWriter{buffer: &output, limit: maxHTMLBytes}, data); err != nil {
		return "", fmt.Errorf("render email_html: %w", err)
	}
	return output.String(), nil
}

// limitedWriter stops a template whose output grows past limit, for example a range over a
// large data array, instead of buffering it whole.
type limitedWriter struct {
	buffer *bytes.Buffer
	limit  int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if w.buffer.Len()+len(p) > w.limit {
		return 0, errors.New("rendered output exceeds the size limit")
	}
	return w.buffer.Write(p)
}
