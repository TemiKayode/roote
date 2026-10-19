package rendering

import (
	"strings"
	"testing"
)

func TestValidateNormalizesLocalesAndRejectsBadContent(t *testing.T) {
	defaultLocale, contents, err := Validate("EN-us", map[string]Content{
		"en-US": {EmailSubject: "Hi {{.name}}", EmailText: "Hello {{.name}}"},
		"fr":    {SMSText: "Bonjour {{.name}}"},
	})
	if err != nil || defaultLocale != "en-US" || len(contents) != 2 {
		t.Fatalf("Validate() = %q, %v, %v", defaultLocale, contents, err)
	}
	for name, invalid := range map[string]map[string]Content{
		"no content":        {"en": {}},
		"subject only":      {"en": {EmailSubject: "Hi"}},
		"bad syntax":        {"en": {SMSText: "{{.name"}},
		"bad locale":        {"not a locale!": {SMSText: "x"}},
		"default missing":   {"fr": {SMSText: "x"}},
		"duplicate locales": {"en-us": {SMSText: "x"}, "EN-US": {SMSText: "y"}},
	} {
		if _, _, err := Validate("en", invalid); err == nil {
			t.Errorf("%s: Validate() succeeded", name)
		}
	}
}

func TestResolveLocaleFallsBackToLanguageThenDefault(t *testing.T) {
	contents := map[string]Content{"en": {}, "fr": {}, "pt-BR": {}}
	for requested, want := range map[string]string{
		"": "en", "fr": "fr", "fr-CA": "fr", "pt-br": "pt-BR", "de": "en",
	} {
		got, err := ResolveLocale(requested, "en", contents)
		if err != nil || got != want {
			t.Errorf("ResolveLocale(%q) = %q, %v; want %q", requested, got, err, want)
		}
	}
}

func TestRenderEscapesHTMLAndRequiresVariables(t *testing.T) {
	content := Content{
		EmailSubject: "Order {{.order}}",
		EmailText:    "Hi {{.name}}",
		EmailHTML:    "<p>Hi {{.name}}</p>",
		SMSText:      "Order {{.order}} shipped",
	}
	data := map[string]any{"name": "<script>alert(1)</script>", "order": "A-1"}
	rendered, err := Render("en", content, []string{"email", "sms"}, data)
	if err != nil {
		t.Fatal(err)
	}
	if rendered.EmailSubject != "Order A-1" || rendered.SMSText != "Order A-1 shipped" {
		t.Fatalf("rendered = %+v", rendered)
	}
	if strings.Contains(rendered.EmailHTML, "<script>") || !strings.Contains(rendered.EmailHTML, "&lt;script&gt;") {
		t.Fatalf("HTML was not escaped: %s", rendered.EmailHTML)
	}
	if _, err := Render("en", content, []string{"email"}, map[string]any{"name": "x"}); err == nil {
		t.Fatal("missing variable rendered without error")
	}
	if _, err := Render("en", Content{SMSText: "x"}, []string{"email"}, nil); err == nil {
		t.Fatal("email rendered from a template without email content")
	}
	if _, err := Render("en", Content{EmailSubject: "{{.s}}", EmailText: "x"}, []string{"email"},
		map[string]any{"s": "line\r\nBcc: victim@example.com"}); err == nil {
		t.Fatal("subject with a line break rendered without error")
	}
}

func TestRenderStopsOversizedOutput(t *testing.T) {
	items := make([]int, 200_000)
	_, err := Render("en", Content{SMSText: "{{range .items}}xxxxxxxx{{end}}"}, []string{"sms"},
		map[string]any{"items": items})
	if err == nil {
		t.Fatal("oversized output rendered without error")
	}
}
