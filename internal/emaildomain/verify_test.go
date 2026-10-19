package emaildomain

import (
	"context"
	"errors"
	"testing"
)

type staticResolver map[string][]string

func (r staticResolver) LookupTXT(_ context.Context, name string) ([]string, error) {
	records, ok := r[name]
	if !ok {
		return nil, errors.New("record not found")
	}
	return records, nil
}

func TestVerifyRecords(t *testing.T) {
	domain := "mail.example.org"
	selector := "notify-test"
	publicKey := "AQABabc123"
	resolver := staticResolver{
		domain:                             {"v=spf1 include:sendgrid.net ~all"},
		selector + "._domainkey." + domain: {"v=DKIM1; k=rsa; p=" + publicKey},
		"_dmarc." + domain:                 {"v=DMARC1; p=none"},
	}
	if err := VerifyRecords(context.Background(), resolver, domain, selector, publicKey, "v=spf1 include:sendgrid.net ~all"); err != nil {
		t.Fatalf("VerifyRecords() error = %v", err)
	}
	resolver[selector+"._domainkey."+domain] = []string{"v=DKIM1; p=wrong"}
	if err := VerifyRecords(context.Background(), resolver, domain, selector, publicKey, "v=spf1 include:sendgrid.net ~all"); err == nil {
		t.Fatal("VerifyRecords() accepted a different DKIM key")
	}
}

func TestSPFIncludesMechanismsAllowsMergedRecords(t *testing.T) {
	required := "v=spf1 include:sendgrid.net ~all"
	for published, want := range map[string]bool{
		"v=spf1 include:sendgrid.net ~all":                         true,
		"v=spf1 include:_spf.google.com include:sendgrid.net -all": true,
		"v=spf1 +include:sendgrid.net ?all":                        true,
		"v=spf1 include:_spf.google.com -all":                      false,
		"include:sendgrid.net ~all":                                false,
		"v=spf1 include:sendgrid.network ~all":                     false,
	} {
		if got := spfIncludesMechanisms(published, required); got != want {
			t.Errorf("spfIncludesMechanisms(%q) = %t, want %t", published, got, want)
		}
	}
}
