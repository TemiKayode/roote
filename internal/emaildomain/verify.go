package emaildomain

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type TXTResolver interface {
	LookupTXT(context.Context, string) ([]string, error)
}

func VerifyRecords(ctx context.Context, resolver TXTResolver, domain, selector, publicKey, spfValue string) error {
	spfRecords, err := resolver.LookupTXT(ctx, domain)
	if err != nil {
		return fmt.Errorf("lookup SPF record for %s: %w", domain, err)
	}
	if !containsTXT(spfRecords, func(value string) bool {
		return spfIncludesMechanisms(value, spfValue)
	}) {
		return errors.New("SPF TXT record does not match the required value")
	}

	dkimRecords, err := resolver.LookupTXT(ctx, selector+"._domainkey."+domain)
	if err != nil {
		return fmt.Errorf("lookup DKIM record for %s: %w", domain, err)
	}
	if !containsTXT(dkimRecords, func(value string) bool {
		value = strings.Join(strings.Fields(value), "")
		return strings.Contains(value, "v=DKIM1") &&
			strings.Contains(value, "p="+publicKey)
	}) {
		return errors.New("DKIM TXT record does not contain the generated public key")
	}

	dmarcRecords, err := resolver.LookupTXT(ctx, "_dmarc."+domain)
	if err != nil {
		return fmt.Errorf("lookup DMARC record for %s: %w", domain, err)
	}
	if !containsTXT(dmarcRecords, func(value string) bool {
		return strings.HasPrefix(strings.ToLower(strings.TrimSpace(value)), "v=dmarc1")
	}) {
		return errors.New("DMARC TXT record is missing")
	}
	return nil
}

func containsTXT(records []string, matches func(string) bool) bool {
	for _, record := range records {
		if matches(record) {
			return true
		}
	}
	return false
}

// spfIncludesMechanisms reports whether the published SPF record contains every mechanism of
// the required value. The terminal "all" qualifier is ignored so the required mechanisms can
// be merged into an existing record that uses its own -all or ~all policy.
func spfIncludesMechanisms(published, required string) bool {
	publishedTerms := strings.Fields(strings.ToLower(published))
	if len(publishedTerms) == 0 || publishedTerms[0] != "v=spf1" {
		return false
	}
	present := make(map[string]struct{}, len(publishedTerms))
	for _, term := range publishedTerms[1:] {
		present[strings.TrimLeft(term, "+")] = struct{}{}
	}
	for _, term := range strings.Fields(strings.ToLower(required))[1:] {
		term = strings.TrimLeft(term, "+")
		if strings.TrimLeft(term, "-~?") == "all" {
			continue
		}
		if _, ok := present[term]; !ok {
			return false
		}
	}
	return true
}
