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
		return strings.HasPrefix(strings.ToLower(value), "v=spf1") &&
			strings.Contains(value, strings.TrimPrefix(spfValue, "v=spf1"))
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
