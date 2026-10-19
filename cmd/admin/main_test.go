package main

import (
	"reflect"
	"testing"
)

func TestParseScopes(t *testing.T) {
	scopes, err := parseScopes("notifications:write, ops:read")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(scopes, []string{"notifications:write", "ops:read"}) {
		t.Fatalf("scopes = %v", scopes)
	}
	for _, value := range []string{"unknown", "ops:read,ops:read", "*,ops:read", ""} {
		if _, err := parseScopes(value); err == nil {
			t.Errorf("parseScopes(%q) succeeded", value)
		}
	}
}
