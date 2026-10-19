// Package apiscope defines the API key scopes shared by the API and the admin CLI.
package apiscope

const (
	NotificationsWrite = "notifications:write"
	MessagesRead       = "messages:read"
	PreferencesWrite   = "preferences:write"
	ConsentsRead       = "consents:read"
	ConsentsWrite      = "consents:write"
	WebhooksRead       = "webhooks:read"
	WebhooksWrite      = "webhooks:write"
	DomainsRead        = "domains:read"
	DomainsWrite       = "domains:write"
	SuppressionsRead   = "suppressions:read"
	SuppressionsWrite  = "suppressions:write"
	TemplatesRead      = "templates:read"
	TemplatesWrite     = "templates:write"
	OpsRead            = "ops:read"
	OpsWrite           = "ops:write"
	DataDelete         = "data:delete"
	// All grants every scope and cannot be combined with other scopes.
	All = "*"
)

// Valid lists every scope an API key may hold.
var Valid = []string{
	NotificationsWrite, MessagesRead, PreferencesWrite, ConsentsRead, ConsentsWrite,
	WebhooksRead, WebhooksWrite, DomainsRead, DomainsWrite, SuppressionsRead, SuppressionsWrite,
	TemplatesRead, TemplatesWrite,
	OpsRead, OpsWrite, DataDelete, All,
}
