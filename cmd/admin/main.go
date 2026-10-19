package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/TemiKayode/roote/internal/apiscope"
	"github.com/TemiKayode/roote/internal/config"
	"github.com/TemiKayode/roote/internal/store"
)

var validScopes = func() map[string]struct{} {
	scopes := make(map[string]struct{}, len(apiscope.Valid))
	for _, scope := range apiscope.Valid {
		scopes[scope] = struct{}{}
	}
	return scopes
}()

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: admin create-key|revoke-key [flags]")
	}
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	accountID, err := uuid.Parse(cfg.AccountID)
	if err != nil {
		return fmt.Errorf("ACCOUNT_ID must be a valid UUID: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database, err := store.Open(ctx, cfg.DatabaseURL, accountID.String())
	if err != nil {
		return fmt.Errorf("open application store: %w", err)
	}
	defer database.Close()

	switch args[0] {
	case "create-key":
		return createKey(ctx, database, accountID, args[1:])
	case "revoke-key":
		return revokeKey(ctx, database, accountID, args[1:])
	default:
		return fmt.Errorf("unknown command %q; use create-key or revoke-key", args[0])
	}
}

func createKey(ctx context.Context, database *store.Store, accountID uuid.UUID, args []string) error {
	flags := flag.NewFlagSet("create-key", flag.ContinueOnError)
	name := flags.String("name", "", "human-readable key name")
	scopeList := flags.String("scopes", "notifications:write", "comma-separated API key scopes")
	expiration := flags.Duration("expires-in", 0, "optional key lifetime (for example 720h)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || strings.TrimSpace(*name) == "" || len(*name) > 100 {
		return errors.New("create-key requires --name with 1 to 100 characters and accepts no positional arguments")
	}
	scopes, err := parseScopes(*scopeList)
	if err != nil {
		return err
	}
	var expiresAt *time.Time
	if *expiration < 0 || *expiration > 87600*time.Hour {
		return errors.New("--expires-in must be positive and no longer than 10 years")
	}
	if *expiration > 0 {
		expiry := time.Now().UTC().Add(*expiration)
		expiresAt = &expiry
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return fmt.Errorf("generate API key: %w", err)
	}
	token := "rt_live_" + base64.RawURLEncoding.EncodeToString(secret)
	hash := sha256.Sum256([]byte(token))
	id, err := database.CreateAPIKey(ctx, accountID, strings.TrimSpace(*name), hash[:], scopes, expiresAt)
	if err != nil {
		return err
	}
	fmt.Printf("API key ID: %s\nAPI key (shown once): %s\nScopes: %s\n",
		id, token, strings.Join(scopes, ","))
	return nil
}

func revokeKey(ctx context.Context, database *store.Store, accountID uuid.UUID, args []string) error {
	flags := flag.NewFlagSet("revoke-key", flag.ContinueOnError)
	idValue := flags.String("id", "", "API key UUID")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("revoke-key accepts no positional arguments")
	}
	id, err := uuid.Parse(strings.TrimSpace(*idValue))
	if err != nil {
		return fmt.Errorf("--id must be a valid API key UUID: %w", err)
	}
	if err := database.RevokeAPIKey(ctx, accountID, id); err != nil {
		return fmt.Errorf("revoke API key: %w", err)
	}
	fmt.Printf("Revoked API key %s\n", id)
	return nil
}

func parseScopes(value string) ([]string, error) {
	parts := strings.Split(value, ",")
	scopes := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		scope := strings.TrimSpace(part)
		if _, ok := validScopes[scope]; !ok {
			return nil, fmt.Errorf("unsupported API key scope %q", scope)
		}
		if _, duplicate := seen[scope]; duplicate {
			return nil, fmt.Errorf("duplicate API key scope %q", scope)
		}
		seen[scope] = struct{}{}
		scopes = append(scopes, scope)
	}
	if len(scopes) > 1 {
		if _, wildcard := seen[apiscope.All]; wildcard {
			return nil, errors.New("wildcard scope cannot be combined with other scopes")
		}
	}
	return scopes, nil
}
