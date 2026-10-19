package main

import (
	"bytes"
	"testing"

	"github.com/TemiKayode/roote/internal/secret"
)

func TestResealWithSkipsCurrentAndUpgradesPrevious(t *testing.T) {
	oldKey, newKey := bytes.Repeat([]byte{5}, 32), bytes.Repeat([]byte{6}, 32)
	keyring, err := secret.NewKeyring(newKey, oldKey)
	if err != nil {
		t.Fatal(err)
	}
	reseal := resealWith(keyring)
	underOld, _ := secret.Encrypt(oldKey, []byte("dkim key"))
	resealed, changed, err := reseal(underOld)
	if err != nil || !changed || !keyring.IsCurrent(resealed) {
		t.Fatalf("reseal(previous) changed=%t current=%t error=%v", changed, keyring.IsCurrent(resealed), err)
	}
	if _, changed, err := reseal(resealed); err != nil || changed {
		t.Fatalf("reseal(current) changed=%t error=%v", changed, err)
	}
	unknown, _ := secret.Encrypt(bytes.Repeat([]byte{7}, 32), []byte("x"))
	if _, _, err := reseal(unknown); err == nil {
		t.Fatal("reseal accepted a ciphertext no key opens")
	}
}
