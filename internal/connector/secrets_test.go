package connector

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestRequireString(t *testing.T) {
	data := map[string]interface{}{
		"ok":    "value",
		"empty": "",
	}

	if _, err := requireString(data, "missing"); err == nil {
		t.Error("expected error for missing key")
	}
	if _, err := requireString(data, "empty"); err == nil {
		t.Error("expected error for empty value")
	}
	v, err := requireString(data, "ok")
	if err != nil || v != "value" {
		t.Errorf("got %q, %v", v, err)
	}
}

func TestSecretsRejectMissingConnectorToken(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	const tursoToken = "turso-token-sentinel"
	base := map[string]any{
		"turso_url":      "libsql://example",
		"turso_token":    tursoToken,
		"encryption_key": key,
	}
	_, err := secretsFromMap(base)
	if err == nil || !strings.Contains(err.Error(), "connector_token") || strings.Contains(err.Error(), tursoToken) || strings.Contains(err.Error(), key) {
		t.Fatalf("missing token: %v", err)
	}
	empty := map[string]any{}
	for k, v := range base {
		empty[k] = v
	}
	empty["connector_token"] = ""
	_, err = secretsFromMap(empty)
	if err == nil || !strings.Contains(err.Error(), "connector_token") || strings.Contains(err.Error(), tursoToken) || strings.Contains(err.Error(), key) {
		t.Fatalf("empty token: %v", err)
	}
	full := map[string]any{}
	for k, v := range base {
		full[k] = v
	}
	full["connector_token"] = "deploy-credential"
	got, err := secretsFromMap(full)
	if err != nil {
		t.Fatal(err)
	}
	if got.ConnectorToken != "deploy-credential" || got.TursoURL == "" || got.TursoToken != tursoToken || len(got.EncryptionKey) != 32 {
		t.Fatalf("parsed %+v", got)
	}
}
