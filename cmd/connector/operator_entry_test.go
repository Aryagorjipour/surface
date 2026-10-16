package main

import (
	"os"
	"strings"
	"testing"
)

func TestOperatorEntry(t *testing.T) {
	justfile := readRepoFile(t, "../../justfile")
	readme := readRepoFile(t, "../../README.md")
	deploy := readRepoFile(t, "../../deployments/README.md")

	for _, want := range []string{
		"help:",
		"server-dev:",
		"server-stage:",
		"server-production:",
		"surface key",
		"go run ./cmd/surface",
		"http://127.0.0.1:8080",
		"127.0.0.1:9001",
		"docker compose up",
		"deployments/docker-compose.yml",
		"deployments/Dockerfile.connector",
		"VAULT_ADDR",
		"VAULT_TOKEN",
		"local stack only",
	} {
		if !strings.Contains(justfile, want) {
			t.Fatalf("justfile missing %q", want)
		}
	}
	for _, banned := range []string{
		"vault server -dev",
		"VAULT_DEV_ROOT_TOKEN",
		"connector_token=dev",
		"ZGV2LXN1cmZhY2Uta2V5LWRldi1zdXJmYWNlLWtleSE=",
		"go build",
	} {
		if strings.Contains(justfile, banned) {
			t.Fatalf("justfile contains %q", banned)
		}
	}
	if strings.Contains(helpBody(justfile), "SURFACE_CONNECTOR_TOKEN") {
		t.Fatal("help prints the client token setting")
	}

	if !strings.Contains(readme, "just help") {
		t.Fatal("readme does not point at just help")
	}
	for _, banned := range []string{
		"docker compose up",
		"go run ./cmd/surface",
		"SURFACE_CONNECTOR_TOKEN=dev surface",
	} {
		if strings.Contains(readme, banned) {
			t.Fatalf("readme gives a second run command %q", banned)
		}
	}

	for _, want := range []string{
		"server-dev",
		"server-stage",
		"server-production",
		"just surface",
		"VAULT_ADDR",
		"VAULT_TOKEN",
		"local stack only",
		"go run ./cmd/surface",
		"before a container starts",
	} {
		if !strings.Contains(deploy, want) {
			t.Fatalf("deployment doc missing %q", want)
		}
	}
	if strings.Contains(deploy, "ZGV2LXN1cmZhY2Uta2V5LWRldi1zdXJmYWNlLWtleSE=") {
		t.Fatal("deployment doc prints the encryption key")
	}
}

func helpBody(justfile string) string {
	start := strings.Index(justfile, "help:")
	if start < 0 {
		return ""
	}
	rest := justfile[start:]
	end := strings.Index(rest, "\nserver-dev:")
	if end < 0 {
		return rest
	}
	return rest[:end]
}

func readRepoFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
