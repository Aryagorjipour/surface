package main

import (
	"os"
	"strings"
	"testing"
)

func TestComposeStack(t *testing.T) {
	raw, err := os.ReadFile("../../deployments/docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, want := range []string{
		"\n  cache:\n",
		"redis:7-alpine",
		"\"6379:6379\"",
		"redis-cli",
		"ping",
		"vault status",
		"http://127.0.0.1:8200",
		"vault kv put",
		"secret/surface/connector",
		"turso_url=file:/app/data/surface.db",
		"turso_token=dev",
		"encryption_key=ZGV2LXN1cmZhY2Uta2V5LWRldi1zdXJmYWNlLWtleSE=",
		"connector_token=dev",
		"redis_addr=cache:6379",
		"/app/connector --ready",
		"SURFACE_PUNCH_ADDR",
		":8081",
		"SURFACE_SOURCE_ALLOWANCE",
		"\"240\"",
		"deployments/Dockerfile.connector",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("compose missing %q", want)
		}
	}
	if strings.Contains(text, "redis:6379") {
		t.Fatal("compose sets redis_addr to redis:6379")
	}
}
