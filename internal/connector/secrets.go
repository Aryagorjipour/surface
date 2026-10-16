package connector

import (
	"context"
	"encoding/base64"
	"fmt"

	vault "github.com/hashicorp/vault/api"
)

type Secrets struct {
	TursoURL       string
	TursoToken     string
	EncryptionKey  []byte
	RedisAddr      string
	RedisPassword  string
	ConnectorToken string
}

// Load reads secret/data/surface/connector. A non-empty addr overrides the Vault client's address.
func Load(ctx context.Context, addr string) (*Secrets, error) {
	cfg := vault.DefaultConfig()
	if addr != "" {
		cfg.Address = addr
	}
	client, err := vault.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("vault client: %w", err)
	}

	secret, err := client.Logical().Read("secret/data/surface/connector")
	if err != nil {
		return nil, fmt.Errorf("vault read: %w", err)
	}
	if secret == nil || secret.Data == nil {
		return nil, fmt.Errorf("vault: empty response")
	}

	data, ok := secret.Data["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("vault: invalid data format")
	}
	return secretsFromMap(data)
}

// secretsFromMap parses one Vault data map. It does not open a connection.
func secretsFromMap(data map[string]any) (*Secrets, error) {
	s := &Secrets{}
	var err error
	s.TursoURL, err = requireString(data, "turso_url")
	if err != nil {
		return nil, err
	}
	s.TursoToken, err = requireString(data, "turso_token")
	if err != nil {
		return nil, err
	}

	keyB64, err := requireString(data, "encryption_key")
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(keyB64)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("encryption_key must be base64-encoded 32 bytes")
	}
	s.EncryptionKey = key

	s.ConnectorToken, err = requireString(data, "connector_token")
	if err != nil {
		return nil, err
	}

	if v, ok := data["redis_addr"].(string); ok {
		s.RedisAddr = v
	}
	if v, ok := data["redis_password"].(string); ok {
		s.RedisPassword = v
	}
	return s, nil
}

func requireString(data map[string]interface{}, key string) (string, error) {
	v, ok := data[key].(string)
	if !ok || v == "" {
		return "", fmt.Errorf("missing or empty secret: %s", key)
	}
	return v, nil
}
