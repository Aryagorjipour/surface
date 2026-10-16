package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	client *redis.Client
}

func NewRedis(addr, password string) *RedisCache {
	if addr == "" {
		return nil
	}
	return &RedisCache{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       0,
		}),
	}
}

func (r *RedisCache) Ping(ctx context.Context) error {
	if r == nil || r.client == nil {
		return fmt.Errorf("redis disabled")
	}
	return r.client.Ping(ctx).Err()
}

func (r *RedisCache) Close() error {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.Close()
}

// cachedPeer is the Redis value for a peer. last_seen stays out of the HTTP body.
type cachedPeer struct {
	Key      string `json:"key"`
	Address  string `json:"address"`
	State    string `json:"state"`
	LastSeen int64  `json:"last_seen"`
}

// Peer cache
func (r *RedisCache) SetPeer(ctx context.Context, key string, p peerResponse, lastSeen int64, ttl time.Duration) error {
	if r == nil {
		return nil
	}
	b, err := json.Marshal(cachedPeer{Key: p.Key, Address: p.Address, State: p.State, LastSeen: lastSeen})
	if err != nil {
		return err
	}
	return r.client.Set(ctx, "peer:"+key, b, ttl).Err()
}

func (r *RedisCache) GetPeer(ctx context.Context, key string) (*cachedPeer, error) {
	if r == nil {
		return nil, redis.Nil
	}
	b, err := r.client.Get(ctx, "peer:"+key).Bytes()
	if err != nil {
		return nil, err
	}
	var p cachedPeer
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (r *RedisCache) DelPeer(ctx context.Context, key string) error {
	if r == nil {
		return nil
	}
	return r.client.Del(ctx, "peer:"+key).Err()
}

// storedRequest is the Redis body. expires_at stays out of the HTTP body.
type storedRequest struct {
	ConnectRequest
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// Pending requests
func (r *RedisCache) SetRequest(ctx context.Context, cr *ConnectRequest, ttl time.Duration) error {
	if r == nil {
		return nil
	}
	b, err := json.Marshal(storedRequest{ConnectRequest: *cr, ExpiresAt: cr.expiresAt})
	if err != nil {
		return err
	}
	pipe := r.client.Pipeline()
	pipe.Set(ctx, "req:"+cr.ID, b, ttl)
	pipe.SAdd(ctx, "pending:"+cr.ToKey, cr.ID)
	pipe.Expire(ctx, "pending:"+cr.ToKey, ttl)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisCache) GetRequest(ctx context.Context, id string) (*ConnectRequest, error) {
	if r == nil {
		return nil, redis.Nil
	}
	b, err := r.client.Get(ctx, "req:"+id).Bytes()
	if err != nil {
		return nil, err
	}
	var stored storedRequest
	if err := json.Unmarshal(b, &stored); err != nil {
		return nil, err
	}
	cr := stored.ConnectRequest
	cr.expiresAt = stored.ExpiresAt
	return &cr, nil
}

// fetchStored reads every pending set. A body that does not parse is counted
// and left in place. Any other error aborts the fill.
func (r *RedisCache) fetchStored(ctx context.Context) ([]storedRequest, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if r == nil || r.client == nil {
		return nil, 0, nil
	}
	var records []storedRequest
	var skipped int
	var cursor uint64
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		keys, next, err := r.client.Scan(ctx, cursor, "pending:*", 100).Result()
		if err != nil {
			return nil, 0, err
		}
		for _, key := range keys {
			toKey := strings.TrimPrefix(key, "pending:")
			ids, err := r.PendingIDs(ctx, toKey)
			if err != nil {
				return nil, 0, err
			}
			for _, id := range ids {
				cr, err := r.GetRequest(ctx, id)
				if err == nil {
					records = append(records, storedRequest{ConnectRequest: *cr, ExpiresAt: cr.expiresAt})
					continue
				}
				if errors.Is(err, redis.Nil) {
					continue
				}
				if badRequestBody(err) {
					skipped++
					continue
				}
				return nil, 0, err
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return records, skipped, nil
}

func badRequestBody(err error) bool {
	var syntax *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	return errors.As(err, &syntax) || errors.As(err, &typeErr)
}

// DelRequest removes the request body and its pending-set membership.
// A nil cache returns nil.
func (r *RedisCache) DelRequest(ctx context.Context, id, toKey string) error {
	if r == nil || r.client == nil {
		return nil
	}
	pipe := r.client.Pipeline()
	pipe.Del(ctx, "req:"+id)
	if toKey != "" {
		pipe.SRem(ctx, "pending:"+toKey, id)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// PendingIDs returns ids whose req key still exists and removes the rest from the set.
func (r *RedisCache) PendingIDs(ctx context.Context, toKey string) ([]string, error) {
	if r == nil {
		return nil, nil
	}
	ids, err := r.client.SMembers(ctx, "pending:"+toKey).Result()
	if err != nil || len(ids) == 0 {
		return ids, err
	}
	kept := make([]string, 0, len(ids))
	var gone []any
	for _, id := range ids {
		n, err := r.client.Exists(ctx, "req:"+id).Result()
		if err != nil {
			return nil, err
		}
		if n == 0 {
			gone = append(gone, id)
			continue
		}
		kept = append(kept, id)
	}
	if len(gone) > 0 {
		if err := r.client.SRem(ctx, "pending:"+toKey, gone...).Err(); err != nil {
			return nil, err
		}
	}
	return kept, nil
}
