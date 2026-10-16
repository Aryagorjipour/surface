package connector

import (
	"context"
	"encoding/json"
	"fmt"
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

// Pending requests
func (r *RedisCache) SetRequest(ctx context.Context, cr *ConnectRequest, ttl time.Duration) error {
	if r == nil {
		return nil
	}
	b, err := json.Marshal(cr)
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
	var cr ConnectRequest
	if err := json.Unmarshal(b, &cr); err != nil {
		return nil, err
	}
	return &cr, nil
}

func (r *RedisCache) UpdateRequestStatus(ctx context.Context, id, status string, ttl time.Duration) error {
	if r == nil {
		return nil
	}
	cr, err := r.GetRequest(ctx, id)
	if err != nil {
		return err
	}
	cr.Status = status
	b, _ := json.Marshal(cr)
	return r.client.Set(ctx, "req:"+id, b, ttl).Err()
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
