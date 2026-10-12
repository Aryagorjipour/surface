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

// Peer cache
func (r *RedisCache) SetPeer(ctx context.Context, key string, p peerResponse, ttl time.Duration) error {
	if r == nil {
		return nil
	}
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, "peer:"+key, b, ttl).Err()
}

func (r *RedisCache) GetPeer(ctx context.Context, key string) (*peerResponse, error) {
	if r == nil {
		return nil, redis.Nil
	}
	b, err := r.client.Get(ctx, "peer:"+key).Bytes()
	if err != nil {
		return nil, err
	}
	var p peerResponse
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

func (r *RedisCache) UpdateRequestStatus(ctx context.Context, id, status string) error {
	if r == nil {
		return nil
	}
	cr, err := r.GetRequest(ctx, id)
	if err != nil {
		return err
	}
	cr.Status = status
	b, _ := json.Marshal(cr)
	return r.client.Set(ctx, "req:"+id, b, 10*time.Minute).Err()
}

func (r *RedisCache) PendingIDs(ctx context.Context, toKey string) ([]string, error) {
	if r == nil {
		return nil, nil
	}
	return r.client.SMembers(ctx, "pending:"+toKey).Result()
}
