package StorageService

import (
	"github.com/go-redis/redis/v7"
	"time"
)

type redisBackend struct {
	client *redis.Client
}

func (r *redisBackend) Set(key, value string, ex int64) error {
	return r.client.Set(key, value, time.Duration(ex) * time.Second).Err()
}

func (r *redisBackend) Get(key string) (string, error) {
	return r.client.Get(key).Result()
}