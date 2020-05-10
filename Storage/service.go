package Storage

import (
	"Puzzle/Storage/RaftBased"
	"Puzzle/conf"
)

type StorageService struct {
	StorageBackend
}

func (s *StorageService) Set(key, value string, ex int64) error {
	return s.StorageBackend.Set(key, value, ex)
}

func (s *StorageService) Get(key string) (string, error) {
	return s.StorageBackend.Get(key)
}

func (s *StorageService) HSet(key, field, val string) error {
	return s.StorageBackend.HSet(key, field, val)
}

func (s *StorageService) HGetAll(key string) (map[string]string, error) {
	return s.StorageBackend.HGetAll(key)
}

func (s *StorageService) HMSet(key string, fields map[string]interface{}) error {
	return s.StorageBackend.HMSet(key, fields)
}

func NewStorageService(conf *conf.Config) *StorageService {
	if !conf.Cluster {
		return &StorageService{NewRedisBackend(conf)}
	} else {
		return &StorageService{RaftBased.NewRaftBackend(conf)}
	}
}