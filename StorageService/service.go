package StorageService

import "Puzzle/conf"

type storageService struct {
	StorageBackend
}

func (s *storageService) Set(key, value string, ex int64) error {
	return s.StorageBackend.Set(key, value, ex)
}

func (s *storageService) Get(key string) (string, error) {
	return s.StorageBackend.Get(key)
}

func NewStorageService(conf *conf.Config) *storageService {
	return &storageService{NewRedisBackend(conf)}
}