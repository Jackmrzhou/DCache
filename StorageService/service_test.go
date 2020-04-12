package StorageService

import (
	"Puzzle/conf"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var service *storageService

func TestStorageService_Set(t *testing.T) {
	err := service.Set("hello", "world", 0)
	assert.Nil(t, err)
	value, err := service.Get("hello")
	assert.Nil(t, err)
	assert.Equal(t, value, "world")
}


func TestMain(m *testing.M) {
	service	= NewStorageService(&conf.Config{RedisConf:conf.RedisConfig{
		Host:"192.168.0.100",
		Port:"6379",
		Password:"",
	}})
	val := m.Run()
	os.Exit(val)
}