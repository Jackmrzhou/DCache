package Storage

import (
	"Puzzle/conf"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var service *StorageService

func TestStorageService_Set(t *testing.T) {
	err := service.Set("hello", "world", 0)
	assert.Nil(t, err)
	value, err := service.Get("hello")
	assert.Nil(t, err)
	assert.Equal(t, value, "world")
}

func TestStorageService_HSet(t *testing.T) {
	err := service.HSet("123e", "col", "val")
	assert.Nil(t, err)
	res, err := service.HGetAll("123e")
	assert.Nil(t, err)
	assert.Equal(t, res, map[string]string{"col": "val"})
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