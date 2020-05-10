package conf

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

var GlobalConf *Config

type RedisConfig struct {
	Port string `yaml:"port"`
	Host string `yaml:"host"`
	Password string `yaml:"password"`
	RDBLocation string `yaml:"rdb_location"`
	MaxMemory string `yaml:"max_memory"`
}

type RaftConfig struct {
	NodeID uint64 `yaml:"node_id"`
	InitialMembers map[uint64]string `yaml:"initial_members"`
	DataDir string `yaml:"data_dir"`
}

type Config struct {
	RedisConf RedisConfig `yaml:"redis_conf"`
	RaftConf RaftConfig	`yaml:"raft_conf"`
	Cluster bool `yaml:"cluster"`
	ServicePort string `yaml:"service_port"`
}

func LoadConf(path string) (*Config,error) {
	GlobalConf = &Config{}

	if data, err := ioutil.ReadFile("_puzzle_config.yml"); err == nil {
		if err = yaml.Unmarshal(data, GlobalConf); err == nil {
			return GlobalConf, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}