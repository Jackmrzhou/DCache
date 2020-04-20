package conf

type RedisConfig struct {
	Port string
	Host string
	Password string
}

type Config struct {
	RedisConf RedisConfig
}

func LoadConf(path string) (*Config,error) {
	//todo
	return &Config{RedisConf:RedisConfig{
		Port:"6379",
		Host:"192.168.0.112",
		Password:"",
	}}, nil
}