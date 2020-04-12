package conf

type RedisConfig struct {
	Port string
	Host string
	Password string
}

type Config struct {
	RedisConf RedisConfig
}
