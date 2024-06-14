package conf

type RedisConfig struct {
	Address           string `json:"addr"`
	Db                int    `json:"db"`
	Password          string `json:"password"`
	ExpirationSeconds int    `json:"expiration_seconds"`
	PoolSize          int    `json:"pool_size"`
	MaxRetries        int    `json:"max_retries"`
}
