package config

// RedisSinkConfig holds the configuration for the Redis sink.
type RedisSinkConfig struct {
	ConnectionDSN       string `env:"REDIS__CONNECTION_DSN"`
	ConnectionTLSCert   string `env:"REDIS__CONNECTION_TLS_CERT"`
	ConnectionTLSCACert string `env:"REDIS__CONNECTION_TLS_CACERT"`
	ConnectionTLSKey    string `env:"REDIS__CONNECTION_TLS_KEY"`
	RecordKey           string `env:"REDIS__RECORD_KEY"`
	RecordValue         string `env:"REDIS__RECORD_VALUE"`
}

// SinkRedis parses the environment variables and returns the RedisSinkConfig.
func SinkRedis(envs ...string) (*RedisSinkConfig, error) {
	return parse[RedisSinkConfig](envs...)
}
