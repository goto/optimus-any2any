package config

// TODO: Add the necessary configuration options for the application.
type Config struct {
	LogLevel                  string
	OtelCollectorGRPCEndpoint string
}

func NewConfig() *Config {
	return &Config{}
}
