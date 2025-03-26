package config

// Config is a common configuration for the component.
type Config struct {
	LogLevel                  string `env:"LOG_LEVEL" envDefault:"INFO"`
	OtelCollectorGRPCEndpoint string `env:"OTEL_COLLECTOR_GRPC_ENDPOINT"`
	OtelAttributes            string `env:"OTEL_ATTRIBUTES"`
	BufferSize                int    `env:"BUFFER_SIZE"`
	MetadataPrefix            string `env:"METADATA_PREFIX" envDefault:"__METADATA__"`
	RetryMax                  int    `env:"RETRY_MAX" envDefault:"3"`
}

// NewConfig parses the environment variables and returns the common configuration.
func NewConfig(envs ...string) (*Config, error) {
	return parse[Config](envs...)
}
