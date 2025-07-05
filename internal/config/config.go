package config

// Config is a common configuration for the component.
type Config struct {
	LogLevel                  string `env:"LOG_LEVEL" envDefault:"INFO"`
	OtelCollectorGRPCEndpoint string `env:"OTEL_COLLECTOR_GRPC_ENDPOINT"`
	OtelAttributes            string `env:"OTEL_ATTRIBUTES"`
	BufferSize                int    `env:"BUFFER_SIZE"`
	Backend                   string `env:"BACKEND" envDefault:"IO"`
	MetadataPrefix            string `env:"METADATA_PREFIX" envDefault:"__METADATA__"`
	DryRun                    bool   `env:"DRY_RUN" envDefault:"false"`
	RetryMax                  int    `env:"RETRY_MAX" envDefault:"3"`
	RetryBackoffMs            int64  `env:"RETRY_BACKOFF_MS" envDefault:"1000"`
	EnablePprof               bool   `env:"ENABLE_PPROF" envDefault:"false"`
	SourceConcurrency         int    `env:"SOURCE_CONCURRENCY" envDefault:"4"`
	SinkConcurrency           int    `env:"SINK_CONCURRENCY" envDefault:"4"`
	ConnectorProcessor        string `env:"CONNECTOR_PROCESSOR" envDefault:"JQ"` // JQ or PY
}

// NewConfig parses the environment variables and returns the common configuration.
func NewConfig(envs ...string) (*Config, error) {
	return parse[Config](envs...)
}
