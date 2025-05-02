package config

// SinkMCConfig is a configuration for the sink maxcompute component.
type SinkMCConfig struct {
	Credentials         string `env:"MC__CREDENTIALS"`
	DestinationTableID  string `env:"MC__DESTINATION_TABLE_ID"`
	LoadMethod          string `env:"MC__LOAD_METHOD" envDefault:"APPEND"`
	UploadMode          string `env:"MC__UPLOAD_MODE" envDefault:"REGULAR"`
	Concurrency         int    `env:"MC__CONCURRENCY" envDefault:"4"`
	BatchSizeInMB       int    `env:"MC__BATCH_SIZE_IN_MB" envDefault:"64"`
	ExecutionProject    string `env:"MC__EXECUTION_PROJECT"`
	AllowSchemaMismatch bool   `env:"MC__ALLOW_SCHEMA_MISMATCH" envDefault:"false"`
}

// SinkMC parses the environment variables and returns the sink maxcompute configuration.
func SinkMC(envs ...string) (*SinkMCConfig, error) {
	return parse[SinkMCConfig](envs...)
}
