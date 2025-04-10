package config

// SourceMCConfig is a configuration for the source maxcompute component.
type SourceMCConfig struct {
	Credentials            string            `env:"MC__CREDENTIALS"`
	QueryFilePath          string            `env:"MC__QUERY_FILE_PATH"`
	PreQueryFilePath       string            `env:"MC__PRE_QUERY_FILE_PATH"`
	ExecutionProject       string            `env:"MC__EXECUTION_PROJECT"`
	AdditionalHints        map[string]string `env:"MC__ADDITIONAL_HINTS" envKeyValSeparator:"=" envSeparator:","`
	LogViewRetentionInDays int               `env:"MC__LOG_VIEW_RETENTION_IN_DAYS" envDefault:"2"`
	BatchSize              int               `env:"MC__BATCH_SIZE" envDefault:"1000"`
}

// SourceMC parses the environment variables and returns the source maxcompute configuration.
func SourceMC(envs ...string) (*SourceMCConfig, error) {
	return parse[SourceMCConfig](envs...)
}
