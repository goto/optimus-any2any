package config

// SourceMCConfig is a configuration for the source maxcompute component.
type SourceMCConfig struct {
	Credentials      string            `env:"MC__CREDENTIALS"`
	QueryFilePath    string            `env:"MC__QUERY_FILE_PATH"`
	PreQueryFilePath string            `env:"MC__PRE_QUERY_FILE_PATH"`
	ExecutionProject string            `env:"MC__EXECUTION_PROJECT"`
	AdditionalHints  map[string]string `env:"ADDITIONAL_HINTS" envKeyValSeparator:"=" envSeparator:","`
}

// SourceMC parses the environment variables and returns the source maxcompute configuration.
func SourceMC(envs ...string) (*SourceMCConfig, error) {
	return parse[SourceMCConfig](envs...)
}
