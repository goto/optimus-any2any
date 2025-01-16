package config

// SourceMCConfig is a configuration for the source maxcompute component.
type SourceMCConfig struct {
	ServiceAccount   string `env:"MC__SERVICE_ACCOUNT"`
	QueryFilePath    string `env:"MC__QUERY_FILE_PATH" envDefault:"/data/in/query.sql"`
	ExecutionProject string `env:"MC__EXECUTION_PROJECT"`
}

// SourceMC parses the environment variables and returns the source maxcompute configuration.
func SourceMC(envs ...string) (*SourceMCConfig, error) {
	return parse[SourceMCConfig](envs...)
}
