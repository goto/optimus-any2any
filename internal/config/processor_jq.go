package config

// ProcessorJQConfig is a configuration for the processor jq component.
type ProcessorJQConfig struct {
	Query         string `env:"JQ__QUERY"`
	QueryFilePath string `env:"JQ__QUERY_FILE_PATH"`
}

// ProcessorJQ parses the environment variables and returns the processor jq configuration.
func ProcessorJQ(envs ...string) (*ProcessorJQConfig, error) {
	return parse[ProcessorJQConfig](envs...)
}
