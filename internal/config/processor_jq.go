package config

// ProcessorJQConfig is a configuration for the processor jq component.
type ProcessorJQConfig struct {
	Query string `env:"JQ__QUERY"`
}

// ProcessorJQ parses the environment variables and returns the processor jq configuration.
func ProcessorJQ(envs ...string) (*ProcessorJQConfig, error) {
	return parse[ProcessorJQConfig](envs...)
}
