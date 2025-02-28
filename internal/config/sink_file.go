package config

// SinkFileConfig is a configuration for the sink file component.
type SinkFileConfig struct {
	DestinationURI string `env:"FILE__DESTINATION_URI"`
}

// SinkFile parses the environment variables and returns the sink file configuration.
func SinkFile(envs ...string) (*SinkFileConfig, error) {
	return parse[SinkFileConfig](envs...)
}
