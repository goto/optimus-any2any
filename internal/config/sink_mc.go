package config

// SinkMCConfig is a configuration for the sink maxcompute component.
type SinkMCConfig struct {
	Credentials        string `env:"MC__CREDENTIALS"`
	DestinationTableID string `env:"MC__DESTINATION_TABLE_ID"`
	LoadMethod         string `env:"MC__LOAD_METHOD" envDefault:"APPEND"`
	UploadMode         string `env:"MC__UPLOAD_MODE" envDefault:"STREAM"`
}

// SinkMC parses the environment variables and returns the sink maxcompute configuration.
func SinkMC(envs ...string) (*SinkMCConfig, error) {
	return parse[SinkMCConfig](envs...)
}
