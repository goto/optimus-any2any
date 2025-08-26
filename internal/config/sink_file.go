package config

// SinkFileConfig is a configuration for the sink file component.
type SinkFileConfig struct {
	DestinationURI      string `env:"FILE__DESTINATION_URI"`
	CompressionType     string `env:"FILE__COMPRESSION_TYPE"`
	CompressionPassword string `env:"FILE__COMPRESSION_PASSWORD"`
	JSONPathSelector    string `env:"FILE__JSONPATH_SELECTOR"`
	CSVDelimiter        rune   `env:"FILE__CSV_DELIMITER" envDefault:","`
}

// SinkFile parses the environment variables and returns the sink file configuration.
func SinkFile(envs ...string) (*SinkFileConfig, error) {
	return parse[SinkFileConfig](envs...)
}
