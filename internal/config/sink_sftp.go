package config

// SinkSFTPConfig is the configuration for the SFTP sink.
type SinkSFTPConfig struct {
	PrivateKey          string `env:"SFTP__PRIVATE_KEY"`
	HostFingerprint     string `env:"SFTP__HOST_FINGERPRINT"`
	DestinationURI      string `env:"SFTP__DESTINATION_URI"`
	CompressionPassword string `env:"SFTP__COMPRESSION_PASSWORD"`
	CompressionType     string `env:"SFTP__COMPRESSION_TYPE"`
	JSONPathSelector    string `env:"SFTP__JSONPATH_SELECTOR"`
	EnableOverwrite     bool   `env:"SFTP__ENABLE_OVERWRITE" default:"false"`
	SkipHeader          bool   `env:"SFTP__SKIP_HEADER" default:"false"`
}

// SinkSFTP parses the environment variables and returns the SinkSFTPConfig.
func SinkSFTP(envs ...string) (*SinkSFTPConfig, error) {
	return parse[SinkSFTPConfig](envs...)
}
