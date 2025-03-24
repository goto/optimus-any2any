package config

// SinkSMTPConfig is a configuration for the sink smtp component.
type SinkSMTPConfig struct {
	ConnectionDSN      string `env:"SMTP__CONNECTION_DSN"`
	From               string `env:"SMTP__FROM"`
	To                 string `env:"SMTP__TO"`
	Subject            string `env:"SMTP__SUBJECT"`
	BodyFilePath       string `env:"SMTP__BODY_FILE_PATH"`
	AttachmentFilename string `env:"SMTP__ATTACHMENT_FILENAME"`
}

// SinkSMTP parses the environment variables and returns the sink smtp configuration.
func SinkSMTP(envs ...string) (*SinkSMTPConfig, error) {
	return parse[SinkSMTPConfig](envs...)
}
