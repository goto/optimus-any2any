package config

// SinkSMTPConfig is a configuration for the sink smtp component.
type SinkSMTPConfig struct {
	Address            string `env:"SMTP__ADDRESS"`
	Username           string `env:"SMTP__USERNAME"`
	Password           string `env:"SMTP__PASSWORD"`
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
