package config

// SinkSMTPConfig is a configuration for the sink smtp component.
type SinkSMTPConfig struct {
	ConnectionDSN         string `env:"SMTP__CONNECTION_DSN"`
	From                  string `env:"SMTP__FROM"`
	To                    string `env:"SMTP__TO"`
	Subject               string `env:"SMTP__SUBJECT"`
	BodyFilePath          string `env:"SMTP__BODY_FILE_PATH"`
	BodyNoRecordFilePath  string `env:"SMTP__BODY_NO_RECORD_FILE_PATH"`
	AttachmentFilename    string `env:"SMTP__ATTACHMENT_FILENAME"`
	StorageMode           string `env:"SMTP__STORAGE_MODE" envDefault:"attachment"`
	StorageDestinationDir string `env:"SMTP__STORAGE_DESTINATION_DIR"`
	StorageLinkExpiration int    `env:"SMTP__STORAGE_LINK_EXPIRATION" envDefault:"604800"`
	StorageCredentials    string `env:"SMTP__STORAGE_CREDENTIALS"`
	CompressionPassword   string `env:"SMTP__COMPRESSION_PASSWORD"`
	CompressionType       string `env:"SMTP__COMPRESSION_TYPE"`
}

// SinkSMTP parses the environment variables and returns the sink smtp configuration.
func SinkSMTP(envs ...string) (*SinkSMTPConfig, error) {
	return parse[SinkSMTPConfig](envs...)
}
