package config

type SinkOSSConfig struct {
	Credentials             string `env:"OSS__CREDENTIALS"`
	DestinationURI          string `env:"OSS__DESTINATION_URI"`
	BatchSize               int    `env:"OSS__BATCH_SIZE"`
	EnableOverwrite         bool   `env:"OSS__ENABLE_OVERWRITE" envDefault:"false"`
	SkipHeader              bool   `env:"OSS__SKIP_HEADER" envDefault:"false"`
	MaxTempFileRecordNumber int    `env:"OSS__MAX_TEMP_FILE_RECORD_NUMBER" envDefault:"50000"`
	CompressionType         string `env:"OSS__COMPRESSION_TYPE"`
	CompressionPassword     string `env:"OSS__COMPRESSION_PASSWORD"`
}

func SinkOSS(envs ...string) (*SinkOSSConfig, error) {
	return parse[SinkOSSConfig](envs...)
}
