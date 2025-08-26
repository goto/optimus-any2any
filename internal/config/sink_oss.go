package config

type SinkOSSConfig struct {
	Credentials              string `env:"OSS__CREDENTIALS"`
	DestinationURI           string `env:"OSS__DESTINATION_URI"`
	BatchSize                int    `env:"OSS__BATCH_SIZE"`
	EnableOverwrite          bool   `env:"OSS__ENABLE_OVERWRITE" envDefault:"false"`
	SkipHeader               bool   `env:"OSS__SKIP_HEADER" envDefault:"false"`
	CompressionType          string `env:"OSS__COMPRESSION_TYPE"`
	CompressionPassword      string `env:"OSS__COMPRESSION_PASSWORD"`
	ConnectionTimeoutSeconds int    `env:"OSS__CONNECTION_TIMEOUT_SECONDS" envDefault:"30"`
	ReadWriteTimeoutSeconds  int    `env:"OSS__READ_WRITE_TIMEOUT_SECONDS" envDefault:"60"`
	JSONPathSelector         string `env:"OSS__JSONPATH_SELECTOR"`
	CSVDelimiter             rune   `env:"OSS__CSV_DELIMITER" envDefault:","`
}

func SinkOSS(envs ...string) (*SinkOSSConfig, error) {
	return parse[SinkOSSConfig](envs...)
}
