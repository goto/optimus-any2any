package config

type SinkOSSConfig struct {
	DestinationBucketPath string `env:"OSS__DESTINATION_BUCKET_PATH"`
	BatchSize             int    `env:"OSS__BATCH_SIZE" envDefault:"1000"`
	ServiceAccount        string `env:"OSS__SERVICE_ACCOUNT"`
	FilenamePattern       string `env:"OSS__FILENAME_PATTERN"`
	EnableOverwrite       bool   `env:"OSS__ENABLE_OVERWRITE" envDefault:"false"`
}

func SinkOSS(envs ...string) (*SinkOSSConfig, error) {
	return parse[SinkOSSConfig](envs...)
}
