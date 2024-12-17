package config

type SinkOSSConfig struct {
	DestinationBucketPath string `env:"OSS__DESTINATION_BUCKET_PATH"`
	BatchSize             int    `env:"OSS__BATCH_SIZE" envDefault:"1000"`
	ServiceAccount        string `env:"OSS__SERVICE_ACCOUNT"`
	FilenamePrefix        string `env:"OSS__FILENAME_PREFIX"`
}

func SinkOSS(envs ...string) (*SinkOSSConfig, error) {
	return parse[SinkOSSConfig](envs...)
}
