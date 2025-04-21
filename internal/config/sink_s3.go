package config

type SinkS3Config struct {
	Credentials             string `env:"S3__CREDENTIALS"`
	Provider                string `env:"S3__PROVIDER" envDefault:"aws"`
	Region                  string `env:"S3__REGION" envDefault:"us-east-1"`
	DestinationURI          string `env:"S3__DESTINATION_URI"`
	BatchSize               int    `env:"S3__BATCH_SIZE"`
	EnableOverwrite         bool   `env:"S3__ENABLE_OVERWRITE" envDefault:"false"`
	SkipHeader              bool   `env:"S3__SKIP_HEADER" envDefault:"false"`
	MaxTempFileRecordNumber int    `env:"S3__MAX_TEMP_FILE_RECORD_NUMBER" envDefault:"50000"`
}

func SinkS3(envs ...string) (*SinkS3Config, error) {
	return parse[SinkS3Config](envs...)
}
