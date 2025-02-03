package config

// OSS2MCConfig is a configuration for the OSS to MaxCompute component.
type OSS2MCConfig struct {
	SourceBucketPath       string   `env:"OSS2MC__SOURCE_BUCKET_PATH"`
	FileFormat             string   `env:"OSS2MC__FILE_FORMAT" envDefault:"json"`
	Credentials            string   `env:"OSS2MC__CREDENTIALS"`
	DestinationTableID     string   `env:"OSS2MC__DESTINATION_TABLE_ID"`
	LoadMethod             string   `env:"OSS2MC__LOAD_METHOD" envDefault:"APPEND"`
	PartitionValues        []string `env:"OSS2MC__PARTITION_VALUES" envDefault:""`
	LogViewRetentionInDays int      `env:"OSS2MC__LOG_VIEW_RETENTION_IN_DAYS" envDefault:"2"`
}

// OSS2MC parses the environment variables and returns the OSS to MaxCompute configuration.
func OSS2MC(envs ...string) (*OSS2MCConfig, error) {
	return parse[OSS2MCConfig](envs...)
}
