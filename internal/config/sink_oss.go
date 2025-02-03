package config

type SinkOSSConfig struct {
	Credentials           string `env:"OSS__CREDENTIALS"`
	DestinationURI        string `env:"OSS__DESTINATION_URI"`
	GroupBy               string `env:"OSS__GROUP_BY"`
	GroupBatchSize        int    `env:"OSS__GROUP_BATCH_SIZE" envDefault:"1000"`
	GroupColumnName       string `env:"OSS__GROUP_COLUMN_NAME"`
	ColumnMappingFilePath string `env:"OSS__COLUMN_MAPPING_FILE_PATH"`
	FilenamePattern       string `env:"OSS__FILENAME_PATTERN"`
	EnableOverwrite       bool   `env:"OSS__ENABLE_OVERWRITE" envDefault:"false"`
}

func SinkOSS(envs ...string) (*SinkOSSConfig, error) {
	return parse[SinkOSSConfig](envs...)
}
