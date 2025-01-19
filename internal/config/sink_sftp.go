package config

// SinkSFTPConfig is the configuration for the SFTP sink.
type SinkSFTPConfig struct {
	Address               string `env:"SFTP__ADDRESS"`
	Username              string `env:"SFTP__USERNAME"`
	PrivateKey            string `env:"SFTP__PRIVATE_KEY"`
	HostFingerprint       string `env:"SFTP__HOST_FINGERPRINT"`
	Password              string `env:"SFTP__PASSWORD"`
	GroupBy               string `env:"SFTP__GROUP_BY"`
	GroupBatchSize        int    `env:"SFTP__GROUP_BATCH_SIZE" envDefault:"1000"`
	GroupColumnName       string `env:"SFTP__GROUP_COLUMN_NAME"`
	ColumnMappingFilePath string `env:"SFTP__COLUMN_MAPPING_FILE_PATH"`
	FileFormat            string `env:"SFTP__FILE_FORMAT"`
	DestinationPath       string `env:"SFTP__DESTINATION_PATH"`
	FilenamePattern       string `env:"SFTP__FILENAME_PATTERN"`
}

// SinkSFTP parses the environment variables and returns the SinkSFTPConfig.
func SinkSFTP(envs ...string) (*SinkSFTPConfig, error) {
	return parse[SinkSFTPConfig](envs...)
}
