package config

// SourceOSSConfig is a configuration for the source oss component.
type SourceOSSConfig struct {
	Credentials           string `env:"OSS__CREDENTIALS"`
	SourceURI             string `env:"OSS__SOURCE_URI"`
	FileFormat            string `env:"OSS__FILE_FORMAT" envDefault:"json"`
	CSVDelimiter          rune   `env:"OSS__CSV_DELIMITER" envDefault:","`
	ColumnMappingFilePath string `env:"OSS__COLUMN_MAPPING_FILE_PATH"`
}

// SourceOSS parses the environment variables and returns the source oss configuration.
func SourceOSS(envs ...string) (*SourceOSSConfig, error) {
	return parse[SourceOSSConfig](envs...)
}
