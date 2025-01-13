package config

// SourceGmailConfig is a configuration for the source gmail component.
type SourceGmailConfig struct {
	Filter                string `env:"GMAIL__FILTER"`
	Token                 string `env:"GMAIL__TOKEN"`
	ExtractorSource       string `env:"GMAIL__EXTRACTOR_SOURCE" envDefault:"attachment"`
	ExtractorPattern      string `env:"GMAIL__EXTRACTOR_PATTERN" envDefault:".*"`
	ExtractorFileFormat   string `env:"GMAIL__EXTRACTOR_FILE_FORMAT" envDefault:"csv"`
	ResultFilenameColumn  string `env:"GMAIL__RESULT_FILENAME_COLUMN"`
	ColumnMappingFilePath string `env:"GMAIL__COLUMN_MAPPING_FILE_PATH"`
}

// SourceGmail parses the environment variables and returns the source gmail configuration.
func SourceGmail(envs ...string) (*SourceGmailConfig, error) {
	return parse[SourceGmailConfig](envs...)
}
