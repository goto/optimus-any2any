package config

// SourceGmailConfig is a configuration for the source gmail component.
type SourceGmailConfig struct {
	Filter              string `env:"GMAIL__FILTER"`
	Token               string `env:"GMAIL__TOKEN"`
	ExtractorFileFormat string `env:"GMAIL__EXTRACTOR_FILE_FORMAT" envDefault:"csv"`
	FilenameColumn      string `env:"GMAIL__FILENAME_COLUMN" envDefault:"__METADATA__filename"`
}

// SourceGmail parses the environment variables and returns the source gmail configuration.
func SourceGmail(envs ...string) (*SourceGmailConfig, error) {
	return parse[SourceGmailConfig](envs...)
}
