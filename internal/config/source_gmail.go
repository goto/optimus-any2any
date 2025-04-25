package config

// SourceGmailConfig is a configuration for the source gmail component.
type SourceGmailConfig struct {
	Filter         string `env:"GMAIL__FILTER"`
	Token          string `env:"GMAIL__TOKEN"`
	FilenameColumn string `env:"GMAIL__FILENAME_COLUMN" envDefault:"__METADATA__filename"`
	CSVDelimiter   rune   `env:"GMAIL__CSV_DELIMITER" envDefault:","`
}

// SourceGmail parses the environment variables and returns the source gmail configuration.
func SourceGmail(envs ...string) (*SourceGmailConfig, error) {
	return parse[SourceGmailConfig](envs...)
}
