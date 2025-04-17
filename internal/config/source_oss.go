package config

// SourceOSSConfig is a configuration for the source oss component.
type SourceOSSConfig struct {
	Credentials  string `env:"OSS__CREDENTIALS"`
	SourceURI    string `env:"OSS__SOURCE_URI"`
	CSVDelimiter rune   `env:"OSS__CSV_DELIMITER" envDefault:","`
	SkipHeader   bool   `env:"OSS__SKIP_HEADER" envDefault:"false"`
	SkipRows     int    `env:"OSS__SKIP_ROWS" envDefault:"0"`
}

// SourceOSS parses the environment variables and returns the source oss configuration.
func SourceOSS(envs ...string) (*SourceOSSConfig, error) {
	return parse[SourceOSSConfig](envs...)
}
