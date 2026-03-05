package config

type SourceGCSConfig struct {
	Credentials  string `env:"GCS__CREDENTIALS"`
	SourceURI    string `env:"GCS__SOURCE_URI"`
	CSVDelimiter rune   `env:"GCS__CSV_DELIMITER" envDefault:","`
	SkipHeader   bool   `env:"GCS__SKIP_HEADER" envDefault:"false"`
	SkipRows     int    `env:"GCS__SKIP_ROWS" envDefault:"0"`
}

func SourceGCS(envs ...string) (*SourceGCSConfig, error) {
	return parse[SourceGCSConfig](envs...)
}
