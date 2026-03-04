package config

// SourceFileConfig is a configuration for the source file component.
type SourceFileConfig struct {
	SourceURI      string `env:"FILE__SOURCE_URI"`
	FilenameColumn string `env:"FILE__FILENAME_COLUMN" envDefault:"__METADATA__filename"`
}

// SourceFile parses the environment variables and returns the source file configuration.
func SourceFile(envs ...string) (*SourceFileConfig, error) {
	return parse[SourceFileConfig](envs...)
}
