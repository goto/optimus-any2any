package config

// SourcePGConfig is a configuration for the source PostgreSQL component.
type SourcePGConfig struct {
	ConnectionDSN     string `env:"PG__CONNECTION_DSN"`
	MaxOpenConnection int32  `env:"PG__MAX_OPEN_CONNECTION"`
	MinOpenConnection int32  `env:"PG__MIN_OPEN_CONNECTION"`
	QueryFilePath     string `env:"PG__QUERY_FILE_PATH"`
}

// SourcePG parses the environment variables and returns the source PG configuration.
func SourcePG(envs ...string) (*SourcePGConfig, error) {
	return parse[SourcePGConfig](envs...)
}
