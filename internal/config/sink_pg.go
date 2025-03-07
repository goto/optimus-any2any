package config

// SinkPGConfig is the configuration for the PostgreSQL sink.
type SinkPGConfig struct {
	ConnectionDSN      string `env:"PG__CONNECTION_DSN"`
	PreSQLScript       string `env:"PG__PRE_SQL_SCRIPT"`
	DestinationTableID string `env:"PG__DESTINATION_TABLE_ID"`
}

// SinkPG parses the environment variables and returns the SinkPGConfig.
func SinkPG(envs ...string) (*SinkPGConfig, error) {
	return parse[SinkPGConfig](envs...)
}
