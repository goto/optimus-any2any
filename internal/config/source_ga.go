package config

// SourceGAConfig is a configuration for the source google analytics component.
type SourceGAConfig struct {
	PropertyID string   `env:"GA__PROPERTY_ID"`
	StartDate  string   `env:"GA__START_DATE"`
	EndDate    string   `env:"GA__END_DATE"`
	Dimensions []string `env:"GA__DIMENSIONS"`
	Metrics    []string `env:"GA__METRICS"`
	BatchSize  int64    `env:"GA__BATCH_SIZE" default:"1000"`

	ServiceAccount      string `env:"GA__SERVICE_ACCOUNT"`
	ConnectionTLSCert   string `env:"GA__CONNECTION_TLS_CERT"`
	ConnectionTLSCACert string `env:"GA__CONNECTION_TLS_CACERT"`
	ConnectionTLSKey    string `env:"GA__CONNECTION_TLS_KEY"`
}

// SourceGA parses the environment variables and returns the source google analytics configuration.
func SourceGA(envs ...string) (*SourceGAConfig, error) {
	return parse[SourceGAConfig](envs...)
}
