package config

// SinkHTTPConfig is a configuration for the sink http component.
type SinkHTTPConfig struct {
	Method                        string            `env:"HTTP__METHOD" envDefault:"POST"`
	Endpoint                      string            `env:"HTTP__ENDPOINT"`
	Headers                       map[string]string `env:"HTTP__HEADER" envSeparator:"," envKeyValSeparator:":"`
	HeadersFile                   string            `env:"HTTP__HEADERS_FILE_PATH,file"`
	Body                          string            `env:"HTTP__BODY"`
	BodyFilePath                  string            `env:"HTTP__BODY_FILE_PATH,file"`
	BatchSize                     int               `env:"HTTP__BATCH_SIZE" envDefault:"1"`
	ConnectionTLSCert             string            `env:"HTTP__CONNECTION_TLS_CERT"`
	ConnectionTLSCACert           string            `env:"HTTP__CONNECTION_TLS_CACERT"`
	ConnectionTLSKey              string            `env:"HTTP__CONNECTION_TLS_KEY"`
	ClientCredentialsProvider     string            `env:"HTTP__CLIENT_CREDENTIALS_PROVIDER" envDefault:""`
	ClientCredentialsClientID     string            `env:"HTTP__CLIENT_CREDENTIALS_CLIENT_ID" envDefault:""`
	ClientCredentialsClientSecret string            `env:"HTTP__CLIENT_CREDENTIALS_CLIENT_SECRET" envDefault:""`
	ClientCredentialsTokenURL     string            `env:"HTTP__CLIENT_CREDENTIALS_TOKEN_URL" envDefault:""`
}

// SinkHTTP parses the environment variables and returns the sink http configuration.
func SinkHTTP(envs ...string) (*SinkHTTPConfig, error) {
	return parse[SinkHTTPConfig](envs...)
}
