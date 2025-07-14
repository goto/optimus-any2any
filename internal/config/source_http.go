package config

// SourceHTTPConfig is a configuration for the source http component.
type SourceHTTPConfig struct {
	Endpoint                      string `env:"HTTP__ENDPOINT"`
	HeadersFilePath               string `env:"HTTP__HEADERS_FILE_PATH,file"`
	ClientCredentialsProvider     string `env:"HTTP__CLIENT_CREDENTIALS_PROVIDER" envDefault:""`
	ClientCredentialsClientID     string `env:"HTTP__CLIENT_CREDENTIALS_CLIENT_ID" envDefault:""`
	ClientCredentialsClientSecret string `env:"HTTP__CLIENT_CREDENTIALS_CLIENT_SECRET" envDefault:""`
	ClientCredentialsTokenURL     string `env:"HTTP__CLIENT_CREDENTIALS_TOKEN_URL" envDefault:""`
}

// SourceHTTP parses the environment variables and returns the source http configuration.
func SourceHTTP(envs ...string) (*SourceHTTPConfig, error) {
	return parse[SourceHTTPConfig](envs...)
}
