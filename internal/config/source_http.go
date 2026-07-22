package config

// SourceHTTPConfig is a configuration for the source http component.
type SourceHTTPConfig struct {
	Endpoint                      string `env:"HTTP__ENDPOINT"`
	HeadersFileContent            string `env:"HTTP__HEADERS_FILE_PATH,file"`
	Method                        string `env:"HTTP__METHOD" envDefault:"GET"`
	Body                          string `env:"HTTP__BODY"`
	BodyFileContent               string `env:"HTTP__BODY_FILE_PATH,file"`
	ResponseFormat                string `env:"HTTP__RESPONSE_FORMAT" envDefault:"ndjson"`
	PaginationType                string `env:"HTTP__PAGINATION_TYPE"` // none (default), cursor, offset
	ResponseRecordsPath           string `env:"HTTP__RESPONSE_RECORDS_JSONPATH" envDefault:"."`
	NextCursorPath                string `env:"HTTP__NEXT_CURSOR_JSONPATH"`
	NextBodyTemplate              string `env:"HTTP__NEXT_BODY_TEMPLATE"`
	OffsetParam                   string `env:"HTTP__OFFSET_PARAM_NAME" envDefault:"offset"`
	LimitParam                    string `env:"HTTP__LIMIT_PARAM_NAME" envDefault:"limit"`
	PageSize                      int    `env:"HTTP__PAGE_SIZE" envDefault:"100"`
	ClientCredentialsProvider     string `env:"HTTP__CLIENT_CREDENTIALS_PROVIDER" envDefault:""`
	ClientCredentialsClientID     string `env:"HTTP__CLIENT_CREDENTIALS_CLIENT_ID" envDefault:""`
	ClientCredentialsClientSecret string `env:"HTTP__CLIENT_CREDENTIALS_CLIENT_SECRET" envDefault:""`
	ClientCredentialsTokenURL     string `env:"HTTP__CLIENT_CREDENTIALS_TOKEN_URL" envDefault:""`
}

// SourceHTTP parses the environment variables and returns the source http configuration.
func SourceHTTP(envs ...string) (*SourceHTTPConfig, error) {
	return parse[SourceHTTPConfig](envs...)
}
