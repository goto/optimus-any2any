package config

// SourceSalesforceConfig is a configuration for the source salesforce component.
type SourceSalesforceConfig struct {
	Host  string `env:"SF__HOST"`
	User  string `env:"SF__USER"`
	Pass  string `env:"SF__PASS"`
	Token string `env:"SF__TOKEN"`

	SOQLFilePath string `env:"SF__SOQL_FILE_PATH"`
}

// SourceSalesforce parses the environment variables and returns the source salesforce configuration.
func SourceSalesforce(envs ...string) (*SourceSalesforceConfig, error) {
	return parse[SourceSalesforceConfig](envs...)
}
