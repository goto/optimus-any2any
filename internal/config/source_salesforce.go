package config

// SourceSalesforceConfig is a configuration for the source salesforce component.
type SourceSalesforceConfig struct {
	Host  string `env:"SALESFORCE__HOST"`
	User  string `env:"SALESFORCE__USER"`
	Pass  string `env:"SALESFORCE__PASS"`
	Token string `env:"SALESFORCE__TOKEN"`

	SOQLFilePath    string `env:"SALESFORCE__SOQL_FILE_PATH"`
	MappingFilePath string `env:"SALESFORCE__MAPPING_FILE_PATH"`
}

// SourceSalesforce parses the environment variables and returns the source salesforce configuration.
func SourceSalesforce(envs ...string) (*SourceSalesforceConfig, error) {
	return parse[SourceSalesforceConfig](envs...)
}
