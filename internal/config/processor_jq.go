package config

// ProcessorJQConfig is a configuration for the processor jq component.
type ProcessorJQConfig struct {
	Query            string `env:"JQ__QUERY"`
	QueryFilePath    string `env:"JQ__QUERY_FILE_PATH"`
	BatchSize        int    `env:"JQ__BATCH_SIZE" envDefault:"512"`
	BatchIndexColumn string `env:"JQ__BATCH_INDEX_COLUMN" envDefault:"__METADATA__jq_batch_index"`
}

// ProcessorJQ parses the environment variables and returns the processor jq configuration.
func ProcessorJQ(envs ...string) (*ProcessorJQConfig, error) {
	return parse[ProcessorJQConfig](envs...)
}
