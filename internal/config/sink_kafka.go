package config

// SinkKafkaConfig is a configuration for the sink kafka component.
type SinkKafkaConfig struct {
	BootstrapServers []string `env:"KAFKA__BOOTSTRAP_SERVERS"`
	Topic            string   `env:"KAFKA__TOPIC"`
	Key              string   `env:"KAFKA__KEY"`
	ValueSerializer  string   `env:"KAFKA__VALUE_SERIALIZER" envDefault:"json"`
	KeySourceField   string   `env:"KAFKA__KEY_SOURCE_FIELD" envDefault:"__proto_key_b64__"`
	ValueSourceField string   `env:"KAFKA__VALUE_SOURCE_FIELD" envDefault:"__proto_value_b64__"`
}

// SinkKafka parses the environment variables and returns the sink kafka configuration.
func SinkKafka(envs ...string) (*SinkKafkaConfig, error) {
	return parse[SinkKafkaConfig](envs...)
}
