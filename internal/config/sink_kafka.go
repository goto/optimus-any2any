package config

// SinkKafkaConfig is a configuration for the sink kafka component.
type SinkKafkaConfig struct {
	BootstrapServers []string `env:"KAFKA__BOOTSTRAP_SERVERS"`
	Topic            string   `env:"KAFKA__TOPIC"`
}

// SinkKafka parses the environment variables and returns the sink kafka configuration.
func SinkKafka(envs ...string) (*SinkKafkaConfig, error) {
	return parse[SinkKafkaConfig](envs...)
}
