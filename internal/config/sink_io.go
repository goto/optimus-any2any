package config

// SinkIOConfig is a configuration for the sink io component.
// It's an empty struct because the sink io component doesn't have any configuration yet.
type SinkIOConfig struct{}

// SinkIO parses the environment variables and returns the sink io configuration.
func SinkIO(envs ...string) (*SinkIOConfig, error) {
	return parse[SinkIOConfig](envs...)
}
