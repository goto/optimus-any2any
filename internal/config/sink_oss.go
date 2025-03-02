package config

type SinkOSSConfig struct {
	Credentials     string `env:"OSS__CREDENTIALS"`
	DestinationURI  string `env:"OSS__DESTINATION_URI"`
	BatchSize       int    `env:"OSS__BATCH_SIZE"`
	EnableOverwrite bool   `env:"OSS__ENABLE_OVERWRITE" envDefault:"false"`
}

func SinkOSS(envs ...string) (*SinkOSSConfig, error) {
	return parse[SinkOSSConfig](envs...)
}
