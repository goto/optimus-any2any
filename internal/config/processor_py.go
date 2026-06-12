package config

// ProcessorPYConfig is a configuration for the PY processor component.
type ProcessorPYConfig struct {
	ScriptPath   string `env:"PY__SCRIPT_PATH"`
	PythonBinary string `env:"PY__PYTHON_BINARY" envDefault:"python3"`
}

// ProcessorPY parses the environment variables and returns the PY processor configuration.
func ProcessorPY(envs ...string) (*ProcessorPYConfig, error) {
	return parse[ProcessorPYConfig](envs...)
}
