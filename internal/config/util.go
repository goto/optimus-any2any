package config

import (
	"os"
	"strings"

	"github.com/caarlos0/env/v11"
)

// parse parses the environment variables and returns the configuration.
func parse[T any](envs ...string) (*T, error) {
	env0 := toMap(os.Environ())
	env1 := toMap(envs)

	c, err := env.ParseAsWithOptions[T](env.Options{
		Environment: mergeMaps(env0, env1),
	})
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// toMap converts the environment variables to a map.
// for example "KEY=VALUE" to map["KEY"] = "VALUE".
func toMap(env []string) map[string]string {
	r := map[string]string{}
	for _, e := range env {
		p := strings.SplitN(e, "=", 2)
		if len(p) == 2 {
			r[p[0]] = p[1]
		}
	}
	return r
}

// mergeMaps merges multiple maps into one.
// If there are duplicate keys, the value from the last map will be used.
func mergeMaps(maps ...map[string]string) map[string]string {
	r := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			r[k] = v
		}
	}
	return r
}
