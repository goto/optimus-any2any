package main

import (
	"fmt"
	"os"

	"github.com/goto/optimus-any2any/internal/logger"
	"github.com/spf13/pflag"
)

func main() {
	// initiate default logger
	l := logger.NewDefaultLogger()

	// parse flags
	var source string
	var sinks []string
	var envs []string
	var noPipeline bool
	pflag.StringVar(&source, "from", "", "source component")
	pflag.StringArrayVar(&sinks, "to", []string{}, "sink component (can be used multiple times)")
	pflag.StringArrayVar(&envs, "env", []string{}, "pass env as argument (can be used multiple times)")
	pflag.BoolVar(&noPipeline, "no-pipeline", false, "run without pipeline")

	// Parse the flags.
	pflag.Parse()

	// any2any is the main function to execute the data transfer from any source to any destination.
	// It also handles graceful shutdown by listening to os signals.
	// It returns error if any.
	if errs := any2any(source, sinks, noPipeline, envs); len(errs) > 0 {
		for _, err := range errs {
			l.Error(fmt.Sprintf("error: %s", err.Error()))
			fmt.Printf("error: %+v\n", err)
		}
		if len(errs) > 0 {
			os.Exit(1)
		}
	}
}
