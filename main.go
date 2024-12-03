package main

import (
	"fmt"
	"strings"

	"github.com/goto/optimus-any2any/internal/logger"
	"github.com/spf13/pflag"
)

func main() {
	// initiate default logger
	l := logger.NewDefaultLogger()

	// parse flags
	var source string
	var sink string
	var envs []string
	pflag.StringVar(&source, "from", "file", "source component")
	pflag.StringVar(&sink, "to", "io", "sink component")
	pflag.StringArrayVar(&envs, "env", []string{}, "Pass env as argument (can be used multiple times)")

	// Parse the flags.
	pflag.Parse()

	// any2any is the main function to execute the data transfer from any source to any destination.
	// It also handles graceful shutdown by listening to os signals.
	// It returns error if any.
	if err := any2any(strings.ToUpper(source), strings.ToUpper(sink), envs); err != nil {
		l.Error(fmt.Sprintf("error: %s", err.Error()))
		fmt.Printf("error: %+v\n", err)
		return
	}
}
