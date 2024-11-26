package main

import (
	"flag"
	"fmt"

	"github.com/goto/optimus-any2any/internal/logger"
)

func main() {
	// initiate default logger
	l := logger.NewDefaultLogger()

	// parse flags
	source := flag.String("from", "file", "source component")
	sink := flag.String("to", "io", "sink component")

	// any2any is the main function to execute the data transfer from any source to any destination.
	// It also handles graceful shutdown by listening to os signals.
	// It returns error if any.
	if err := any2any(*source, *sink); err != nil {
		l.Error(fmt.Sprintf("error: %s", err.Error()))
		fmt.Printf("error: %+v\n", err)
		return
	}
}
