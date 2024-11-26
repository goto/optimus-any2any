package connector

import "github.com/goto/optimus-any2any/pkg/flow"

// PassThrough is a connector that passes data from outlet to inlet.
func PassThrough(outlet flow.Outlet, inlet flow.Inlet) {
	go func() {
		defer func() {
			println("DEBUG: connector: close")
			close(inlet.In())
		}()
		for v := range outlet.Out() {
			println("DEBUG: connector: send:", string(v.([]byte)))
			inlet.In() <- v
			println("DEBUG: connector: done:", string(v.([]byte)))
		}
	}()
}
