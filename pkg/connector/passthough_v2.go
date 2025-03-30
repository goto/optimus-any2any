package connector

import (
	"bufio"
	"io"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

func PassThroughV2(l *slog.Logger) flow.ConnectV2 {
	return func(r io.ReadCloser, w io.WriteCloser) {
		go func() {
			defer func() {
				l.Debug("connectorv2(passthrough): close")
				w.Close()
			}()
			sc := bufio.NewScanner(r)
			l.Debug("connectorv2(passthrough): start")
			for sc.Scan() {
				l.Debug("connectorv2(passthrough): read")
				raw := sc.Bytes()
				line := make([]byte, len(raw))
				copy(line, raw)
				w.Write(append(line, '\n'))
				l.Debug("connectorv2(passthrough): write")
			}
		}()
	}
}
