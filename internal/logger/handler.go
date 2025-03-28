package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)

type customHandler struct {
	handler slog.Handler
	w       io.Writer
	group   string
	attrs   []slog.Attr
}

func (h *customHandler) Handle(ctx context.Context, r slog.Record) error {
	// Format time
	timeStr := r.Time.Format(time.RFC3339)

	// Prepare group string
	var groupStr string
	if h.group != "" {
		if len(h.attrs) > 0 {
			attrs := []string{}
			for i := 0; i < len(h.attrs) && h.attrs[i].Key == "name"; i++ {
				attrs = append(attrs, h.attrs[i].Value.String())
			}
			groupStr = fmt.Sprintf("%s(%v)", h.group, strings.Join(attrs, ","))
		} else {
			groupStr = h.group
		}
		groupStr += ": "
	}

	// Create custom log message
	customMsg := fmt.Sprintf("%s %s %s%s\n",
		timeStr,
		strings.ToUpper(r.Level.String()),
		groupStr,
		r.Message,
	)

	_, err := h.w.Write([]byte(customMsg))
	return err
}

func (h *customHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h *customHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &customHandler{
		handler: h.handler,
		w:       h.w,
		group:   h.group,
		attrs:   append(h.attrs, attrs...),
	}
}

func (h *customHandler) WithGroup(name string) slog.Handler {
	return &customHandler{
		handler: h.handler,
		w:       h.w,
		group:   name,
		attrs:   []slog.Attr{},
	}
}

func newCustomHandler(opts *slog.HandlerOptions) slog.Handler {
	return &customHandler{
		handler: slog.NewTextHandler(os.Stdout, opts),
		w:       os.Stdout,
	}
}
