package common

import (
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/component"
)

type CommonSink struct {
	*component.CoreSink
	*Common
	MetadataPrefix string
}

func NewCommonSink(l *slog.Logger, name, metadataPrefix string, opts ...Option) *CommonSink {
	coreSink := component.NewCoreSink(l, name)
	c := &CommonSink{
		CoreSink:       coreSink,
		Common:         NewCommon(coreSink.Core, coreSink.Component()),
		MetadataPrefix: metadataPrefix,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}
