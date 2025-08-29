package oss

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/goccy/go-json"

	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/config"
	"github.com/goto/optimus-any2any/internal/fs"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type OSSSink struct {
	common.Sink

	destinationURITemplate *template.Template
	handlers               fs.WriteHandler
	batchStepTemplate      *template.Template // TODO: deprecate this

	// json path selector
	jsonPathSelector string
}

var _ flow.Sink = (*OSSSink)(nil)

// NewSink creates a new OSSSink
func NewSink(commonSink common.Sink, sinkCfg *config.SinkOSSConfig, opts ...common.Option) (*OSSSink, error) {

	// create OSS client
	client, err := NewOSSClient(commonSink.Context(), sinkCfg.Credentials, OSSClientConfig{
		ConnectionTimeoutSeconds: sinkCfg.ConnectionTimeoutSeconds, // TODO: refactor connection related configs
		ReadWriteTimeoutSeconds:  sinkCfg.ReadWriteTimeoutSeconds,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse destinationURI as template
	tmpl, err := compiler.NewTemplate("sink_oss_destination_uri", sinkCfg.DestinationURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}

	// parse batch step template // TODO: deprecate this, we keep this for backward compatibility
	var batchStepTmpl *template.Template
	if sinkCfg.BatchSize > 0 {
		batchStepTmpl, err = compiler.NewTemplate("sink_oss_batch_step", fmt.Sprintf("[[ mul (div .__METADATA__record_index %d) %d ]]", sinkCfg.BatchSize, sinkCfg.BatchSize))
		if err != nil {
			return nil, fmt.Errorf("failed to parse batch step template: %w", err)
		}
	}

	// prepare handlers
	handlers, err := NewOSSHandler(commonSink.Context(), commonSink.Logger(),
		client, sinkCfg.EnableOverwrite,
		fs.WithConcurrentLimiter(commonSink),
		fs.WithWriteCompression(sinkCfg.CompressionType),
		fs.WithWriteCompressionStaticDestinationURI(sinkCfg.DestinationURI),
		fs.WithWriteCompressionPassword(sinkCfg.CompressionPassword),
		fs.WithWriteChunkOptions(
			xio.WithCSVSkipHeader(sinkCfg.SkipHeader),
			xio.WithCSVDelimiter(sinkCfg.CSVDelimiter),
		),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	o := &OSSSink{
		Sink:                   commonSink,
		destinationURITemplate: tmpl,
		handlers:               handlers,
		batchStepTemplate:      batchStepTmpl,
		// jsonPath selector
		jsonPathSelector: sinkCfg.JSONPathSelector,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		o.Logger().Info("closing writers")
		o.handlers.Close()
		return nil
	})

	// register sink process
	commonSink.RegisterProcess(o.process)

	return o, nil
}

func (o *OSSSink) process() error {
	recordCounter := 0
	for record, err := range o.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		if o.IsSpecializedMetadataRecord(record) {
			o.Logger().Debug("skip specialized metadata record")
			continue
		}

		destinationURI, err := compiler.Compile(o.destinationURITemplate, model.ToMap(record))
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to compile destination URI"))
			return errors.WithStack(err)
		}

		// TODO: deprecate this, we keep this for backward compatibility
		if o.batchStepTemplate != nil {
			l, r := fs.SplitExtension(destinationURI)
			batchStep, err := compiler.Compile(o.batchStepTemplate, model.ToMap(record))
			if err != nil {
				return errors.WithStack(err)
			}
			destinationURI = fmt.Sprintf("%s.%s", strings.TrimRight(destinationURI, l+r), batchStep) + l + r
		}

		// record without metadata
		recordWithoutMetadata := o.RecordWithoutMetadata(record)
		raw, err := json.MarshalWithOption(recordWithoutMetadata, json.DisableHTMLEscape())
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}
		// if jsonPathSelector is provided, select the data using it
		if o.jsonPathSelector != "" {
			raw, err = o.JSONPathSelector(raw, o.jsonPathSelector)
			if err != nil {
				o.Logger().Error(fmt.Sprintf("failed to select data using json path selector"))
				return errors.WithStack(err)
			}
		}

		err = o.DryRunable(func() error {
			if err := o.handlers.Write(destinationURI, append(raw, '\n')); err != nil {
				o.Logger().Error("failed to write to file")
				return errors.WithStack(err)
			}
			recordCounter++
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}

	}

	if recordCounter == 0 {
		o.Logger().Info(fmt.Sprintf("no records to write"))
		return nil
	}

	// flush all write handlers
	if err := o.DryRunable(o.handlers.Sync); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
