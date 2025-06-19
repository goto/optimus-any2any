package oss

import (
	"fmt"
	"text/template"

	"github.com/goccy/go-json"

	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/fs"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type OSSSink struct {
	common.Sink

	client                 *Client
	destinationURITemplate *template.Template
	handlers               fs.WriteHandler
}

var _ flow.Sink = (*OSSSink)(nil)

// NewSink creates a new OSSSink
func NewSink(commonSink common.Sink,
	creds, destinationURI string,
	batchSize int, enableOverwrite bool, skipHeader bool,
	maxTempFileRecordNumber int,
	compressionType string, compressionPassword string,
	connectionTimeout, readWriteTimeout int,
	opts ...common.Option) (*OSSSink, error) {

	// create OSS client
	client, err := NewOSSClient(commonSink.Context(), creds, OSSClientConfig{
		ConnectionTimeoutSeconds: connectionTimeout,
		ReadWriteTimeoutSeconds:  readWriteTimeout,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse destinationURI as template
	tmpl, err := compiler.NewTemplate("sink_oss_destination_uri", destinationURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}

	// prepare handlers
	handlers, err := NewOSSHandler(commonSink.Context(), commonSink.Logger(),
		client, enableOverwrite,
		fs.WithWriteConcurrentFunc(commonSink.ConcurrentTasks),
		fs.WithWriteCompression(compressionType),
		fs.WithWriteCompressionPassword(compressionPassword),
		fs.WithWriteChunkOptions(xio.WithCSVSkipHeader(skipHeader)),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	o := &OSSSink{
		Sink:                   commonSink,
		client:                 client,
		destinationURITemplate: tmpl,
		handlers:               handlers,
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

		// TODO: batch splitting by using templating

		// record without metadata
		recordWithoutMetadata := o.RecordWithoutMetadata(record)
		raw, err := json.MarshalWithOption(recordWithoutMetadata, json.DisableHTMLEscape())
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
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

	_ = o.DryRunable(func() error { // ignore log when dry run
		o.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))
		return nil
	})
	return nil
}
