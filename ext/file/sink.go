package file

import (
	"fmt"
	"text/template"

	"github.com/goccy/go-json"

	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/fs"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// FileSink is a sink that writes data to a file.
type FileSink struct {
	common.Sink
	destinationURITemplate *template.Template
	handlers               fs.WriteHandler
	// json path selector TODO: refactor this
	jsonPathSelector string
}

var _ flow.Sink = (*FileSink)(nil)

func NewSink(commonSink common.Sink, destinationURI string,
	compressionType string, compressionPassword string,
	jsonPathSelector string, opts ...common.Option) (*FileSink, error) {
	// parse destinationURI as template
	tmpl, err := compiler.NewTemplate("sink_file_destination_uri", destinationURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}

	// prepare handlers
	handlers, err := NewFileHandler(commonSink.Context(), commonSink.Logger(),
		fs.WithWriteConcurrentFunc(commonSink.ConcurrentTasks),
		fs.WithWriteCompression(compressionType),
		fs.WithWriteCompressionPassword(compressionPassword),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// create sink
	fs := &FileSink{
		Sink:                   commonSink,
		destinationURITemplate: tmpl,
		handlers:               handlers,
		jsonPathSelector:       jsonPathSelector,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		fs.Logger().Info("close files")
		fs.handlers.Close()
		return nil
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(fs.Process)

	return fs, nil
}

// Process reads from the channel and writes to the file.
func (fs *FileSink) Process() error {
	recordCounter := 0
	for record, err := range fs.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		if fs.IsSpecializedMetadataRecord(record) {
			fs.Logger().Debug("skip specialized metadata record")
			continue
		}

		destinationURI, err := compiler.Compile(fs.destinationURITemplate, model.ToMap(record))
		if err != nil {
			return errors.WithStack(err)
		}

		// record without metadata
		recordWithoutMetadata := fs.RecordWithoutMetadata(record)
		raw, err := json.MarshalWithOption(recordWithoutMetadata, json.DisableHTMLEscape())
		if err != nil {
			fs.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}
		// if jsonPathSelector is provided, select the data using it
		if fs.jsonPathSelector != "" {
			raw, err = fs.JSONPathSelector(raw, fs.jsonPathSelector)
			if err != nil {
				fs.Logger().Error(fmt.Sprintf("failed to select data using json path selector"))
				return errors.WithStack(err)
			}
		}

		// write to file
		err = fs.DryRunable(func() error {
			fs.Logger().Debug(fmt.Sprintf("write %s", string(raw)))
			if err := fs.handlers.Write(destinationURI, append(raw, '\n')); err != nil {
				fs.Logger().Error(fmt.Sprintf("failed to write to file"))
				return errors.WithStack(err)
			}
			recordCounter++
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// flush all write handlers
	return errors.WithStack(fs.DryRunable(fs.handlers.Sync))
}
