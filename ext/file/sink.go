package file

import (
	"fmt"
	"io"
	"net/url"
	"text/template"

	"github.com/goccy/go-json"

	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// FileSink is a sink that writes data to a file.
type FileSink struct {
	flow.Sink
	component.Getter
	common.RecordReader
	common.RecordHelper
	DestinationURITemplate *template.Template
	WriterFactory          func(string) (io.WriteCloser, error)
	WriteHandlers          map[string]io.WriteCloser
	FileRecordCounters     map[string]int
}

var _ flow.Sink = (*FileSink)(nil)

func NewSink(commonSink *common.CommonSink, destinationURI string, opts ...common.Option) (*FileSink, error) {
	// parse destinationURI as template
	tmpl, err := compiler.NewTemplate("sink_file_destination_uri", destinationURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}
	// create sink
	fs := &FileSink{
		Sink:                   commonSink,
		Getter:                 commonSink,
		RecordReader:           commonSink,
		RecordHelper:           commonSink,
		DestinationURITemplate: tmpl,
		WriterFactory: func(s string) (io.WriteCloser, error) {
			return xio.NewWriteHandler(commonSink.Logger(), s)
		},
		WriteHandlers:      make(map[string]io.WriteCloser),
		FileRecordCounters: make(map[string]int),
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		fs.Logger().Info("close files")
		for _, fh := range fs.WriteHandlers {
			fh.Close()
		}
		return nil
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(fs.Process)

	return fs, nil
}

// Process reads from the channel and writes to the file.
func (fs *FileSink) Process() error {
	logCheckPoint := 1000
	recordCounter := 0
	for record, err := range fs.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		destinationURI, err := compiler.Compile(fs.DestinationURITemplate, model.ToMap(record))
		if err != nil {
			return errors.WithStack(err)
		}
		fh, ok := fs.WriteHandlers[destinationURI]
		if !ok {
			fs.Logger().Debug(fmt.Sprintf("create new file handler: %s", destinationURI))
			targetURI, err := url.Parse(destinationURI)
			if err != nil {
				fs.Logger().Error(fmt.Sprintf("failed to parse destination URI: %s", destinationURI))
				return errors.WithStack(err)
			}
			if targetURI.Scheme != "file" {
				fs.Logger().Error(fmt.Sprintf("invalid scheme: %s", targetURI.Scheme))
				return fmt.Errorf("invalid scheme: %s", targetURI.Scheme)
			}
			fh, err = fs.WriterFactory(targetURI.Path)
			if err != nil {
				fs.Logger().Error(fmt.Sprintf("failed to create file handler: %s", err.Error()))
				return errors.WithStack(err)
			}
			fs.WriteHandlers[destinationURI] = fh
			fs.FileRecordCounters[destinationURI] = 0
		}

		recordWithoutMetadata := fs.RecordWithoutMetadata(record)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			fs.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}

		fs.Logger().Debug(fmt.Sprintf("write %s", string(raw)))
		_, err = fh.Write(append(raw, '\n'))
		if err != nil {
			fs.Logger().Error(fmt.Sprintf("failed to write to file"))
			return errors.WithStack(err)
		}
		recordCounter++
		fs.FileRecordCounters[destinationURI]++
		if fs.FileRecordCounters[destinationURI]%logCheckPoint == 0 {
			fs.Logger().Info(fmt.Sprintf("written %d records to file: %s", fs.FileRecordCounters[destinationURI], destinationURI))
		}
	}
	// close all file handlers
	for _, fh := range fs.WriteHandlers {
		if err := fh.Close(); err != nil {
			fs.Logger().Error(fmt.Sprintf("failed to close file handler: %s", err.Error()))
			return errors.WithStack(err)
		}
	}
	fs.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))

	return nil
}
