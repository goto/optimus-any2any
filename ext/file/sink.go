package file

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"text/template"

	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/helper"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

// FileSink is a sink that writes data to a file.
type FileSink struct {
	*common.CommonSink
	destinationURITemplate *template.Template
	fileHandlers           map[string]io.WriteCloser
	fileRecordCounters     map[string]int
}

func NewSink(l *slog.Logger, metadataPrefix string, destinationURI string, opts ...common.Option) (*FileSink, error) {
	// create commonSink
	commonSink := common.NewCommonSink(l, "file", metadataPrefix, opts...)

	// parse destinationURI as template
	tmpl, err := compiler.NewTemplate("sink_file_destination_uri", destinationURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}
	// create sink
	fs := &FileSink{
		CommonSink:             commonSink,
		destinationURITemplate: tmpl,
		fileHandlers:           make(map[string]io.WriteCloser),
		fileRecordCounters:     make(map[string]int),
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		fs.Logger().Info("close files")
		for _, fh := range fs.fileHandlers {
			fh.Close()
		}
		return nil
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(fs.process)

	return fs, nil
}

// process reads from the channel and writes to the file.
func (fs *FileSink) process() error {
	logCheckPoint := 1000
	recordCounter := 0
	for v := range fs.Read() {
		raw, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("invalid data type")
		}
		var record model.Record
		if err := json.Unmarshal(raw, &record); err != nil {
			return errors.WithStack(err)
		}
		destinationURI, err := compiler.Compile(fs.destinationURITemplate, model.ToMap(record))
		if err != nil {
			return errors.WithStack(err)
		}
		fh, ok := fs.fileHandlers[destinationURI]
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
			fh, err = NewStdFileHandler(fs.Logger(), targetURI.Path)
			if err != nil {
				fs.Logger().Error(fmt.Sprintf("failed to create file handler: %s", err.Error()))
				return errors.WithStack(err)
			}
			fs.fileHandlers[destinationURI] = fh
			fs.fileRecordCounters[destinationURI] = 0
		}

		recordWithoutMetadata := helper.RecordWithoutMetadata(record, fs.MetadataPrefix)
		raw, err = json.Marshal(recordWithoutMetadata)
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
		fs.fileRecordCounters[destinationURI]++
		if fs.fileRecordCounters[destinationURI]%logCheckPoint == 0 {
			fs.Logger().Info(fmt.Sprintf("written %d records to file: %s", fs.fileRecordCounters[destinationURI], destinationURI))
		}
	}
	fs.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))

	return nil
}
