package file

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"text/template"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/pkg/errors"
)

// FileSink is a sink that writes data to a file.
type FileSink struct {
	*common.Sink
	destinationURITemplate *template.Template
	fileHandlers           map[string]extcommon.FileHandler
}

func NewSink(l *slog.Logger, metadataPrefix string, destinationURI string, opts ...common.Option) (*FileSink, error) {
	// create commonSink
	commonSink := common.NewSink(l, metadataPrefix, opts...)
	// parse destinationURI as template
	tmpl, err := extcommon.NewTemplate("sink_file_destination_uri", destinationURI)
	if err != nil {
		return nil, fmt.Errorf("sink(file): failed to parse destination URI template: %w", err)
	}
	// create sink
	fs := &FileSink{
		Sink:                   commonSink,
		destinationURITemplate: tmpl,
		fileHandlers:           make(map[string]extcommon.FileHandler),
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		commonSink.Logger.Debug("sink(file): close file")
		for _, fh := range fs.fileHandlers {
			_ = fh.Close()
		}
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(fs.process)

	return fs, nil
}

// process reads from the channel and writes to the file.
func (fs *FileSink) process() {
	for v := range fs.Read() {
		raw, ok := v.([]byte)
		if !ok {
			fs.Logger.Error("sink(file): invalid data type")
			fs.SetError(fmt.Errorf("invalid data type"))
			continue
		}
		var record map[string]interface{}
		if err := json.Unmarshal(raw, &record); err != nil {
			fs.Logger.Error("sink(file): invalid data format")
			fs.SetError(errors.WithStack(err))
			continue
		}
		destinationURI, err := extcommon.Compile(fs.destinationURITemplate, record)
		if err != nil {
			fs.Logger.Error("sink(file): failed to compile destination URI")
			fs.SetError(errors.WithStack(err))
			continue
		}
		fh, ok := fs.fileHandlers[destinationURI]
		if !ok {
			fs.Logger.Debug(fmt.Sprintf("sink(file): create new file handler: %s", destinationURI))
			targetURI, err := url.Parse(destinationURI)
			if err != nil {
				fs.Logger.Error(fmt.Sprintf("sink(file): failed to parse destination URI: %s", destinationURI))
				fs.SetError(errors.WithStack(err))
				continue
			}
			if targetURI.Scheme != "file" {
				fs.Logger.Error(fmt.Sprintf("sink(file): invalid scheme: %s", targetURI.Scheme))
				fs.SetError(fmt.Errorf("invalid scheme"))
				continue
			}
			fh, err = NewStdFileHandler(fs.Logger, targetURI.Path)
			if err != nil {
				fs.Logger.Error("sink(file): failed to create file handler")
				fs.SetError(errors.WithStack(err))
				continue
			}
			fs.fileHandlers[destinationURI] = fh
		}

		recordWithoutMetadata := extcommon.RecordWithoutMetadata(record, fs.MetadataPrefix)
		raw, err = json.Marshal(recordWithoutMetadata)
		if err != nil {
			fs.Logger.Error("sink(file): failed to marshal record")
			fs.SetError(errors.WithStack(err))
			continue
		}

		fs.Logger.Debug(fmt.Sprintf("sink(file): write %s", string(raw)))
		_, err = fh.Write(append(raw, '\n'))
		if err != nil {
			fs.Logger.Error("sink(file): failed to write to file")
			fs.SetError(errors.WithStack(err))
			continue
		}
	}
	for uri, fh := range fs.fileHandlers {
		if err := fh.Flush(); err != nil {
			fs.Logger.Error(fmt.Sprintf("sink(file): failed to flush file: %s", uri))
			fs.SetError(fmt.Errorf("failed to flush file: %s", uri))
		}
	}
}
