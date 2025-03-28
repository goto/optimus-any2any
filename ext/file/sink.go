package file

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"text/template"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/ext/common/model"
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
	commonSink.SetName("sink(file)")
	// parse destinationURI as template
	tmpl, err := extcommon.NewTemplate("sink_file_destination_uri", destinationURI)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to parse destination URI template: %w", commonSink.Name(), err)
	}
	// create sink
	fs := &FileSink{
		Sink:                   commonSink,
		destinationURITemplate: tmpl,
		fileHandlers:           make(map[string]extcommon.FileHandler),
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		commonSink.Logger.Info(fmt.Sprintf("%s: close files", commonSink.Name()))
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
func (fs *FileSink) process() error {
	for v := range fs.Read() {
		raw, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("%s: invalid data type", fs.Name())
		}
		var record model.Record
		if err := json.Unmarshal(raw, &record); err != nil {
			return errors.WithStack(err)
		}
		destinationURI, err := extcommon.Compile(fs.destinationURITemplate, model.ToMap(record))
		if err != nil {
			return errors.WithStack(err)
		}
		fh, ok := fs.fileHandlers[destinationURI]
		if !ok {
			fs.Logger.Debug(fmt.Sprintf("%s: create new file handler: %s", fs.Name(), destinationURI))
			targetURI, err := url.Parse(destinationURI)
			if err != nil {
				fs.Logger.Error(fmt.Sprintf("%s: failed to parse destination URI: %s", fs.Name(), destinationURI))
				return errors.WithStack(err)
			}
			if targetURI.Scheme != "file" {
				fs.Logger.Error(fmt.Sprintf("%s: invalid scheme: %s", fs.Name(), targetURI.Scheme))
				return fmt.Errorf("invalid scheme: %s", targetURI.Scheme)
			}
			fh, err = NewStdFileHandler(fs.Logger, targetURI.Path)
			if err != nil {
				fs.Logger.Error(fmt.Sprintf("%s: failed to create file handler: %s", fs.Name(), err.Error()))
				return errors.WithStack(err)
			}
			fs.fileHandlers[destinationURI] = fh
		}

		recordWithoutMetadata := extcommon.RecordWithoutMetadata(record, fs.MetadataPrefix)
		raw, err = json.Marshal(recordWithoutMetadata)
		if err != nil {
			fs.Logger.Error(fmt.Sprintf("%s: failed to marshal record", fs.Name()))
			return errors.WithStack(err)
		}

		fs.Logger.Debug(fmt.Sprintf("%s: write %s", fs.Name(), string(raw)))
		_, err = fh.Write(append(raw, '\n'))
		if err != nil {
			fs.Logger.Error(fmt.Sprintf("%s: failed to write to file", fs.Name()))
			return errors.WithStack(err)
		}
	}

	return nil
}
