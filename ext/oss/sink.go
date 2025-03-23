package oss

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type OSSSink struct {
	*common.Sink
	ctx context.Context

	client                 *oss.Client
	destinationURITemplate *template.Template
	fileHandlers           map[string]extcommon.FileHandler // tmp file handler
	ossHandlers            map[string]extcommon.FileHandler
	fileRecordCounters     map[string]int
	batchSize              int
	enableOverwrite        bool
}

var _ flow.Sink = (*OSSSink)(nil)

// NewSink creates a new OSSSink
func NewSink(ctx context.Context, l *slog.Logger, metadataPrefix string,
	creds, destinationURI string,
	batchSize int, enableOverwrite bool,
	opts ...common.Option) (*OSSSink, error) {

	// create common sink
	commonSink := common.NewSink(l, metadataPrefix, opts...)

	// create OSS client
	client, err := NewOSSClient(creds)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse destinationURI as template
	tmpl, err := extcommon.NewTemplate("sink_oss_destination_uri", destinationURI)
	if err != nil {
		return nil, fmt.Errorf("sink(oss): failed to parse destination URI template: %w", err)
	}

	ossSink := &OSSSink{
		Sink:                   commonSink,
		ctx:                    ctx,
		client:                 client,
		destinationURITemplate: tmpl,
		fileHandlers:           make(map[string]extcommon.FileHandler),
		ossHandlers:            make(map[string]extcommon.FileHandler),
		fileRecordCounters:     make(map[string]int),
		batchSize:              batchSize,
		enableOverwrite:        enableOverwrite,
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		for destinationURI, oh := range ossSink.fileHandlers {
			commonSink.Logger.Info(fmt.Sprintf("sink(oss): close file: %s", destinationURI))
			_ = oh.Close()
		}
		for tmpURI, fh := range ossSink.fileHandlers {
			targetTmpURI, err := url.Parse(tmpURI)
			if err != nil {
				commonSink.Logger.Error(fmt.Sprintf("sink(oss): failed to parse tmp URI: %s", tmpURI))
				continue
			}
			commonSink.Logger.Info(fmt.Sprintf("sink(oss): remove tmp file: %s", tmpURI))
			if err := os.Remove(targetTmpURI.Path); err != nil {
				commonSink.Logger.Error(fmt.Sprintf("sink(oss): failed to remove tmp file: %s", tmpURI))
			}
			commonSink.Logger.Info(fmt.Sprintf("sink(oss): close tmp file: %s", tmpURI))
			_ = fh.Close()
		}
	})

	// register sink process
	commonSink.RegisterProcess(ossSink.process)

	return ossSink, nil
}

func (o *OSSSink) process() {
	logCheckPoint := 1000
	recordCounter := 0
	if o.batchSize > 0 {
		logCheckPoint = o.batchSize
	}

	for msg := range o.Read() {
		if o.Err() != nil {
			continue
		}
		b, ok := msg.([]byte)
		if !ok {
			o.Logger.Error(fmt.Sprintf("sink(oss): message type assertion error: %T", msg))
			o.SetError(errors.New(fmt.Sprintf("sink(oss): message type assertion error: %T", msg)))
			continue
		}
		o.Logger.Debug(fmt.Sprintf("sink(oss): received message: %s", string(b)))

		var record map[string]interface{}
		if err := json.Unmarshal(b, &record); err != nil {
			o.Logger.Error("sink(oss): invalid data format")
			o.SetError(errors.WithStack(err))
			continue
		}
		destinationURI, err := extcommon.Compile(o.destinationURITemplate, record)
		if err != nil {
			o.Logger.Error("sink(oss): failed to compile destination URI")
			o.SetError(errors.WithStack(err))
			continue
		}
		// use uri with batch size for its suffix if batch size is set
		if o.batchSize > 0 {
			destinationURI = fmt.Sprintf("%s.%d.%s",
				destinationURI[:len(destinationURI)-len(filepath.Ext(destinationURI))],
				int(recordCounter/o.batchSize)*o.batchSize,
				filepath.Ext(destinationURI)[1:])
		}

		// stream to tmp file
		tmpURI, err := getTmpURI(destinationURI)
		if err != nil {
			o.Logger.Error(fmt.Sprintf("sink(oss): failed to get tmp URI: %s", destinationURI))
			o.SetError(errors.WithStack(err))
			continue
		}
		fh, ok := o.fileHandlers[tmpURI]
		if !ok {
			// create new tmp file handler
			targetTempURI, err := url.Parse(tmpURI)
			if err != nil {
				o.Logger.Error(fmt.Sprintf("sink(oss): failed to parse tmp URI: %s", tmpURI))
				o.SetError(errors.WithStack(err))
				continue
			}
			fh, err = file.NewStdFileHandler(o.Logger, targetTempURI.Path)
			if err != nil {
				o.Logger.Error(fmt.Sprintf("sink(oss): failed to create tmp file handler: %s", err.Error()))
				o.SetError(errors.WithStack(err))
				continue
			}

			// create new oss file handler
			targetDestinationURI, err := url.Parse(destinationURI)
			if err != nil {
				o.Logger.Error(fmt.Sprintf("sink(oss): failed to parse destination URI: %s", destinationURI))
				o.SetError(errors.WithStack(err))
				continue
			}
			if targetDestinationURI.Scheme != "oss" {
				o.Logger.Error(fmt.Sprintf("sink(oss): invalid scheme: %s", targetDestinationURI.Scheme))
				o.SetError(errors.WithStack(err))
				continue
			}
			// remove object if overwrite is enabled
			if _, ok := o.ossHandlers[destinationURI]; !ok && o.enableOverwrite {
				o.Logger.Info(fmt.Sprintf("sink(oss): remove object: %s", destinationURI))
				if err := o.remove(targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/")); err != nil {
					o.Logger.Error(fmt.Sprintf("sink(oss): failed to remove object: %s", destinationURI))
					o.SetError(errors.WithStack(err))
					continue
				}
			}
			oh, err := oss.NewAppendFile(o.ctx, o.client, targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
			if err != nil {
				o.Logger.Error(fmt.Sprintf("sink(oss): failed to create oss file handler: %s", err.Error()))
				o.SetError(errors.WithStack(err))
				continue
			}

			// dual handlers for both tmp file and oss
			o.fileHandlers[tmpURI] = fh
			o.ossHandlers[destinationURI] = oh
		}
		_, err = fh.Write(append(b, '\n'))
		if err != nil {
			o.Logger.Error("sink(oss): failed to write to file")
			o.SetError(errors.WithStack(err))
			continue
		}

		recordCounter++
		o.fileRecordCounters[tmpURI]++
		if recordCounter%logCheckPoint == 0 {
			o.Logger.Info(fmt.Sprintf("sink(oss): written %d records to tmp file: %s", o.fileRecordCounters[tmpURI], tmpURI))
		}
	}

	// upload tmp file to oss
	for destinationURI, oh := range o.ossHandlers {
		tmpURI, err := getTmpURI(destinationURI)
		if err != nil {
			o.Logger.Error(fmt.Sprintf("sink(oss): failed to get tmp URI: %s", destinationURI))
			o.SetError(errors.WithStack(err))
			return
		}
		sourceURI, err := url.Parse(tmpURI)
		if err != nil {
			o.Logger.Error(fmt.Sprintf("sink(oss): failed to parse tmp URI: %s", tmpURI))
			o.SetError(errors.WithStack(err))
			return
		}
		f, err := os.OpenFile(sourceURI.Path, os.O_RDONLY, 0644)
		if err != nil {
			o.Logger.Error(fmt.Sprintf("sink(oss): failed to open tmp file: %s", tmpURI))
			o.SetError(errors.WithStack(err))
			return
		}
		defer f.Close()

		// convert to appropriate format if necessary
		var tmpReader io.Reader
		switch filepath.Ext(destinationURI) {
		case ".json":
			tmpReader = f
		case ".csv":
			tmpReader = extcommon.FromJSONToCSV(o.Logger, f)
		case ".tsv":
			tmpReader = extcommon.FromJSONToCSV(o.Logger, f, rune('\t'))
		default:
			o.Logger.Warn(fmt.Sprintf("sink(oss): unsupported file format: %s, use default (json)", filepath.Ext(destinationURI)))
			tmpReader = f
		}
		o.Logger.Info(fmt.Sprintf("sink(oss): upload tmp file %s to oss %s", tmpURI, destinationURI))
		if _, err := io.Copy(oh, tmpReader); err != nil {
			o.Logger.Error(fmt.Sprintf("sink(oss): failed to upload tmp file to oss: %s", destinationURI))
			o.SetError(errors.WithStack(err))
			return
		}
	}
	o.Logger.Info(fmt.Sprintf("sink(oss): successfully written %d records", recordCounter))
}

func (o *OSSSink) remove(bucket, path string) error {
	response, err := o.client.DeleteObject(o.ctx, &oss.DeleteObjectRequest{
		Bucket: oss.Ptr(bucket),
		Key:    oss.Ptr(path),
	})
	if err != nil {
		return errors.WithStack(err)
	}
	if response.StatusCode >= 400 {
		err := errors.New(fmt.Sprintf("failed to delete object: %d", response.StatusCode))
		return errors.WithStack(err)
	}
	o.Logger.Info(fmt.Sprintf("sink(oss): delete %s objects", path))
	return nil
}

func getTmpURI(destinationURI string) (string, error) {
	targetURI, err := url.Parse(destinationURI)
	if err != nil {
		return "", errors.WithStack(err)
	}
	filepath.Base(targetURI.Path)
	return fmt.Sprintf("file:///tmp/%s", filepath.Base(targetURI.Path)), nil
}
