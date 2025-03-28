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
	"github.com/goto/optimus-any2any/ext/common/model"
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
	skipHeader             bool
}

var _ flow.Sink = (*OSSSink)(nil)

// NewSink creates a new OSSSink
func NewSink(ctx context.Context, l *slog.Logger, metadataPrefix string,
	creds, destinationURI string,
	batchSize int, enableOverwrite bool, skipHeader bool,
	opts ...common.Option) (*OSSSink, error) {

	// create common sink
	commonSink := common.NewSink(l, metadataPrefix, opts...)
	commonSink.SetName("sink(oss)")

	// create OSS client
	client, err := NewOSSClient(creds)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse destinationURI as template
	tmpl, err := extcommon.NewTemplate("sink_oss_destination_uri", destinationURI)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to parse destination URI template: %w", commonSink.Name(), err)
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
		skipHeader:             skipHeader,
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		for destinationURI, oh := range ossSink.fileHandlers {
			commonSink.Logger.Info(fmt.Sprintf("%s: close file: %s", ossSink.Name(), destinationURI))
			_ = oh.Close()
		}
	})
	commonSink.AddCleanFunc(func() {
		for tmpPath, fh := range ossSink.fileHandlers {
			commonSink.Logger.Info(fmt.Sprintf("%s: close tmp file: %s", ossSink.Name(), tmpPath))
			_ = fh.Close()
			commonSink.Logger.Info(fmt.Sprintf("%s: remove tmp file: %s", ossSink.Name(), tmpPath))
			if err := os.Remove(tmpPath); err != nil {
				commonSink.Logger.Error(fmt.Sprintf("%s: failed to remove tmp file: %s", ossSink.Name(), tmpPath))
			}
		}
	})

	// register sink process
	commonSink.RegisterProcess(ossSink.process)

	return ossSink, nil
}

func (o *OSSSink) process() error {
	var destinationURI string
	var err error

	logCheckPoint := 1000
	recordCounter := 0
	if o.batchSize > 0 {
		logCheckPoint = o.batchSize
	}

	for msg := range o.Read() {
		b, ok := msg.([]byte)
		if !ok {
			o.Logger.Error(fmt.Sprintf("%s: message type assertion error: %T", o.Name(), msg))
			return errors.New(fmt.Sprintf("message type assertion error: %T", msg))
		}
		o.Logger.Debug(fmt.Sprintf("%s: received message: %s", o.Name(), string(b)))

		var record model.Record
		if err := json.Unmarshal(b, &record); err != nil {
			o.Logger.Error(fmt.Sprintf("%s: invalid data format", o.Name()))
			return errors.WithStack(err)
		}
		destinationURI, err = extcommon.Compile(o.destinationURITemplate, model.ToMap(record))
		if err != nil {
			o.Logger.Error(fmt.Sprintf("%s: failed to compile destination URI", o.Name()))
			return errors.WithStack(err)
		}

		if o.batchSize > 0 {
			// flush previous batch
			if recordCounter%o.batchSize == 0 && recordCounter > 0 {
				prevDestinationURI := getDestinationURIByBatch(destinationURI, recordCounter-1, o.batchSize)
				if err := o.flush(prevDestinationURI, o.ossHandlers[prevDestinationURI]); err != nil {
					o.Logger.Error(fmt.Sprintf("%s: failed to flush records: %s", o.Name(), err.Error()))
					return errors.WithStack(err)
				}
			}
			// use uri with batch size for its suffix if batch size is set
			destinationURI = getDestinationURIByBatch(destinationURI, recordCounter, o.batchSize)
		}

		// stream to tmp file
		tmpPath, err := getTmpPath(destinationURI)
		if err != nil {
			o.Logger.Error(fmt.Sprintf("%s: failed to get tmp URI: %s", o.Name(), destinationURI))
			return errors.WithStack(err)
		}
		fh, ok := o.fileHandlers[tmpPath]
		if !ok {
			// create new tmp file handler
			fh, err = file.NewStdFileHandler(o.Logger, tmpPath)
			if err != nil {
				o.Logger.Error(fmt.Sprintf("%s: failed to create tmp file handler: %s", o.Name(), err.Error()))
				return errors.WithStack(err)
			}

			// create new oss file handler
			targetDestinationURI, err := url.Parse(destinationURI)
			if err != nil {
				o.Logger.Error(fmt.Sprintf("%s: failed to parse destination URI: %s", o.Name(), destinationURI))
				return errors.WithStack(err)
			}
			if targetDestinationURI.Scheme != "oss" {
				o.Logger.Error(fmt.Sprintf("%s: invalid scheme: %s", o.Name(), targetDestinationURI.Scheme))
				return errors.WithStack(err)
			}
			// remove object if overwrite is enabled
			if _, ok := o.ossHandlers[destinationURI]; !ok && o.enableOverwrite {
				o.Logger.Info(fmt.Sprintf("%s: remove object: %s", o.Name(), destinationURI))
				if err := o.remove(targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/")); err != nil {
					o.Logger.Error(fmt.Sprintf("%s: failed to remove object: %s", o.Name(), destinationURI))
					return errors.WithStack(err)
				}
			}
			oh, err := oss.NewAppendFile(o.ctx, o.client, targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
			if err != nil {
				o.Logger.Error(fmt.Sprintf("%s: failed to create oss file handler: %s", o.Name(), err.Error()))
				return errors.WithStack(err)
			}

			// dual handlers for both tmp file and oss
			o.fileHandlers[tmpPath] = fh
			o.ossHandlers[destinationURI] = oh
		}

		// record without metadata
		recordWithoutMetadata := extcommon.RecordWithoutMetadata(record, o.MetadataPrefix)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			o.Logger.Error(fmt.Sprintf("%s: failed to marshal record", o.Name()))
			return errors.WithStack(err)
		}

		_, err = fh.Write(append(raw, '\n'))
		if err != nil {
			o.Logger.Error(fmt.Sprintf("%s: failed to write to file", o.Name()))
			return errors.WithStack(err)
		}

		recordCounter++
		o.fileRecordCounters[tmpPath]++
		if recordCounter%logCheckPoint == 0 {
			o.Logger.Info(fmt.Sprintf("%s: written %d records to tmp file: %s", o.Name(), o.fileRecordCounters[tmpPath], tmpPath))
		}
	}

	if recordCounter == 0 {
		o.Logger.Info(fmt.Sprintf("%s: no records to write", o.Name()))
		return nil
	}

	// flush remaining records
	if err := o.flush(destinationURI, o.ossHandlers[destinationURI]); err != nil {
		o.Logger.Error(fmt.Sprintf("%s: failed to flush records: %s", o.Name(), err.Error()))
		return errors.WithStack(err)
	}

	o.Logger.Info(fmt.Sprintf("%s: successfully written %d records", o.Name(), recordCounter))
	return nil
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
	o.Logger.Info(fmt.Sprintf("%s: delete %s objects", o.Name(), path))
	return nil
}

func (o *OSSSink) flush(destinationURI string, oh extcommon.FileHandler) error {
	tmpPath, err := getTmpPath(destinationURI)
	if err != nil {
		return errors.WithStack(err)
	}
	f, err := os.OpenFile(tmpPath, os.O_RDONLY, 0644)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()

	// convert to appropriate format if necessary
	var tmpReader io.ReadCloser
	switch filepath.Ext(destinationURI) {
	case ".json":
		tmpReader = f
	case ".csv":
		tmpReader = extcommon.FromJSONToCSV(o.Logger, f, o.skipHeader)
	case ".tsv":
		tmpReader = extcommon.FromJSONToCSV(o.Logger, f, o.skipHeader, rune('\t'))
	default:
		o.Logger.Warn(fmt.Sprintf("%s: unsupported file format: %s, use default (json)", o.Name(), filepath.Ext(destinationURI)))
		tmpReader = f
	}
	o.Logger.Info(fmt.Sprintf("%s: upload tmp file %s to oss %s", o.Name(), tmpPath, destinationURI))
	return o.Retry(func() error {
		_, err := io.Copy(oh, tmpReader)
		return err
	})
}

func getTmpPath(destinationURI string) (string, error) {
	targetURI, err := url.Parse(destinationURI)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return filepath.Join("/tmp", filepath.Base(targetURI.Path)), nil
}

func getDestinationURIByBatch(destinationURI string, recordCounter, batchSize int) string {
	return fmt.Sprintf("%s.%d.%s",
		destinationURI[:len(destinationURI)-len(filepath.Ext(destinationURI))],
		int(recordCounter/batchSize)*batchSize,
		filepath.Ext(destinationURI)[1:])
}
