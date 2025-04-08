package oss

import (
	"context"
	errs "errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/goccy/go-json"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/helper"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type OSSSink struct {
	*common.CommonSink
	ctx context.Context

	client                 *oss.Client
	destinationURITemplate *template.Template
	fileHandlers           map[string]io.WriteCloser // tmp file handler
	ossHandlers            map[string]io.WriteCloser
	fileRecordCounters     map[string]int
	batchSize              int
	enableOverwrite        bool
	skipHeader             bool
}

var _ flow.Sink = (*OSSSink)(nil)

// NewSink creates a new OSSSink
func NewSink(ctx context.Context, l *slog.Logger,
	creds, destinationURI string,
	batchSize int, enableOverwrite bool, skipHeader bool,
	opts ...common.Option) (*OSSSink, error) {

	// create common sink
	commonSink := common.NewCommonSink(l, "oss", opts...)

	// create OSS client
	client, err := NewOSSClient(creds)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse destinationURI as template
	tmpl, err := compiler.NewTemplate("sink_oss_destination_uri", destinationURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}

	o := &OSSSink{
		CommonSink:             commonSink,
		ctx:                    ctx,
		client:                 client,
		destinationURITemplate: tmpl,
		fileHandlers:           make(map[string]io.WriteCloser),
		ossHandlers:            make(map[string]io.WriteCloser),
		fileRecordCounters:     make(map[string]int),
		batchSize:              batchSize,
		enableOverwrite:        enableOverwrite,
		skipHeader:             skipHeader,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		o.Logger().Info("close oss files")
		var e error
		for destinationURI, oh := range o.fileHandlers {
			o.Logger().Debug(fmt.Sprintf("close file: %s", destinationURI))
			err := oh.Close()
			e = errs.Join(e, err)
		}
		return e
	})
	commonSink.AddCleanFunc(func() error {
		o.Logger().Info("close & remove tmp files")
		var e error
		for tmpPath, fh := range o.fileHandlers {
			o.Logger().Debug(fmt.Sprintf("close tmp file: %s", tmpPath))
			fh.Close()

			o.Logger().Debug(fmt.Sprintf("remove tmp file: %s", tmpPath))
			err := os.Remove(tmpPath)
			e = errs.Join(e, err)
		}
		return e
	})

	// register sink process
	commonSink.RegisterProcess(o.process)

	return o, nil
}

func (o *OSSSink) process() error {
	var destinationURI string

	logCheckPoint := 1000
	recordCounter := 0
	if o.batchSize > 0 {
		logCheckPoint = o.batchSize
	}

	for record, err := range o.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		destinationURI, err = compiler.Compile(o.destinationURITemplate, model.ToMap(record))
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to compile destination URI"))
			return errors.WithStack(err)
		}

		if o.batchSize > 0 {
			// flush previous batch
			if recordCounter%o.batchSize == 0 && recordCounter > 0 {
				prevDestinationURI := getDestinationURIByBatch(destinationURI, recordCounter-1, o.batchSize)
				if err := o.flush(prevDestinationURI, o.ossHandlers[prevDestinationURI]); err != nil {
					o.Logger().Error(fmt.Sprintf("failed to flush records: %s", err.Error()))
					return errors.WithStack(err)
				}
			}
			// use uri with batch size for its suffix if batch size is set
			destinationURI = getDestinationURIByBatch(destinationURI, recordCounter, o.batchSize)
		}

		// stream to tmp file
		tmpPath, err := getTmpPath(destinationURI)
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to get tmp URI: %s", destinationURI))
			return errors.WithStack(err)
		}
		fh, ok := o.fileHandlers[tmpPath]
		if !ok {
			// create new tmp file handler
			fh, err = xio.NewWriteHandler(o.Logger(), tmpPath)
			if err != nil {
				o.Logger().Error(fmt.Sprintf("failed to create tmp file handler: %s", err.Error()))
				return errors.WithStack(err)
			}

			// create new oss file handler
			targetDestinationURI, err := url.Parse(destinationURI)
			if err != nil {
				o.Logger().Error(fmt.Sprintf("failed to parse destination URI: %s", destinationURI))
				return errors.WithStack(err)
			}
			if targetDestinationURI.Scheme != "oss" {
				o.Logger().Error(fmt.Sprintf("invalid scheme: %s", targetDestinationURI.Scheme))
				return errors.WithStack(err)
			}
			// remove object if overwrite is enabled
			if _, ok := o.ossHandlers[destinationURI]; !ok && o.enableOverwrite {
				o.Logger().Info(fmt.Sprintf("remove object: %s", destinationURI))
				if err := o.remove(targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/")); err != nil {
					o.Logger().Error(fmt.Sprintf("failed to remove object: %s", destinationURI))
					return errors.WithStack(err)
				}
			}
			oh, err := oss.NewAppendFile(o.ctx, o.client, targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
			if err != nil {
				o.Logger().Error(fmt.Sprintf("failed to create oss file handler: %s", err.Error()))
				return errors.WithStack(err)
			}

			// dual handlers for both tmp file and oss
			o.fileHandlers[tmpPath] = fh
			o.ossHandlers[destinationURI] = oh
		}

		// record without metadata
		recordWithoutMetadata := o.RecordWithoutMetadata(record)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}

		_, err = fh.Write(append(raw, '\n'))
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to write to file"))
			return errors.WithStack(err)
		}

		recordCounter++
		o.fileRecordCounters[tmpPath]++
		if recordCounter%logCheckPoint == 0 {
			o.Logger().Info(fmt.Sprintf("written %d records to tmp file: %s", o.fileRecordCounters[tmpPath], tmpPath))
		}
	}

	if recordCounter == 0 {
		o.Logger().Info(fmt.Sprintf("no records to write"))
		return nil
	}

	// flush remaining records
	if err := o.flush(destinationURI, o.ossHandlers[destinationURI]); err != nil {
		o.Logger().Error(fmt.Sprintf("failed to flush records: %s", err.Error()))
		return errors.WithStack(err)
	}

	o.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))
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
	o.Logger().Info(fmt.Sprintf("delete %s objects", path))
	return nil
}

func (o *OSSSink) flush(destinationURI string, oh io.WriteCloser) error {
	tmpPath, err := getTmpPath(destinationURI)
	if err != nil {
		return errors.WithStack(err)
	}
	// flush tmp file first by closing the writer
	fh, ok := o.fileHandlers[tmpPath]
	if !ok {
		return errors.New(fmt.Sprintf("tmp file handler not found: %s", tmpPath))
	}
	if err := fh.Close(); err != nil {
		return errors.WithStack(err)
	}
	// open tmp file for reading
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
		tmpReader = helper.FromJSONToCSV(o.Logger(), f, o.skipHeader)
	case ".tsv":
		tmpReader = helper.FromJSONToCSV(o.Logger(), f, o.skipHeader, rune('\t'))
	default:
		o.Logger().Warn(fmt.Sprintf("unsupported file format: %s, use default (json)", filepath.Ext(destinationURI)))
		tmpReader = f
	}
	o.Logger().Info(fmt.Sprintf("upload tmp file %s to oss %s", tmpPath, destinationURI))
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
