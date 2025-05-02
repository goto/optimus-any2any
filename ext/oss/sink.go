package oss

import (
	errs "errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/goccy/go-json"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type OSSSink struct {
	common.Sink

	client                  *oss.Client
	destinationURITemplate  *template.Template
	writeHandlers           map[string]xio.WriteFlushCloser
	fileRecordCounters      map[string]int
	batchSize               int
	enableOverwrite         bool
	skipHeader              bool
	maxTempFileRecordNumber int
}

var _ flow.Sink = (*OSSSink)(nil)

// NewSink creates a new OSSSink
func NewSink(commonSink common.Sink,
	creds, destinationURI string,
	batchSize int, enableOverwrite bool, skipHeader bool,
	maxTempFileRecordNumber int,
	opts ...common.Option) (*OSSSink, error) {

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
		Sink:                    commonSink,
		client:                  client,
		destinationURITemplate:  tmpl,
		writeHandlers:           make(map[string]xio.WriteFlushCloser),
		fileRecordCounters:      make(map[string]int),
		batchSize:               batchSize,
		enableOverwrite:         enableOverwrite,
		skipHeader:              skipHeader,
		maxTempFileRecordNumber: maxTempFileRecordNumber,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		o.Logger().Info("closing oss writers")
		var e error
		for destURI, wh := range o.writeHandlers {
			o.Logger().Debug(fmt.Sprintf("close handlers: %s", destURI))
			err := o.DryRunable(wh.Close)
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
			// use uri with batch size for its suffix if batch size is set
			destinationURI = getDestinationURIByBatch(destinationURI, recordCounter, o.batchSize)
		}

		// create file write handlers
		wh, ok := o.writeHandlers[destinationURI]
		if !ok {
			// create new oss write handler
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
			if o.enableOverwrite {
				o.Logger().Info(fmt.Sprintf("overwrite is enabled, remove object: %s", destinationURI))
				if err := o.Retry(func() error {
					err := o.remove(targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
					return err
				}); err != nil {
					o.Logger().Error(fmt.Sprintf("failed to remove object: %s", destinationURI))
					return errors.WithStack(err)
				}
			}
			var oh io.WriteCloser
			if err := o.Retry(func() (err error) {
				oh, err = oss.NewAppendFile(o.Context(), o.client, targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
				return
			}); err != nil {
				o.Logger().Error(fmt.Sprintf("failed to create oss write handler: %s", err.Error()))
				return errors.WithStack(err)
			}

			wh = xio.NewChunkWriter(
				o.Logger(), oh,
				xio.WithExtension(filepath.Ext(destinationURI)),
				xio.WithCSVSkipHeader(o.skipHeader),
			)
			o.writeHandlers[destinationURI] = wh
		}

		// record without metadata
		recordWithoutMetadata := o.RecordWithoutMetadata(record)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}

		err = o.DryRunable(func() error {
			_, err = wh.Write(append(raw, '\n'))
			if err != nil {
				o.Logger().Error(fmt.Sprintf("failed to write to file"))
				return errors.WithStack(err)
			}

			recordCounter++
			o.fileRecordCounters[destinationURI]++
			if recordCounter%logCheckPoint == 0 {
				o.Logger().Info(fmt.Sprintf("written %d records to file writer: %s", o.fileRecordCounters[destinationURI], destinationURI))
			}
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

	// flush remaining records concurrently
	funcs := []func() error{}
	for destinationURI, wh := range o.writeHandlers {
		funcs = append(funcs, func() error {
			err := o.DryRunable(func() error {
				if err := wh.Flush(); err != nil {
					o.Logger().Error(fmt.Sprintf("failed to flush to %s", destinationURI))
					return errors.WithStack(err)
				}
				o.Logger().Info(fmt.Sprintf("flushed file: %s", destinationURI))
				return nil
			})
			return errors.WithStack(err)
		})
	}
	if err := o.ConcurrentTasks(o.Context(), 4, funcs); err != nil {
		return errors.WithStack(err)
	}

	_ = o.DryRunable(func() error { // ignore log when dry run
		o.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))
		return nil
	})
	return nil
}

func (o *OSSSink) remove(bucket, path string) error {
	var response *oss.DeleteObjectResult
	var err error
	// remove object
	err = o.DryRunable(func() error {
		response, err = o.client.DeleteObject(o.Context(), &oss.DeleteObjectRequest{
			Bucket: oss.Ptr(bucket),
			Key:    oss.Ptr(path),
		})
		return errors.WithStack(err)
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

func getDestinationURIByBatch(destinationURI string, recordCounter, batchSize int) string {
	return fmt.Sprintf("%s.%d.%s",
		destinationURI[:len(destinationURI)-len(filepath.Ext(destinationURI))],
		int(recordCounter/batchSize)*batchSize,
		filepath.Ext(destinationURI)[1:])
}
