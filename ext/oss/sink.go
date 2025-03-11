package oss

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type OSSSink struct {
	*common.Sink
	ctx context.Context

	client                 *oss.Client
	destinationURITemplate *template.Template
	fileHandlers           map[string]extcommon.FileHandler
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
		fileRecordCounters:     make(map[string]int),
		batchSize:              batchSize,
		enableOverwrite:        enableOverwrite,
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		commonSink.Logger.Debug("sink(oss): close record writer")
		for _, fh := range ossSink.fileHandlers {
			_ = fh.Close()
		}
	})

	// register sink process
	commonSink.RegisterProcess(ossSink.process)

	return ossSink, nil
}

func (o *OSSSink) process() {
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
		recordCounter := o.fileRecordCounters[destinationURI]
		// use uri with batch size for its suffix if batch size is set
		if o.batchSize > 0 && recordCounter%o.batchSize == 0 {
			destinationURI = fmt.Sprintf("%s.%d.%s",
				destinationURI[:len(destinationURI)-len(filepath.Ext(destinationURI))],
				int(recordCounter/o.batchSize)*o.batchSize,
				filepath.Ext(destinationURI))
		}
		fh, ok := o.fileHandlers[destinationURI]
		if !ok {
			targetURI, err := url.Parse(destinationURI)
			if err != nil {
				o.Logger.Error(fmt.Sprintf("sink(oss): failed to parse destination URI: %s", destinationURI))
				o.SetError(errors.WithStack(err))
				continue
			}
			if targetURI.Scheme != "oss" {
				o.Logger.Error(fmt.Sprintf("sink(oss): invalid scheme: %s", targetURI.Scheme))
				o.SetError(errors.WithStack(err))
				continue
			}
			if o.enableOverwrite {
				if err := o.remove(targetURI.Host, targetURI.Path); err != nil {
					o.Logger.Error(fmt.Sprintf("sink(oss): failed to remove object: %s", destinationURI))
					o.SetError(errors.WithStack(err))
					continue
				}
			}
			fh, err = NewOSSFileHandler(o.ctx, o.Logger, o.client, targetURI.Host, strings.TrimLeft(targetURI.Path, "/"))
			if err != nil {
				o.Logger.Error("sink(oss): failed to create file handler")
				o.SetError(errors.WithStack(err))
				continue
			}
			o.Logger.Info(fmt.Sprintf("sink(oss): created file: %s", destinationURI))
			o.fileHandlers[destinationURI] = fh
		}
		_, err = fh.Write(append(b, '\n'))
		if err != nil {
			o.Logger.Error("sink(oss): failed to write to file")
			o.SetError(errors.WithStack(err))
			continue
		}
		o.fileRecordCounters[destinationURI]++
		o.Logger.Debug(fmt.Sprintf("sink(oss): written to file: %s", destinationURI))
	}

	for uri, fh := range o.fileHandlers {
		if err := fh.Flush(); err != nil {
			o.Logger.Error(fmt.Sprintf("sink(oss): failed to flush file: %s", uri))
			o.SetError(errors.WithStack(err))
		}
	}
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
