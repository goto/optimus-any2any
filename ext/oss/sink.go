package oss

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/component/sink"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type OSSSink struct {
	*sink.CommonSink

	ctx context.Context

	client *oss.Client

	bucket          string
	pathPrefix      string
	filenamePattern string
	batchSize       int64
	enableOverwrite bool
}

var _ flow.Sink = (*OSSSink)(nil)

// NewSink creates a new OSSSink
func NewSink(ctx context.Context, l *slog.Logger,
	svcAcc, destinationBucketPath, filenamePattern string,
	batchSize int64, enableOverwrite bool,
	opts ...option.Option) (*OSSSink, error) {
	commonSink := sink.NewCommonSink(l, opts...)

	client, err := NewOSSClient(svcAcc)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse the given destinationBucketPath, so we can obtain both the bucket and the path
	// to be used as the uploaded object's prefix
	parsedURL, err := url.Parse(fmt.Sprintf("oss://%s", destinationBucketPath))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ossSink := &OSSSink{
		CommonSink:      commonSink,
		ctx:             ctx,
		client:          client,
		bucket:          parsedURL.Host,
		pathPrefix:      strings.TrimPrefix(parsedURL.Path, "/"),
		filenamePattern: filenamePattern,
		batchSize:       batchSize,
		enableOverwrite: enableOverwrite,
	}

	// preprocess, truncate the objects if overwrite is enabled
	if ossSink.enableOverwrite {
		ossSink.Logger.Info(fmt.Sprintf("sink(oss): truncating the object on %s/%s", ossSink.bucket, ossSink.pathPrefix))
		// Truncate the objects
		if err := ossSink.truncate(); err != nil {
			ossSink.Logger.Error(fmt.Sprintf("sink(oss): failed to truncate object: %s", err.Error()))
			return nil, err
		}
		ossSink.Logger.Info("sink(oss): objects truncated")
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		commonSink.Logger.Debug("sink(oss): close record writer")
		// oss uploader already handles the cleanup, so there's no need to explicitly call
	})

	// register sink process
	commonSink.RegisterProcess(ossSink.process)

	return ossSink, nil
}

func (o *OSSSink) process() {
	records := []string{}
	values := map[string]string{}
	batchCount := 1

	for msg := range o.Read() {
		b, ok := msg.([]byte)
		if !ok {
			o.Logger.Error(fmt.Sprintf("sink(oss): message type assertion error: %T", msg))
			o.SetError(errors.New(fmt.Sprintf("sink(oss): message type assertion error: %T", msg)))
			continue
		}
		record := string(b)
		o.Logger.Debug(fmt.Sprintf("sink(oss): received message: %s", record))

		records = append(records, record)
		if int64(len(records)) >= o.batchSize {
			o.Logger.Info(fmt.Sprintf("sink(oss): (batch %d) uploading %d records", batchCount, len(records)))
			values["batch_start"] = fmt.Sprintf("%d", batchCount*int(o.batchSize))
			values["batch_end"] = fmt.Sprintf("%d", (batchCount+1)*int(o.batchSize))
			filename := renderFilename(o.filenamePattern, values)
			if err := o.upload(records, filename); err != nil {
				o.Logger.Error(fmt.Sprintf("sink(oss): (batch %d) failed to upload records: %s", batchCount, err.Error()))
				o.SetError(err)
			}

			batchCount++
			// free up the memory
			records = []string{}
		}
	}

	// Upload remaining records
	if len(records) > 0 {
		o.Logger.Info(fmt.Sprintf("sink(oss): (batch %d) uploading %d records", batchCount, len(records)))
		values["batch_start"] = fmt.Sprintf("%d", batchCount*int(o.batchSize))
		values["batch_end"] = fmt.Sprintf("%d", batchCount*int(o.batchSize)+len(records))
		filename := renderFilename(o.filenamePattern, values)
		if err := o.upload(records, filename); err != nil {
			o.Logger.Error(fmt.Sprintf("sink(oss): (batch %d) failed to upload records: %s", batchCount, err.Error()))
			o.SetError(err)
		}
	}
}

func (o *OSSSink) upload(records []string, filename string) error {
	// Convert records to the format required by OSS
	data := strings.Join(records, "\n")
	path := fmt.Sprintf("%s/%s", o.pathPrefix, filename)

	o.Logger.Info(fmt.Sprintf("sink(oss): uploading %d records to path %s", len(records), path))
	uploader := o.client.NewUploader()

	// Upload the data to OSS
	uploadResult, err := uploader.UploadFrom(o.ctx, &oss.PutObjectRequest{
		Bucket: oss.Ptr(o.bucket),
		Key:    oss.Ptr(path),
	}, strings.NewReader(data))
	if err != nil {
		return errors.WithStack(err)
	}

	o.Logger.Info(fmt.Sprintf("sink(oss): uploaded %d records to path %s (Response OSS: %d)",
		len(records), path, uploadResult.StatusCode))

	return nil
}

func (o *OSSSink) truncate() error {
	// List all objects with the given path prefix
	objectResult, err := o.client.ListObjectsV2(o.ctx, &oss.ListObjectsV2Request{
		Bucket: oss.Ptr(o.bucket),
		Prefix: oss.Ptr(o.pathPrefix),
	})
	if err != nil {
		return errors.WithStack(err)
	}
	if len(objectResult.Contents) == 0 {
		o.Logger.Info("sink(oss): no objects found")
		return nil
	}

	objects := make([]oss.DeleteObject, len(objectResult.Contents))
	for i, obj := range objectResult.Contents {
		objects[i] = oss.DeleteObject{Key: obj.Key}
	}

	// Truncate the objects
	response, err := o.client.DeleteMultipleObjects(o.ctx, &oss.DeleteMultipleObjectsRequest{
		Bucket:  oss.Ptr(o.bucket),
		Objects: objects,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	if response.StatusCode >= 400 {
		return errors.New(fmt.Sprintf("failed to truncate object: %d", response.StatusCode))
	}
	o.Logger.Info(fmt.Sprintf("sink(oss): truncated %d objects", len(objects)))
	return nil
}

func renderFilename(filenamePattern string, values map[string]string) string {
	// Replace the filename pattern with the values
	for k, v := range values {
		filenamePattern = strings.ReplaceAll(filenamePattern, fmt.Sprintf("{%s}", k), v)
	}
	return filenamePattern
}
