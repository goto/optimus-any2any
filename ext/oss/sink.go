package oss

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

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

	bucket         string
	objectPrefix   string
	batchSize      int64
	enableTruncate bool
}

var _ flow.Sink = (*OSSSink)(nil)

// NewSink creates a new OSSSink
// svcAcc is the service account json string refer to ossCredentials
// destinationBucketPath is the bucket path to write to, it must be in the format of bucket_name/path
// filenamePrefix is the prefix of the filename to write to
// batchSize is the number of records to batch before uploading
func NewSink(ctx context.Context, l *slog.Logger, svcAcc, destinationBucketPath, filenamePrefix string, batchSize int64, enableTruncate bool, opts ...option.Option) (*OSSSink, error) {
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
	objectPrefix := fmt.Sprintf("%s/%s", strings.Trim(parsedURL.Path, "/\\"), filenamePrefix)

	ossSink := &OSSSink{
		CommonSink:     commonSink,
		ctx:            ctx,
		client:         client,
		bucket:         parsedURL.Host,
		objectPrefix:   objectPrefix,
		batchSize:      batchSize,
		enableTruncate: enableTruncate,
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
	batchCount := 1

	if o.enableTruncate {
		o.Logger.Info(fmt.Sprintf("sink(oss): truncating the object with prefix %s", o.objectPrefix))
		// Truncate the object prefix
		if err := o.truncate(); err != nil {
			o.Logger.Error(fmt.Sprintf("sink(oss): failed to truncate object prefix: %s", err.Error()))
			o.SetError(err)
			return
		}
		o.Logger.Info("sink(oss): object prefix truncated")
	}

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
			if err := o.upload(records); err != nil {
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
		if err := o.upload(records); err != nil {
			o.Logger.Error(fmt.Sprintf("sink(oss): (batch %d) failed to upload records: %s", batchCount, err.Error()))
			o.SetError(err)
		}
	}
}

func (o *OSSSink) upload(records []string) error {
	// Convert records to the format required by OSS
	data := strings.Join(records, "\n")

	// Create the object key (filename) for the upload
	objectKey := fmt.Sprintf("%s-%d.json", o.objectPrefix, time.Now().UnixNano())

	o.Logger.Info(fmt.Sprintf("sink(oss): uploading %d records to path %s", len(records), objectKey))
	uploader := o.client.NewUploader()

	// Upload the data to OSS
	uploadResult, err := uploader.UploadFrom(o.ctx, &oss.PutObjectRequest{
		Bucket: oss.Ptr(o.bucket),
		Key:    oss.Ptr(objectKey),
	}, strings.NewReader(data))
	if err != nil {
		return errors.WithStack(err)
	}

	o.Logger.Info(fmt.Sprintf("sink(oss): uploaded %d records to path %s (Response OSS: %d)",
		len(records), objectKey, uploadResult.StatusCode))

	return nil
}

func (o *OSSSink) truncate() error {
	// List all objects with the given prefix
	objectResult, err := o.client.ListObjectsV2(o.ctx, &oss.ListObjectsV2Request{
		Bucket: oss.Ptr(o.bucket),
		Prefix: oss.Ptr(o.objectPrefix),
	})
	if err != nil {
		return errors.WithStack(err)
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
		return errors.New(fmt.Sprintf("failed to truncate object prefix: %d", response.StatusCode))
	}
	o.Logger.Info(fmt.Sprintf("sink(oss): truncated %d objects", len(objects)))
	return nil
}
