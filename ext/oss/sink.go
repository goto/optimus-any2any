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

	uploader *oss.Uploader

	bucket       string
	objectPrefix string
	batchSize    int64
}

var _ flow.Sink = (*OSSSink)(nil)

func NewSink(l *slog.Logger, svcAcc, destinationBucketPath, filenamePrefix string, batchSize int64, opts ...option.Option) (*OSSSink, error) {
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

	uploader := client.NewUploader()
	ossSink := &OSSSink{
		CommonSink:   commonSink,
		uploader:     uploader,
		bucket:       parsedURL.Host,
		objectPrefix: objectPrefix,
		batchSize:    batchSize,
		ctx:          context.Background(),
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

	o.Logger.Info(fmt.Sprintf("sink(oss): uploading %d records to path %s",
		len(records), objectKey))

	// Upload the data to OSS
	uploadResult, err := o.uploader.UploadFrom(o.ctx, &oss.PutObjectRequest{
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
