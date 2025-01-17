package oss

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
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
	columnMap       map[string]string
	enableOverwrite bool
	// batch size for the sink
	isGroupByBatch bool
	batchSize      int
	// group by column for the sink
	isGroupByColumn bool
	groupByColumn   string
}

var _ flow.Sink = (*OSSSink)(nil)

// NewSink creates a new OSSSink
func NewSink(ctx context.Context, l *slog.Logger,
	svcAcc, destinationBucketPath string,
	groupBy string, groupBatchSize int, groupColumnName string,
	columnMappingFilePath string,
	filenamePattern string, enableOverwrite bool,
	opts ...option.Option) (*OSSSink, error) {

	// create common sink
	commonSink := sink.NewCommonSink(l, opts...)

	// create OSS client
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

	// read column map
	columnMap, err := getColumnMap(columnMappingFilePath)
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
		columnMap:       columnMap,
		enableOverwrite: enableOverwrite,
		isGroupByBatch:  strings.ToLower(groupBy) == "batch",
		batchSize:       groupBatchSize,
		isGroupByColumn: strings.ToLower(groupBy) == "column",
		groupByColumn:   groupColumnName,
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
	records := [][]byte{}
	values := map[string]string{}
	batchCount := 1

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

		// modify the message based on the column map
		var val map[string]interface{}
		if err := json.Unmarshal(b, &val); err != nil {
			o.Logger.Error(fmt.Sprintf("sink(oss): failed to unmarshal message: %s", err.Error()))
			o.SetError(errors.WithStack(err))
			continue
		}
		val = o.mapping(val)
		raw, err := json.Marshal(val)
		if err != nil {
			o.Logger.Error(fmt.Sprintf("sink(oss): failed to marshal message: %s", err.Error()))
			o.SetError(errors.WithStack(err))
			continue
		}

		records = append(records, raw)
		if o.isGroupByBatch && len(records) >= o.batchSize {
			o.Logger.Info(fmt.Sprintf("sink(oss): (batch %d) uploading %d records", batchCount, len(records)))
			values["batch_start"] = fmt.Sprintf("%d", batchCount*int(o.batchSize))
			values["batch_end"] = fmt.Sprintf("%d", (batchCount+1)*int(o.batchSize))
			filename := renderFilename(o.filenamePattern, values)
			if err := o.upload(records, filename); err != nil {
				o.Logger.Error(fmt.Sprintf("sink(oss): (batch %d) failed to upload records: %s", batchCount, err.Error()))
				o.SetError(errors.WithStack(err))
			}

			batchCount++
			// free up the memory
			records = [][]byte{}
		}
	}

	// Upload remaining records
	if o.isGroupByBatch && len(records) > 0 {
		o.Logger.Info(fmt.Sprintf("sink(oss): (batch %d) uploading %d records", batchCount, len(records)))
		values["batch_start"] = fmt.Sprintf("%d", batchCount*o.batchSize)
		values["batch_end"] = fmt.Sprintf("%d", batchCount*o.batchSize+len(records))
		filename := renderFilename(o.filenamePattern, values)
		if err := o.upload(records, filename); err != nil {
			o.Logger.Error(fmt.Sprintf("sink(oss): (batch %d) failed to upload records: %s", batchCount, err.Error()))
			o.SetError(errors.WithStack(err))
		}
		return
	}

	if o.isGroupByColumn {
		// Upload the remaining records
		if len(records) > 0 {
			o.Logger.Info(fmt.Sprintf("sink(oss): uploading %d records", len(records)))
			groupedRecords, err := o.groupRecordsByColumn(records)
			if err != nil {
				o.Logger.Error(fmt.Sprintf("sink(oss): failed to group records by column: %s", err.Error()))
				o.SetError(errors.WithStack(err))
				return
			}
			for groupKey, groupRecords := range groupedRecords {
				o.Logger.Info(fmt.Sprintf("sink(oss): uploading %d records for group %s", len(groupRecords), groupKey))
				values[o.groupByColumn] = groupKey
				filename := renderFilename(o.filenamePattern, values)
				if err := o.upload(groupRecords, filename); err != nil {
					o.Logger.Error(fmt.Sprintf("sink(oss): failed to upload records for group %s: %s", groupKey, err.Error()))
					o.SetError(errors.WithStack(err))
				}
			}
		}
		return
	}

	// upload all records if not grouped
	if len(records) > 0 {
		o.Logger.Info(fmt.Sprintf("sink(oss): uploading %d records", len(records)))
		filename := renderFilename(o.filenamePattern, values)
		if err := o.upload(records, filename); err != nil {
			o.Logger.Error(fmt.Sprintf("sink(oss): failed to upload records: %s", err.Error()))
			o.SetError(errors.WithStack(err))
		}
	}
}

func (o *OSSSink) mapping(value map[string]interface{}) map[string]interface{} {
	if o.columnMap == nil {
		return value
	}
	o.Logger.Debug(fmt.Sprintf("sink(oss): record before map: %v", value))
	mappedValue := make(map[string]interface{})
	for key, val := range value {
		if mappedKey, ok := o.columnMap[key]; ok {
			mappedValue[mappedKey] = val
		} else {
			mappedValue[key] = val
		}
	}
	o.Logger.Debug(fmt.Sprintf("sink(oss): record after map: %v", mappedValue))
	return mappedValue
}

func (o *OSSSink) upload(records [][]byte, filename string) error {
	// Convert records to the format required by OSS
	data := bytes.Join(records, []byte("\n"))
	path := fmt.Sprintf("%s/%s", o.pathPrefix, filename)

	o.Logger.Info(fmt.Sprintf("sink(oss): uploading %d records to path %s", len(records), path))
	uploader := o.client.NewUploader()

	// Upload the data to OSS
	uploadResult, err := uploader.UploadFrom(o.ctx, &oss.PutObjectRequest{
		Bucket: oss.Ptr(o.bucket),
		Key:    oss.Ptr(path),
	}, bytes.NewReader(data))
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
		err := errors.New(fmt.Sprintf("failed to truncate object: %d", response.StatusCode))
		return errors.WithStack(err)
	}
	o.Logger.Info(fmt.Sprintf("sink(oss): truncated %d objects", len(objects)))
	return nil
}

func (o *OSSSink) groupRecordsByColumn(records [][]byte) (map[string][][]byte, error) {
	o.Logger.Info(fmt.Sprintf("sink(oss): grouping records by column: %s", o.groupByColumn))
	groupRecords := map[string][][]byte{}
	for _, raw := range records {
		// parse the record
		var v map[string]interface{}
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, errors.WithStack(err)
		}
		groupKeyRaw, ok := v[o.groupByColumn]
		if !ok {
			return nil, errors.New(fmt.Sprintf("group by column not found: %s", o.groupByColumn))
		}
		groupKey := fmt.Sprintf("%v", groupKeyRaw)
		groupRecords[groupKey] = append(groupRecords[groupKey], raw)
	}
	o.Logger.Info(fmt.Sprintf("sink(oss): %d groups found", len(groupRecords)))
	return groupRecords, nil
}

func renderFilename(filenamePattern string, values map[string]string) string {
	// Replace the filename pattern with the values
	for k, v := range values {
		filenamePattern = strings.ReplaceAll(filenamePattern, fmt.Sprintf("{%s}", k), v)
	}
	return filenamePattern
}

// getColumnMap reads the column map from the file.
func getColumnMap(columnMapFilePath string) (map[string]string, error) {
	if columnMapFilePath == "" {
		return nil, nil
	}
	columnMapRaw, err := os.ReadFile(columnMapFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	columnMap := make(map[string]string)
	if err = json.Unmarshal(columnMapRaw, &columnMap); err != nil {
		return nil, errors.WithStack(err)
	}
	return columnMap, nil
}
