package oss

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/component/source"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// OSSSource is the source component for OSS.
type OSSSource struct {
	*source.CommonSource

	ctx          context.Context
	client       *oss.Client
	bucket       string
	pathPrefix   string
	fileFormat   string
	csvDelimiter rune
	columnMap    map[string]string
}

var _ flow.Source = (*OSSSource)(nil)

// NewSource creates a new OSSSource.
func NewSource(ctx context.Context, l *slog.Logger, creds string,
	sourceURI, fileFormat string, csvDelimiter rune,
	columnMappingFilePath string, opts ...option.Option) (*OSSSource, error) {
	// create commonSource source
	commonSource := source.NewCommonSource(l, opts...)

	// create OSS client
	client, err := NewOSSClient(creds)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse source bucket path
	parsedURL, err := url.Parse(sourceURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// read column map
	columnMap, err := extcommon.GetColumnMap(columnMappingFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ossSource := &OSSSource{
		CommonSource: commonSource,
		ctx:          ctx,
		client:       client,
		bucket:       parsedURL.Host,
		pathPrefix:   strings.TrimPrefix(parsedURL.Path, "/"),
		fileFormat:   fileFormat,
		csvDelimiter: csvDelimiter,
		columnMap:    columnMap,
	}

	// add clean function
	commonSource.AddCleanFunc(func() {
		commonSource.Logger.Debug("source(oss): cleaning up")
	})

	// register source process
	commonSource.RegisterProcess(ossSource.process)

	return ossSource, nil
}

func (o *OSSSource) process() {
	// list objects
	objectResult, err := o.client.ListObjects(o.ctx, &oss.ListObjectsRequest{
		Bucket: oss.Ptr(o.bucket),
		Prefix: oss.Ptr(o.pathPrefix),
	})
	if err != nil {
		o.Logger.Error(fmt.Sprintf("source(oss): failed to list objects in bucket: %s", o.bucket))
		o.SetError(errors.WithStack(err))
		return
	}
	if len(objectResult.Contents) == 0 {
		o.Logger.Info("sink(oss): no objects found")
		return
	}

	// process objects
	for _, objectProp := range objectResult.Contents {
		// download object
		object, err := o.client.GetObject(o.ctx, &oss.GetObjectRequest{
			Bucket: oss.Ptr(o.bucket),
			Key:    objectProp.Key,
		})
		if err != nil {
			o.Logger.Warn(fmt.Sprintf("source(oss): failed to download, skip the object: %s. error: %s", oss.ToString(objectProp.Key), err.Error()))
			return
		}

		// unpack records
		records, err := o.unpackRecords(object)
		if err != nil {
			o.Logger.Error(fmt.Sprintf("source(oss): failed to unpack records from object: %s", oss.ToString(objectProp.Key)))
			o.SetError(errors.WithStack(err))
			return
		}

		// send records
		for _, record := range records {
			mappedRecord := extcommon.KeyMapping(o.columnMap, record)
			raw, err := json.Marshal(mappedRecord)
			if err != nil {
				o.Logger.Error(fmt.Sprintf("source(oss): failed to marshal record: %s", err.Error()))
				o.SetError(errors.WithStack(err))
				continue
			}
			o.Send(raw)
		}
	}
}

func (o *OSSSource) unpackRecords(object *oss.GetObjectResult) ([]map[string]interface{}, error) {
	// unmarshal object based on file format
	var (
		records []map[string]interface{}
		err     error
	)
	switch strings.ToLower(o.fileFormat) {
	case "csv":
		records, err = o.unmarshalCSV(object)
	case "json":
		records, err = o.unmarshalJSON(object)
	default:
		err = errors.Errorf("unsupported file format: %s", o.fileFormat)
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return records, nil
}

func (o *OSSSource) unmarshalCSV(object *oss.GetObjectResult) ([]map[string]interface{}, error) {
	// read object content
	raw, err := io.ReadAll(object.Body)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// unmarshal CSV
	reader := csv.NewReader(bytes.NewReader(raw))

	// set delimiter
	if o.csvDelimiter != 0 {
		reader.Comma = o.csvDelimiter
	}

	// read all records
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// convert rows to records
	records := make([]map[string]interface{}, 0, len(rows)-1)
	headers := rows[0]
	for _, row := range rows[1:] {
		record := make(map[string]interface{}, len(headers))
		for i, header := range headers {
			record[header] = row[i]
		}
		records = append(records, record)
	}

	return records, nil
}

func (o *OSSSource) unmarshalJSON(object *oss.GetObjectResult) ([]map[string]interface{}, error) {
	// read object content
	raw, err := io.ReadAll(object.Body)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// read records
	records := make([]map[string]interface{}, 0)
	reader := bufio.NewReader(bytes.NewReader(raw))
	for {
		// read line
		raw, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errors.WithStack(err)
		}

		// remove trailing newline
		raw = bytes.TrimSuffix(raw, []byte("\n"))

		// unmarshal JSON
		var record map[string]interface{}
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, errors.WithStack(err)
		}

		records = append(records, record)
	}

	return records, nil
}
