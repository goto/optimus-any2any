package oss

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// OSSSource is the source component for OSS.
type OSSSource struct {
	*common.Source

	ctx          context.Context
	client       *oss.Client
	bucket       string
	path         string
	csvDelimiter rune
}

var _ flow.Source = (*OSSSource)(nil)

// NewSource creates a new OSSSource.
func NewSource(ctx context.Context, l *slog.Logger, creds string,
	sourceURI string, csvDelimiter rune, opts ...common.Option) (*OSSSource, error) {
	// create commonSource source
	commonSource := common.NewSource(l, opts...)

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

	ossSource := &OSSSource{
		Source:       commonSource,
		ctx:          ctx,
		client:       client,
		bucket:       parsedURL.Host,
		path:         strings.TrimPrefix(parsedURL.Path, "/"),
		csvDelimiter: csvDelimiter,
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
		Prefix: oss.Ptr(o.path),
	})
	if err != nil {
		o.Logger.Error(fmt.Sprintf("source(oss): failed to list objects in bucket: %s", o.bucket))
		o.SetError(errors.WithStack(err))
		return
	}
	if len(objectResult.Contents) == 0 {
		o.Logger.Info("source(oss): no objects found")
		return
	}

	// process objects
	for _, objectProp := range objectResult.Contents {
		o.Logger.Info(fmt.Sprintf("source(oss): processing object: %s", oss.ToString(objectProp.Key)))
		// read object
		ossFile, err := o.client.OpenFile(o.ctx, o.bucket, oss.ToString(objectProp.Key))
		if err != nil {
			o.Logger.Warn(fmt.Sprintf("source(oss): failed to open object: %s", oss.ToString(objectProp.Key)))
			o.SetError(errors.WithStack(err))
			return
		}
		defer ossFile.Close()

		// unpack records
		records, err := o.unpackRecords(filepath.Ext(oss.ToString(objectProp.Key)), ossFile)
		if err != nil {
			o.Logger.Error(fmt.Sprintf("source(oss): failed to unpack records from object: %s", oss.ToString(objectProp.Key)))
			o.SetError(errors.WithStack(err))
			return
		}

		// send records
		for _, record := range records {
			raw, err := json.Marshal(record)
			if err != nil {
				o.Logger.Error(fmt.Sprintf("source(oss): failed to marshal record: %s", err.Error()))
				o.SetError(errors.WithStack(err))
				continue
			}
			o.Send(raw)
		}
	}
}

func (o *OSSSource) unpackRecords(ext string, reader io.Reader) ([]map[string]interface{}, error) {
	// unmarshal object based on file format
	var (
		records []map[string]interface{}
		err     error
	)
	switch ext {
	case ".csv":
		records, err = o.unmarshalCSV(reader)
	case ".json":
		records, err = o.unmarshalJSON(reader)
	default:
		err = errors.Errorf("unsupported file format: %s", ext)
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return records, nil
}

func (o *OSSSource) unmarshalCSV(r io.Reader) ([]map[string]interface{}, error) {
	// unmarshal CSV
	reader := csv.NewReader(r)

	// set delimiter
	if o.csvDelimiter != 0 {
		reader.Comma = o.csvDelimiter
	}

	// read all records
	records := make([]map[string]interface{}, 0)
	headers := []string{}
	isHeader := true
	for record, err := reader.Read(); err == nil; record, err = reader.Read() {
		if isHeader {
			isHeader = false
			headers = record
			continue
		}
		recordResult := map[string]interface{}{}
		for i, header := range headers {
			recordResult[header] = record[i]
		}
		records = append(records, recordResult)
	}
	return records, nil
}

func (o *OSSSource) unmarshalJSON(r io.Reader) ([]map[string]interface{}, error) {
	// read records
	records := make([]map[string]interface{}, 0)
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		// read line
		raw := sc.Bytes()
		line := make([]byte, len(raw)) // Important: make a copy of the line before sending
		copy(line, raw)
		o.Logger.Debug(fmt.Sprintf("source(oss): read line: %s", string(line)))

		// unmarshal JSON
		var record map[string]interface{}
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, errors.WithStack(err)
		}

		records = append(records, record)
	}

	return records, nil
}
