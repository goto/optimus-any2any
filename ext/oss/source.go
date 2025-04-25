package oss

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/helper"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// OSSSource is the source component for OSS.
type OSSSource struct {
	common.Source

	client       *oss.Client
	bucket       string
	path         string
	csvDelimiter rune
	skipHeader   bool
	skipRows     int
}

var _ flow.Source = (*OSSSource)(nil)

// NewSource creates a new OSSSource.
func NewSource(commonSource common.Source, creds string,
	sourceURI string, csvDelimiter rune, skipHeader bool, skipRows int, opts ...common.Option) (*OSSSource, error) {
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

	o := &OSSSource{
		Source:       commonSource,
		client:       client,
		bucket:       parsedURL.Host,
		path:         strings.TrimPrefix(parsedURL.Path, "/"),
		csvDelimiter: csvDelimiter,
		skipHeader:   skipHeader,
		skipRows:     skipRows,
	}

	// add clean function
	commonSource.AddCleanFunc(func() error {
		o.Logger().Debug(fmt.Sprintf("cleaning up"))
		return nil
	})

	// register source process
	commonSource.RegisterProcess(o.process)

	return o, nil
}

func (o *OSSSource) process() error {
	logCheckPoint := 1000
	recordCounter := 0
	// list objects
	objectResult, err := o.client.ListObjects(o.Context(), &oss.ListObjectsRequest{
		Bucket: oss.Ptr(o.bucket),
		Prefix: oss.Ptr(o.path),
	})
	if err != nil {
		o.Logger().Error(fmt.Sprintf("failed to list objects in bucket: %s", o.bucket))
		return errors.WithStack(err)
	}
	if len(objectResult.Contents) == 0 {
		o.Logger().Info(fmt.Sprintf("no objects found"))
		return nil
	}

	// process objects
	for _, objectProp := range objectResult.Contents {
		o.Logger().Info(fmt.Sprintf("processing object: %s", oss.ToString(objectProp.Key)))
		// read object
		ossFile, err := o.client.OpenFile(o.Context(), o.bucket, oss.ToString(objectProp.Key))
		if err != nil {
			o.Logger().Warn(fmt.Sprintf("failed to open object: %s", oss.ToString(objectProp.Key)))
			return errors.WithStack(err)
		}
		defer ossFile.Close()

		var reader io.Reader
		switch filepath.Ext(oss.ToString(objectProp.Key)) {
		case ".json":
			reader = ossFile
		case ".csv":
			reader = helper.FromCSVToJSON(o.Logger(), ossFile, o.skipHeader, o.skipRows, o.csvDelimiter)
		case ".tsv":
			reader = helper.FromCSVToJSON(o.Logger(), ossFile, o.skipHeader, o.skipRows, rune('\t'))
		default:
			o.Logger().Warn(fmt.Sprintf("unsupported file format: %s, use default (json)", filepath.Ext(oss.ToString(objectProp.Key))))
			reader = ossFile
		}

		sc := bufio.NewScanner(reader)
		for sc.Scan() {
			raw := sc.Bytes()
			line := make([]byte, len(raw))
			copy(line, raw)
			o.Send(line)
			recordCounter++
			if recordCounter%logCheckPoint == 0 {
				o.Logger().Info(fmt.Sprintf("sent %d records", recordCounter))
			}
		}
	}
	o.Logger().Info(fmt.Sprintf("successfully sent %d records", recordCounter))
	return nil
}
