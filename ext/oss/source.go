package oss

import (
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/fileconverter"
	"github.com/goto/optimus-any2any/internal/helper"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// OSSSource is the source component for OSS.
type OSSSource struct {
	common.Source

	client         *Client
	bucket         string
	path           string
	filenameColumn string
	csvDelimiter   rune
	skipHeader     bool
	skipRows       int
}

var _ flow.Source = (*OSSSource)(nil)

// NewSource creates a new OSSSource.
func NewSource(commonSource common.Source, creds string,
	sourceURI string, filenameColumn string, csvDelimiter rune, skipHeader bool, skipRows int, opts ...common.Option) (*OSSSource, error) {
	// create OSS client
	client, err := NewOSSClient(commonSource.Context(), creds, OSSClientConfig{})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse source bucket path
	parsedURL, err := url.Parse(sourceURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	o := &OSSSource{
		Source:         commonSource,
		client:         client,
		bucket:         parsedURL.Host,
		path:           strings.TrimPrefix(parsedURL.Path, "/"),
		filenameColumn: filenameColumn,
		csvDelimiter:   csvDelimiter,
		skipHeader:     skipHeader,
		skipRows:       skipRows,
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
	// list objects
	var objectResult *oss.ListObjectsResult
	var err error
	err = o.DryRunable(func() error {
		objectResult, err = o.client.ListObjects(o.Context(), &oss.ListObjectsRequest{
			Bucket: oss.Ptr(o.bucket),
			Prefix: oss.Ptr(o.path),
		})
		return errors.WithStack(err)
	}, func() error {
		// use empty result for dry run
		objectResult = &oss.ListObjectsResult{}
		return nil
	})
	if err != nil {
		o.Logger().Error(fmt.Sprintf("failed to list objects in bucket: %s", o.bucket))
		return errors.WithStack(err)
	}
	if len(objectResult.Contents) == 0 {
		o.Logger().Info(fmt.Sprintf("no objects found"))
		return nil
	}
	o.Logger().Info(fmt.Sprintf("found %d objects in bucket path: %s", len(objectResult.Contents), o.path))

	// process objects
	for _, content := range objectResult.Contents {
		objectProp := content
		err := o.ConcurrentQueue(func() error {
			o.Logger().Info(fmt.Sprintf("processing object: %s", oss.ToString(objectProp.Key)))
			// read object
			ossFile, err := o.client.OpenFile(o.Context(), o.bucket, oss.ToString(objectProp.Key))
			if err != nil {
				o.Logger().Warn(fmt.Sprintf("failed to open object: %s", oss.ToString(objectProp.Key)))
				return errors.WithStack(err)
			}
			defer ossFile.Close()

			// TODO: refactor this
			var r io.ReadSeeker = xio.NewNormalizeLineEndingReader(ossFile)
			switch filepath.Ext(oss.ToString(objectProp.Key)) {
			case ".json":
				// skip
				o.Logger().Debug(fmt.Sprintf("file format is json: %s", oss.ToString(objectProp.Key)))
			case ".csv":
				dst, err := fileconverter.CSV2JSON(o.Logger(), r, o.skipHeader, o.skipRows, o.csvDelimiter)
				if err != nil {
					o.Logger().Error(fmt.Sprintf("failed to convert csv to json: %s", oss.ToString(objectProp.Key)))
					return errors.WithStack(err)
				}
				r = dst
			case ".tsv":
				dst, err := fileconverter.CSV2JSON(o.Logger(), r, o.skipHeader, o.skipRows, rune('\t'))
				if err != nil {
					o.Logger().Error(fmt.Sprintf("failed to convert tsv to json: %s", oss.ToString(objectProp.Key)))
					return errors.WithStack(err)
				}
				r = dst
			default:
				o.Logger().Warn(fmt.Sprintf("unsupported file format: %s, use default (json)", filepath.Ext(oss.ToString(objectProp.Key))))
				// skip
			}

			// get filename from object key
			filename := filepath.Base(oss.ToString(objectProp.Key))

			// send records
			recordReader := helper.NewRecordReader(o.Logger(), r)
			for record, err := range recordReader.ReadRecord() {
				if err != nil {
					o.Logger().Error(fmt.Sprintf("failed to read record %s", err.Error()))
					return errors.WithStack(err)
				}
				// add metadata filename
				record.Set(o.filenameColumn, filename)

				// send to channel
				o.SendRecord(record)
			}

			return nil
		})
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to process object: %s", oss.ToString(objectProp.Key)))
			return errors.WithStack(err)
		}
	}
	if err := o.ConcurrentQueueWait(); err != nil {
		o.Logger().Error(fmt.Sprintf("failed to wait for concurrent queue: %s", err))
		return errors.WithStack(err)
	}
	return nil
}
