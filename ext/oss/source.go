package oss

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	extcommon "github.com/goto/optimus-any2any/ext/common"
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
	skipHeader   bool
}

var _ flow.Source = (*OSSSource)(nil)

// NewSource creates a new OSSSource.
func NewSource(ctx context.Context, l *slog.Logger, creds string,
	sourceURI string, csvDelimiter rune, skipHeader bool, opts ...common.Option) (*OSSSource, error) {
	// create commonSource source
	commonSource := common.NewSource(l, opts...)
	commonSource.SetName("oss")

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
		skipHeader:   skipHeader,
	}

	// add clean function
	commonSource.AddCleanFunc(func() {
		commonSource.Logger.Debug(fmt.Sprintf("cleaning up"))
	})

	// register source process
	commonSource.RegisterProcess(ossSource.process)

	return ossSource, nil
}

func (o *OSSSource) process() error {
	// list objects
	objectResult, err := o.client.ListObjects(o.ctx, &oss.ListObjectsRequest{
		Bucket: oss.Ptr(o.bucket),
		Prefix: oss.Ptr(o.path),
	})
	if err != nil {
		o.Logger.Error(fmt.Sprintf("failed to list objects in bucket: %s", o.bucket))
		return errors.WithStack(err)
	}
	if len(objectResult.Contents) == 0 {
		o.Logger.Info(fmt.Sprintf("no objects found"))
		return nil
	}

	// process objects
	for _, objectProp := range objectResult.Contents {
		o.Logger.Info(fmt.Sprintf("processing object: %s", oss.ToString(objectProp.Key)))
		// read object
		ossFile, err := o.client.OpenFile(o.ctx, o.bucket, oss.ToString(objectProp.Key))
		if err != nil {
			o.Logger.Warn(fmt.Sprintf("failed to open object: %s", oss.ToString(objectProp.Key)))
			return errors.WithStack(err)
		}
		defer ossFile.Close()

		var reader io.Reader
		switch filepath.Ext(oss.ToString(objectProp.Key)) {
		case ".json":
			reader = ossFile
		case ".csv":
			reader = extcommon.FromCSVToJSON(o.Logger, ossFile, o.skipHeader)
			if o.csvDelimiter != 0 {
				reader = extcommon.FromCSVToJSON(o.Logger, ossFile, o.skipHeader, o.csvDelimiter)
			}
		case ".tsv":
			reader = extcommon.FromCSVToJSON(o.Logger, ossFile, o.skipHeader, rune('\t'))
		default:
			o.Logger.Warn(fmt.Sprintf("unsupported file format: %s, use default (json)", filepath.Ext(oss.ToString(objectProp.Key))))
			reader = ossFile
		}

		sc := bufio.NewScanner(reader)
		for sc.Scan() {
			raw := sc.Bytes()
			line := make([]byte, len(raw))
			copy(line, raw)
			o.Send(line)
		}
	}
	return nil
}
