package gcs

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/goccy/go-json"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/fileconverter"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type GCSSource struct {
	common.Source

	client       *Client
	bucket       string
	path         string
	csvDelimiter rune
	skipHeader   bool
	skipRows     int
}

var _ flow.Source = (*GCSSource)(nil)

func NewSource(commonSource common.Source, creds string,
	sourceURI string, csvDelimiter rune, skipHeader bool, skipRows int, opts ...common.Option) (*GCSSource, error) {
	client, err := NewGCSClient(commonSource.Context(), creds)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	parsedURL, err := url.Parse(sourceURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	s := &GCSSource{
		Source:       commonSource,
		client:       client,
		bucket:       parsedURL.Host,
		path:         strings.TrimPrefix(parsedURL.Path, "/"),
		csvDelimiter: csvDelimiter,
		skipHeader:   skipHeader,
		skipRows:     skipRows,
	}

	commonSource.AddCleanFunc(func() error {
		s.Logger().Debug(fmt.Sprintf("cleaning up"))
		return s.client.Close()
	})

	commonSource.RegisterProcess(s.process)

	return s, nil
}

func (s *GCSSource) process() error {
	var objectKeys []string
	var err error
	err = s.DryRunable(func() error {
		objectKeys, err = s.client.ListObjects(s.Context(), s.bucket, s.path)
		return errors.WithStack(err)
	}, func() error {
		objectKeys = []string{}
		return nil
	})
	if err != nil {
		s.Logger().Error(fmt.Sprintf("failed to list objects in bucket: %s", s.bucket))
		return errors.WithStack(err)
	}
	if len(objectKeys) == 0 {
		s.Logger().Info(fmt.Sprintf("no objects found"))
		return nil
	}
	s.Logger().Info(fmt.Sprintf("found %d objects in bucket path: %s", len(objectKeys), s.path))

	for _, key := range objectKeys {
		objectKey := key
		err := s.ConcurrentQueue(func() error {
			s.Logger().Info(fmt.Sprintf("processing object: %s", objectKey))

			rc, err := s.client.OpenFile(s.Context(), s.bucket, objectKey)
			if err != nil {
				s.Logger().Warn(fmt.Sprintf("failed to open object: %s", objectKey))
				return errors.WithStack(err)
			}
			defer rc.Close()

			var r io.ReadSeeker = xio.NewNormalizeLineEndingReader(rc)
			switch filepath.Ext(objectKey) {
			case ".json":
				s.Logger().Debug(fmt.Sprintf("file format is json: %s", objectKey))
			case ".csv":
				dst, err := fileconverter.CSV2JSON(s.Logger(), r, s.skipHeader, s.skipRows, s.csvDelimiter)
				if err != nil {
					s.Logger().Error(fmt.Sprintf("failed to convert csv to json: %s", objectKey))
					return errors.WithStack(err)
				}
				r = dst
			case ".tsv":
				dst, err := fileconverter.CSV2JSON(s.Logger(), r, s.skipHeader, s.skipRows, rune('\t'))
				if err != nil {
					s.Logger().Error(fmt.Sprintf("failed to convert tsv to json: %s", objectKey))
					return errors.WithStack(err)
				}
				r = dst
			default:
				s.Logger().Warn(fmt.Sprintf("unsupported file format: %s, use default (json)", filepath.Ext(objectKey)))
			}

			reader := bufio.NewReader(r)
			for {
				raw, err := reader.ReadBytes('\n')
				if len(raw) > 0 && raw[0] != '\n' {
					line := make([]byte, len(raw))
					copy(line, raw)

					record := model.NewRecord()
					if unmarshalErr := json.Unmarshal(line, &record); unmarshalErr != nil {
						return errors.WithStack(fmt.Errorf("failed to unmarshal record: %w", unmarshalErr))
					}
					s.SendRecord(record)
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.WithStack(err)
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	if err := s.ConcurrentQueueWait(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
