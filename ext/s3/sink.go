package s3

import (
	errs "errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/goccy/go-json"
	xaws "github.com/goto/optimus-any2any/internal/auth/aws"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/helper"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type S3Sink struct {
	common.Sink

	client                    *S3Client
	destinationURITemplate    *template.Template
	writeHandlers             map[string]xio.WriteFlusher // tmp write handler
	s3Handlers                map[string]io.WriteCloser
	fileRecordCounters        map[string]int
	batchSize                 int
	enableOverwrite           bool
	skipHeader                bool
	maxTempFileRecordNumber   int
	partialFileRecordCounters map[string]int // map of destinationURI to the number of records
}

var _ flow.Sink = (*S3Sink)(nil)

// NewS3Sink creates a new S3 sink instance
func NewSink(commonSink common.Sink,
	rawCreds string, clientCredsProvider string,
	region string, destinationURI string,
	batchSize int, enableOverwrite bool, skipHeader bool,
	maxTempFileRecordNumber int,
	opts ...common.Option) (*S3Sink, error) {

	// parse credentials
	creds, err := parseCredentials(rawCreds)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// get provider
	var provider aws.CredentialsProvider
	switch strings.ToLower(clientCredsProvider) {
	case string(xaws.TikTokProviderType):
		provider = xaws.NewTikTokProvider(creds.AWSAccessKeyID, creds.AWSSecretAccessKey, xaws.S3ResourceType)
	default:
		provider = credentials.NewStaticCredentialsProvider(creds.AWSAccessKeyID, creds.AWSSecretAccessKey, creds.AWSSessionToken)
	}

	// create S3 client uploader
	client, err := NewS3Client(commonSink.Context(), region, provider)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse destinationURI as template
	tmpl, err := compiler.NewTemplate("sink_s3_destination_uri", destinationURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}

	s3 := &S3Sink{
		Sink:                      commonSink,
		client:                    client,
		destinationURITemplate:    tmpl,
		writeHandlers:             make(map[string]xio.WriteFlusher),
		s3Handlers:                make(map[string]io.WriteCloser),
		fileRecordCounters:        make(map[string]int),
		batchSize:                 batchSize,
		enableOverwrite:           enableOverwrite,
		skipHeader:                skipHeader,
		maxTempFileRecordNumber:   maxTempFileRecordNumber,
		partialFileRecordCounters: make(map[string]int),
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s3.Logger().Info("closing S3 files")
		var e error
		for _, handler := range s3.s3Handlers {
			err := handler.Close()
			e = errs.Join(e, err)
		}
		return e
	})
	commonSink.AddCleanFunc(func() error {
		s3.Logger().Info("remove tmp files")
		var e error
		for tmpPath := range s3.writeHandlers {
			err := os.Remove(tmpPath)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			e = errs.Join(e, err)
		}
		return e
	})

	// register sink process
	commonSink.RegisterProcess(s3.process)
	return s3, nil
}

func (s3 *S3Sink) process() error {
	var destinationURI string

	logCheckPoint := 1000
	recordCounter := 0
	if s3.batchSize > 0 {
		logCheckPoint = s3.batchSize
	}

	for record, err := range s3.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		destinationURI, err = compiler.Compile(s3.destinationURITemplate, model.ToMap(record))
		if err != nil {
			s3.Logger().Error(fmt.Sprintf("failed to compile destination URI"))
			return errors.WithStack(err)
		}
		if s3.batchSize > 0 {
			// use uri with batch size for its suffix if batch size is set
			destinationURI = getDestinationURIByBatch(destinationURI, recordCounter, s3.batchSize)
		}

		// stream to tmp file
		tmpPath, err := getTmpPath(destinationURI)
		if err != nil {
			s3.Logger().Error(fmt.Sprintf("failed to get tmp URI: %s", destinationURI))
			return errors.WithStack(err)
		}
		wh, ok := s3.writeHandlers[tmpPath]
		if !ok {
			// create new tmp write handler
			wh, err = xio.NewWriteHandler(s3.Logger(), tmpPath)
			if err != nil {
				s3.Logger().Error(fmt.Sprintf("failed to create tmp write handler: %s", err.Error()))
				return errors.WithStack(err)
			}

			// create new s3 write handler
			targetDestinationURI, err := url.Parse(destinationURI)
			if err != nil {
				s3.Logger().Error(fmt.Sprintf("failed to parse destination URI: %s", destinationURI))
				return errors.WithStack(err)
			}
			if targetDestinationURI.Scheme != "s3" {
				s3.Logger().Error(fmt.Sprintf("invalid scheme: %s", targetDestinationURI.Scheme))
				return errors.WithStack(err)
			}
			// remove object if overwrite is enabled
			if _, ok := s3.s3Handlers[destinationURI]; !ok && s3.enableOverwrite {
				s3.Logger().Info(fmt.Sprintf("remove object: %s", destinationURI))
				if err := s3.Retry(func() error {
					return s3.client.DeleteObject(s3.Context(), targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
				}); err != nil {
					s3.Logger().Error(fmt.Sprintf("failed to remove object: %s", destinationURI))
					return errors.WithStack(err)
				}
			}
			var sh io.WriteCloser = s3.client.GetUploadWriter(s3.Context(), targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))

			// dual handlers for both tmp file and s3
			s3.writeHandlers[tmpPath] = wh
			s3.s3Handlers[destinationURI] = sh
		}

		// record without metadata
		recordWithoutMetadata := s3.RecordWithoutMetadata(record)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			s3.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}

		_, err = wh.Write(append(raw, '\n'))
		if err != nil {
			s3.Logger().Error(fmt.Sprintf("failed to write to file"))
			return errors.WithStack(err)
		}

		recordCounter++
		s3.fileRecordCounters[tmpPath]++
		s3.partialFileRecordCounters[destinationURI]++
		if recordCounter%logCheckPoint == 0 {
			s3.Logger().Info(fmt.Sprintf("written %d records to tmp file: %s", s3.fileRecordCounters[tmpPath], tmpPath))
		}

		// EXPERIMENTAL: flush and upload to S3 if maximum number of records in tmp file is reached.
		// a method of partial upload is implemented to avoid having large number of tmp files which leads to high disk usage.
		// as long as the streamed records from source are guaranteed to be uniform (same header & value ordering), this should work.
		if s3.maxTempFileRecordNumber > 0 && s3.partialFileRecordCounters[destinationURI] >= s3.maxTempFileRecordNumber {
			s3.Logger().Info(fmt.Sprintf("maximum number of temp file records %d reached. flushing %s to OSS", s3.maxTempFileRecordNumber, tmpPath))
			if err := wh.Flush(); err != nil {
				s3.Logger().Error(fmt.Sprintf("failed to flush tmp file: %s", tmpPath))
				return errors.WithStack(err)
			}
			s3.Logger().Info(fmt.Sprintf("flushed tmp file: %s", tmpPath))

			if err := s3.flush(destinationURI, s3.s3Handlers[destinationURI]); err != nil {
				s3.Logger().Error(fmt.Sprintf("failed to upload to OSS: %s", destinationURI))
				return errors.WithStack(err)
			}

			// reset counters and handlers for the current batch
			// except for s3 handler which is reused to upload the next part of the file
			s3.partialFileRecordCounters[destinationURI] = 0
			delete(s3.writeHandlers, tmpPath)

			if err := os.Remove(tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				s3.Logger().Error(fmt.Sprintf("failed to remove tmp file: %s", tmpPath))
				return errors.WithStack(err)
			}
		}
	}
	if recordCounter == 0 {
		s3.Logger().Info(fmt.Sprintf("no records to write"))
		return nil
	}

	// flush remaining tmp files
	for tmpPath, wh := range s3.writeHandlers {
		if err := wh.Flush(); err != nil {
			s3.Logger().Error(fmt.Sprintf("failed to flush tmp file: %s", tmpPath))
			return errors.WithStack(err)
		}
		s3.Logger().Info(fmt.Sprintf("flushed tmp file: %s", tmpPath))
	}

	// flush remaining records concurrently
	funcs := []func() error{}
	for uri := range s3.s3Handlers {
		funcs = append(funcs, func() error {
			return s3.flush(uri, s3.s3Handlers[uri])
		})
	}
	if err := s3.ConcurrentTasks(s3.Context(), 4, funcs); err != nil {
		return errors.WithStack(err)
	}

	s3.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))
	return nil
}

func (s3 *S3Sink) flush(destinationURI string, wh io.WriteCloser) error {
	tmpPath, err := getTmpPath(destinationURI)
	if err != nil {
		return errors.WithStack(err)
	}
	// open tmp file for reading
	var tmpReader io.ReadSeekCloser
	tmpReader, err = os.OpenFile(tmpPath, os.O_RDONLY, 0644)
	if err != nil {
		return errors.WithStack(err)
	}
	// header is skipped if SKIP_HEADER is explicitly set to true OR if file has been partially uploaded previously
	skipHeader := s3.skipHeader || (s3.maxTempFileRecordNumber > 0 && s3.fileRecordCounters[tmpPath] > s3.maxTempFileRecordNumber)

	// convert to appropriate format if necessary
	cleanUpFn := func() error { return nil }
	switch filepath.Ext(destinationURI) {
	case ".json":
		// do nothing
	case ".csv":
		tmpReader, cleanUpFn, err = helper.FromJSONToCSV(s3.Logger(), tmpReader, skipHeader)
	case ".tsv":
		tmpReader, cleanUpFn, err = helper.FromJSONToCSV(s3.Logger(), tmpReader, skipHeader, rune('\t'))
	case ".xlsx":
		tmpReader, cleanUpFn, err = helper.FromJSONToXLSX(s3.Logger(), tmpReader, skipHeader)
	default:
		s3.Logger().Warn(fmt.Sprintf("unsupported file format: %s, use default (json)", filepath.Ext(destinationURI)))
		// do nothing
	}
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		cleanUpFn()
		tmpReader.Close()
	}()

	// remove tmp file if destination is csv or tsv
	if filepath.Ext(destinationURI) == ".csv" || filepath.Ext(destinationURI) == ".tsv" {
		if err := os.Remove(tmpPath); err != nil {
			return errors.WithStack(err)
		}
	}
	defer tmpReader.Close()

	s3.Logger().Info(fmt.Sprintf("upload tmp file %s to s3 %s", tmpPath, destinationURI))
	n, err := io.Copy(wh, tmpReader)
	if err != nil {
		return errors.WithStack(err)
	}

	s3.Logger().Debug(fmt.Sprintf("uploaded %d bytes to s3 %s", n, destinationURI))
	return nil
}

func getTmpPath(destinationURI string) (string, error) {
	targetURI, err := url.Parse(destinationURI)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return filepath.Join("/tmp", filepath.Base(targetURI.Path)), nil
}

func getDestinationURIByBatch(destinationURI string, recordCounter, batchSize int) string {
	return fmt.Sprintf("%s.%d.%s",
		destinationURI[:len(destinationURI)-len(filepath.Ext(destinationURI))],
		int(recordCounter/batchSize)*batchSize,
		filepath.Ext(destinationURI)[1:])
}
