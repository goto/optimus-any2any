package s3

import (
	errs "errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/goccy/go-json"
	"github.com/goto/optimus-any2any/internal/archive"
	xaws "github.com/goto/optimus-any2any/internal/auth/aws"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type S3Sink struct {
	common.Sink

	client                  *S3Client
	destinationURITemplate  *template.Template
	writeHandlers           map[string]xio.WriteFlushCloser
	fileRecordCounters      map[string]int
	batchSize               int
	enableOverwrite         bool
	skipHeader              bool
	maxTempFileRecordNumber int

	// archive properties
	enableArchive       bool
	compressionType     string
	compressionPassword string
}

var _ flow.Sink = (*S3Sink)(nil)

// NewS3Sink creates a new S3 sink instance
func NewSink(commonSink common.Sink,
	rawCreds string, clientCredsProvider string,
	region string, destinationURI string,
	batchSize int, enableOverwrite bool, skipHeader bool,
	maxTempFileRecordNumber int,
	compressionType string, compressionPassword string,
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
		Sink:                    commonSink,
		client:                  client,
		destinationURITemplate:  tmpl,
		writeHandlers:           make(map[string]xio.WriteFlushCloser),
		fileRecordCounters:      make(map[string]int),
		batchSize:               batchSize,
		enableOverwrite:         enableOverwrite,
		skipHeader:              skipHeader,
		maxTempFileRecordNumber: maxTempFileRecordNumber,
		// archive-related options
		enableArchive:       compressionType != "",
		compressionType:     compressionType,
		compressionPassword: compressionPassword,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s3.Logger().Info("closing S3 files")
		var e error
		for _, handler := range s3.writeHandlers {
			err := handler.Close()
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

		wh, ok := s3.writeHandlers[destinationURI]
		if !ok {
			var sh io.Writer
			if s3.enableArchive {
				tmpPath, err := getTmpPath(destinationURI)
				if err != nil {
					s3.Logger().Error(fmt.Sprintf("failed to get tmp path for %s: %s", destinationURI, err.Error()))
					return errors.WithStack(err)
				}

				sh, err = xio.NewWriteHandler(s3.Logger(), tmpPath)
				if err != nil {
					s3.Logger().Error(fmt.Sprintf("failed to create write handler: %s", err.Error()))
					return errors.WithStack(err)
				}
			} else {
				// create new s3 write handler
				sh, err = s3.newS3Writer(destinationURI)
				if err != nil {
					s3.Logger().Error(fmt.Sprintf("failed to create write handler: %s", err.Error()))
					return errors.WithStack(err)
				}
			}

			wh = xio.NewChunkWriter(
				s3.Logger(), sh,
				xio.WithExtension(filepath.Ext(destinationURI)),
				xio.WithCSVSkipHeader(s3.skipHeader),
			)
			s3.writeHandlers[destinationURI] = wh
		}

		// record without metadata
		recordWithoutMetadata := s3.RecordWithoutMetadata(record)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			s3.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}

		err = s3.DryRunable(func() error {
			_, err = wh.Write(append(raw, '\n'))
			if err != nil {
				s3.Logger().Error(fmt.Sprintf("failed to write to file"))
				return errors.WithStack(err)
			}

			recordCounter++
			s3.fileRecordCounters[destinationURI]++
			if recordCounter%logCheckPoint == 0 {
				s3.Logger().Info(fmt.Sprintf("written %d records to file writer: %s", s3.fileRecordCounters[destinationURI], destinationURI))
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	if recordCounter == 0 {
		s3.Logger().Info(fmt.Sprintf("no records to write"))
		return nil
	}

	// flush remaining records concurrently
	funcs := []func() error{}
	for destinationURI, wh := range s3.writeHandlers {
		funcs = append(funcs, func() error {
			err := s3.DryRunable(func() error {
				if err := wh.Flush(); err != nil {
					s3.Logger().Error(fmt.Sprintf("failed to flush to %s", destinationURI))
					return errors.WithStack(err)
				}
				s3.Logger().Info(fmt.Sprintf("flushed file: %s", destinationURI))
				return nil
			})
			return errors.WithStack(err)
		})
	}
	if err := s3.ConcurrentTasks(s3.Context(), 4, funcs); err != nil {
		return errors.WithStack(err)
	}

	if s3.enableArchive {
		err := s3.DryRunable(func() error {
			pathsToArchive := []string{}
			for destinationURI := range s3.writeHandlers {
				tmpPath, err := getTmpPath(destinationURI)
				if err != nil {
					s3.Logger().Error(fmt.Sprintf("failed to get tmp path for %s: %s", destinationURI, err.Error()))
					return errors.WithStack(err)
				}

				pathsToArchive = append(pathsToArchive, tmpPath)
			}
			s3.Logger().Info(fmt.Sprintf("compressing %d files: %s", len(pathsToArchive), strings.Join(pathsToArchive, ", ")))

			archivePaths, err := s3.archive(pathsToArchive)
			if err != nil {
				s3.Logger().Error(fmt.Sprintf("failed to compress files: %s", err.Error()))
				return errors.WithStack(err)
			}

			s3.Logger().Info(fmt.Sprintf("successfully uploaded archive file(s) to OSS: %s", strings.Join(archivePaths, ", ")))

			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	s3.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))
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

func (s3 *S3Sink) newS3Writer(fullPath string) (io.WriteCloser, error) {
	// create new s3 write handler
	targetDestinationURI, err := url.Parse(fullPath)
	if err != nil {
		s3.Logger().Error(fmt.Sprintf("failed to parse destination URI: %s", fullPath))
		return nil, errors.WithStack(err)
	}
	if targetDestinationURI.Scheme != "s3" {
		s3.Logger().Error(fmt.Sprintf("invalid scheme: %s", targetDestinationURI.Scheme))
		return nil, errors.WithStack(err)
	}

	// remove object if overwrite is enabled
	if s3.enableOverwrite {
		if err := s3.Retry(func() error {
			return s3.DryRunable(func() error {
				s3.Logger().Info(fmt.Sprintf("remove object: %s", fullPath))
				return s3.client.DeleteObject(s3.Context(), targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
			})
		}); err != nil {
			s3.Logger().Error(fmt.Sprintf("failed to remove object: %s", fullPath))
			return nil, errors.WithStack(err)
		}
	}

	return s3.client.GetUploadWriter(
		s3.Context(), targetDestinationURI.Host,
		strings.TrimLeft(targetDestinationURI.Path, "/")), nil
}

func (s3 *S3Sink) archive(filesToArchive []string) ([]string, error) {
	templateURI := s3.destinationURITemplate.Root.String()
	destinationDir := strings.TrimRight(strings.TrimSuffix(templateURI, filepath.Base(templateURI)), "/")

	var archiveDestinationPaths []string
	switch s3.compressionType {
	case "gz":
		for _, filePath := range filesToArchive {
			fileName := fmt.Sprintf("%s.gz", filepath.Base(filePath))
			archiveDestinationPath := fmt.Sprintf("%s/%s", destinationDir, fileName)
			archiveDestinationPaths = append(archiveDestinationPaths, archiveDestinationPath)

			archiveWriter, err := s3.newS3Writer(archiveDestinationPath)
			if err != nil {
				return archiveDestinationPaths, errors.WithStack(err)
			}
			defer archiveWriter.Close()

			archiver := archive.NewFileArchiver(s3.Logger(), archive.WithExtension("gz"))
			if err := archiver.Archive([]string{filePath}, archiveWriter); err != nil {
				return archiveDestinationPaths, errors.WithStack(err)
			}
		}
	case "zip", "tar.gz":
		// for zip & tar.gz file, the whole file is archived into a single archive file
		// whose file name is deferred from the destination URI
		re := strings.NewReplacer("{{", "", "}}", "", "{{ ", "", " }}", "")
		fileName := fmt.Sprintf("%s.%s", strings.TrimSuffix(re.Replace(filepath.Base(templateURI)), filepath.Ext(templateURI)), s3.compressionType)
		archiveDestinationPath := fmt.Sprintf("%s/%s", destinationDir, fileName)
		archiveDestinationPaths = append(archiveDestinationPaths, archiveDestinationPath)

		// use appender
		archiveWriter, err := s3.newS3Writer(archiveDestinationPath)
		if err != nil {
			return archiveDestinationPaths, errors.WithStack(err)
		}
		defer archiveWriter.Close()

		archiver := archive.NewFileArchiver(s3.Logger(), archive.WithExtension(s3.compressionType), archive.WithPassword(s3.compressionPassword))
		if err := archiver.Archive(filesToArchive, archiveWriter); err != nil {
			return archiveDestinationPaths, errors.WithStack(err)
		}
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", s3.compressionType)
	}

	return archiveDestinationPaths, nil
}
