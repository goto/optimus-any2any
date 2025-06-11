package oss

import (
	errs "errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/goccy/go-json"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/goto/optimus-any2any/internal/archive"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type OSSSink struct {
	common.Sink

	client                  *oss.Client
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

var _ flow.Sink = (*OSSSink)(nil)

// NewSink creates a new OSSSink
func NewSink(commonSink common.Sink,
	creds, destinationURI string,
	batchSize int, enableOverwrite bool, skipHeader bool,
	maxTempFileRecordNumber int,
	compressionType string, compressionPassword string,
	connectionTimeout, readWriteTimeout int,
	opts ...common.Option) (*OSSSink, error) {

	// create OSS client
	client, err := NewOSSClient(creds, OSSClientConfig{
		ConnectionTimeoutSeconds: connectionTimeout,
		ReadWriteTimeoutSeconds:  readWriteTimeout,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse destinationURI as template
	tmpl, err := compiler.NewTemplate("sink_oss_destination_uri", destinationURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}

	o := &OSSSink{
		Sink:                    commonSink,
		client:                  client,
		destinationURITemplate:  tmpl,
		writeHandlers:           make(map[string]xio.WriteFlushCloser),
		fileRecordCounters:      make(map[string]int),
		batchSize:               batchSize,
		enableOverwrite:         enableOverwrite,
		skipHeader:              skipHeader,
		maxTempFileRecordNumber: maxTempFileRecordNumber,
		// archive options
		enableArchive:       compressionType != "",
		compressionType:     compressionType,
		compressionPassword: compressionPassword,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		o.Logger().Info("closing oss writers")
		var e error
		for destURI, wh := range o.writeHandlers {
			o.Logger().Debug(fmt.Sprintf("close handlers: %s", destURI))
			err := o.DryRunable(wh.Close)
			e = errs.Join(e, err)
		}
		return e
	})

	// register sink process
	commonSink.RegisterProcess(o.process)

	return o, nil
}

func (o *OSSSink) process() error {
	var destinationURI string

	logCheckPoint := 1000
	recordCounter := 0
	if o.batchSize > 0 {
		logCheckPoint = o.batchSize
	}

	for record, err := range o.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		if o.IsSpecializedMetadataRecord(record) {
			o.Logger().Debug("skip specialized metadata record")
			continue
		}

		destinationURI, err = compiler.Compile(o.destinationURITemplate, model.ToMap(record))
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to compile destination URI"))
			return errors.WithStack(err)
		}

		if o.batchSize > 0 {
			// use uri with batch size for its suffix if batch size is set
			destinationURI = getDestinationURIByBatch(destinationURI, recordCounter, o.batchSize)
		}

		// create file write handlers
		wh, ok := o.writeHandlers[destinationURI]
		if !ok {
			var oh io.Writer
			if o.enableArchive {
				tmpPath, err := getTmpPath(destinationURI)
				if err != nil {
					o.Logger().Error(fmt.Sprintf("failed to get tmp path for %s: %s", destinationURI, err.Error()))
					return errors.WithStack(err)
				}

				oh, err = xio.NewWriteHandler(o.Logger(), tmpPath)
				if err != nil {
					o.Logger().Error(fmt.Sprintf("failed to create write handler: %s", err.Error()))
					return errors.WithStack(err)
				}
			} else {
				// create new oss write handler
				// all files will be written to a temporary URI first
				// and then copied to the final destination URI
				destinationURITemp := getDestinationTempURI(destinationURI)
				oh, err = o.newOSSWriter(destinationURITemp, o.enableOverwrite)
				if err != nil {
					o.Logger().Error(fmt.Sprintf("failed to create oss write handler: %s", err.Error()))
					return errors.WithStack(err)
				}
			}

			wh = xio.NewChunkWriter(
				o.Logger(), oh,
				xio.WithExtension(filepath.Ext(destinationURI)),
				xio.WithCSVSkipHeader(o.skipHeader),
			)
			o.writeHandlers[destinationURI] = wh
		}

		// record without metadata
		recordWithoutMetadata := o.RecordWithoutMetadata(record)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}

		err = o.DryRunable(func() error {
			_, err = wh.Write(append(raw, '\n'))
			if err != nil {
				o.Logger().Error(fmt.Sprintf("failed to write to file"))
				return errors.WithStack(err)
			}

			recordCounter++
			o.fileRecordCounters[destinationURI]++
			if recordCounter%logCheckPoint == 0 {
				o.Logger().Info(fmt.Sprintf("written %d records to file writer: %s", o.fileRecordCounters[destinationURI], destinationURI))
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	if recordCounter == 0 {
		o.Logger().Info(fmt.Sprintf("no records to write"))
		return nil
	}

	// flush remaining records concurrently
	funcs := []func() error{}
	for destinationURI, wh := range o.writeHandlers {
		funcs = append(funcs, func() error {
			err := o.DryRunable(func() error {
				if err := wh.Flush(); err != nil {
					o.Logger().Error(fmt.Sprintf("failed to flush to %s", destinationURI))
					return errors.WithStack(err)
				}
				o.Logger().Info(fmt.Sprintf("flushed %d records to file: %s", o.fileRecordCounters[destinationURI], destinationURI))
				return nil
			})
			return errors.WithStack(err)
		})
	}
	if err := o.ConcurrentTasks(funcs); err != nil {
		return errors.WithStack(err)
	}

	if o.enableArchive {
		err := o.DryRunable(func() error {
			pathsToArchive := []string{}
			for destinationURI := range o.writeHandlers {
				tmpPath, err := getTmpPath(destinationURI)
				if err != nil {
					o.Logger().Error(fmt.Sprintf("failed to get tmp path for %s: %s", destinationURI, err.Error()))
					return errors.WithStack(err)
				}

				pathsToArchive = append(pathsToArchive, tmpPath)
			}
			o.Logger().Info(fmt.Sprintf("compressing %d files: %s", len(pathsToArchive), strings.Join(pathsToArchive, ", ")))

			archivePaths, err := o.archive(pathsToArchive)
			if err != nil {
				o.Logger().Error(fmt.Sprintf("failed to compress files: %s", err.Error()))
				return errors.WithStack(err)
			}

			o.Logger().Info(fmt.Sprintf("successfully uploaded archive file(s) to OSS: %s", strings.Join(archivePaths, ", ")))

			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	} else {
		for destinationURI := range o.writeHandlers {
			// copy the temporary file to the final destination
			if err := o.renameFile(getDestinationTempURI(destinationURI), destinationURI); err != nil {
				o.Logger().Error(fmt.Sprintf("failed to copy file from temporary URI to final destination: %s", err.Error()))
				return errors.WithStack(err)
			}
		}
	}

	_ = o.DryRunable(func() error { // ignore log when dry run
		o.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))
		return nil
	})
	return nil
}

func (o *OSSSink) remove(bucket, path string) error {
	var response *oss.DeleteObjectResult
	var err error
	// remove object
	err = o.DryRunable(func() error {
		response, err = o.client.DeleteObject(o.Context(), &oss.DeleteObjectRequest{
			Bucket: oss.Ptr(bucket),
			Key:    oss.Ptr(path),
		})
		return errors.WithStack(err)
	})
	if err != nil {
		return errors.WithStack(err)
	}
	if response.StatusCode >= 400 {
		err := errors.New(fmt.Sprintf("failed to delete object: %d", response.StatusCode))
		return errors.WithStack(err)
	}
	o.Logger().Debug(fmt.Sprintf("delete %s objects", path))
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

func (o *OSSSink) archive(filesToArchive []string) ([]string, error) {
	templateURI := o.destinationURITemplate.Root.String()
	destinationDir := strings.TrimRight(strings.TrimSuffix(templateURI, filepath.Base(templateURI)), "/")

	var archiveDestinationPaths []string
	switch o.compressionType {
	case "gz":
		for _, filePath := range filesToArchive {
			fileName := fmt.Sprintf("%s.gz", filepath.Base(filePath))
			archiveDestinationPath := fmt.Sprintf("%s/%s", destinationDir, fileName)
			archiveDestinationPaths = append(archiveDestinationPaths, archiveDestinationPath)

			tmpPath := getDestinationTempURI(archiveDestinationPath)
			archiveWriter, err := o.newOSSWriter(tmpPath, o.enableOverwrite)
			if err != nil {
				return archiveDestinationPaths, errors.WithStack(err)
			}
			defer archiveWriter.Close()

			archiver := archive.NewFileArchiver(o.Logger(), archive.WithExtension("gz"))
			if err := archiver.Archive([]string{filePath}, archiveWriter); err != nil {
				return archiveDestinationPaths, errors.WithStack(err)
			}
		}
	case "zip", "tar.gz":
		// for zip & tar.gz file, the whole file is archived into a single archive file
		// whose file name is deferred from the destination URI
		re := strings.NewReplacer("{{", "", "}}", "", "{{ ", "", " }}", "")
		fileName := fmt.Sprintf("%s.%s", strings.TrimSuffix(re.Replace(filepath.Base(templateURI)), filepath.Ext(templateURI)), o.compressionType)
		archiveDestinationPath := fmt.Sprintf("%s/%s", destinationDir, fileName)
		archiveDestinationPaths = append(archiveDestinationPaths, archiveDestinationPath)

		// use appender
		tmpPath := getDestinationTempURI(archiveDestinationPath)
		archiveWriter, err := o.newOSSWriter(tmpPath, o.enableOverwrite)
		if err != nil {
			return archiveDestinationPaths, errors.WithStack(err)
		}
		defer archiveWriter.Close()

		archiver := archive.NewFileArchiver(o.Logger(), archive.WithExtension(o.compressionType), archive.WithPassword(o.compressionPassword))
		if err := archiver.Archive(filesToArchive, archiveWriter); err != nil {
			return archiveDestinationPaths, errors.WithStack(err)
		}
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", o.compressionType)
	}

	// copy the temporary file to the final destination
	for _, archiveDestinationPath := range archiveDestinationPaths {
		if err := o.renameFile(getDestinationTempURI(archiveDestinationPath), archiveDestinationPath); err != nil {
			o.Logger().Error(fmt.Sprintf("failed to copy file from temporary URI to final destination: %s", err.Error()))
			return archiveDestinationPaths, errors.WithStack(err)
		}
	}

	return archiveDestinationPaths, nil
}

func (o *OSSSink) newOSSWriter(fullPath string, shouldOverwrite bool) (io.WriteCloser, error) {
	// create new oss write handler
	targetDestinationURI, err := url.Parse(fullPath)
	if err != nil {
		o.Logger().Error(fmt.Sprintf("failed to parse destination URI: %s", fullPath))
		return nil, errors.WithStack(err)
	}
	if targetDestinationURI.Scheme != "oss" {
		o.Logger().Error(fmt.Sprintf("invalid scheme: %s", targetDestinationURI.Scheme))
		return nil, errors.WithStack(err)
	}

	if shouldOverwrite {
		err = o.DryRunable(func() error {
			o.Logger().Debug(fmt.Sprintf("remove object: %s", fullPath))
			if err := o.Retry(func() error {
				err := o.remove(targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
				return err
			}); err != nil {
				o.Logger().Error(fmt.Sprintf("failed to remove object: %s", fullPath))
				return errors.WithStack(err)
			}
			return nil
		})
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	var oh io.WriteCloser
	err = o.Retry(func() error {
		var err error
		oh, err = oss.NewAppendFile(o.Context(), o.client, targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
		return errors.WithStack(err)
	})
	if err != nil {
		o.Logger().Error(fmt.Sprintf("failed to create oss write handler: %s", err.Error()))
		return nil, errors.WithStack(err)
	}

	return oh, nil
}

func getDestinationTempURI(destinationURI string) string {
	return fmt.Sprintf("%s_inprogress", destinationURI)
}

func (o *OSSSink) renameFile(tempURI, finalURI string) error {
	tempParsedURI, err := url.Parse(tempURI)
	if err != nil {
		o.Logger().Error(fmt.Sprintf("failed to parse temporary URI: %s", tempURI))
		return errors.WithStack(err)
	}

	finalParsedURI, err := url.Parse(finalURI)
	if err != nil {
		o.Logger().Error(fmt.Sprintf("failed to parse final URI: %s", finalURI))
		return errors.WithStack(err)
	}

	err = o.DryRunable(func() error {
		return o.Retry(func() error {
			if o.enableOverwrite {
				// Overwrite logic: Remove the existing file
				_, err := o.client.CopyObject(o.Context(), &oss.CopyObjectRequest{
					SourceBucket: oss.Ptr(tempParsedURI.Host),
					SourceKey:    oss.Ptr(strings.TrimLeft(tempParsedURI.Path, "/")),
					Bucket:       oss.Ptr(finalParsedURI.Host),
					Key:          oss.Ptr(strings.TrimLeft(finalParsedURI.Path, "/")),
				})
				return errors.WithStack(err)
			} else {
				// Stream-like copying logic
				tempFileResp, err := o.client.GetObject(o.Context(), &oss.GetObjectRequest{
					Bucket: oss.Ptr(tempParsedURI.Host),
					Key:    oss.Ptr(strings.TrimLeft(tempParsedURI.Path, "/")),
				})
				if err != nil {
					return errors.WithStack(err)
				}
				defer tempFileResp.Body.Close()

				ossWriter, err := o.newOSSWriter(finalURI, o.enableOverwrite)
				if err != nil {
					return errors.WithStack(err)
				}
				defer ossWriter.Close()

				_, err = io.Copy(ossWriter, tempFileResp.Body)
				return errors.WithStack(err)
			}
		})
	})
	if err != nil {
		o.Logger().Error(fmt.Sprintf("failed to copy file from %s to %s: %s", tempURI, finalURI, err.Error()))
		return errors.WithStack(err)
	}

	// remove the temporary file after copying
	if err := o.remove(tempParsedURI.Host, strings.TrimLeft(tempParsedURI.Path, "/")); err != nil {
		o.Logger().Error(fmt.Sprintf("failed to remove temporary file %s: %s", tempURI, err.Error()))
		return errors.WithStack(err)
	}

	o.Logger().Debug(fmt.Sprintf("successfully copied file from %s to %s", tempURI, finalURI))
	return nil
}
