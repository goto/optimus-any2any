package oss

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/goccy/go-json"

	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type OSSSink struct {
	common.Sink

	// client                 *oss.Client
	destinationURITemplate *template.Template
	handlers               xio.WriteHandler
	// writeHandlers           map[string]xio.WriteFlushCloser
	// fileRecordCounters      map[string]int
	// handlers
	// batchSize               int
	// enableOverwrite         bool
	// skipHeader              bool
	// maxTempFileRecordNumber int

	// archive properties TODO: refactor
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

	// prepare handlers
	var handlers xio.WriteHandler
	if compressionType != "" {
		handlers = file.NewFileHandler(commonSink.Context(), commonSink.Logger(),
			xio.WithCSVSkipHeader(skipHeader),
		)
	} else {
		handlers = NewOSSHandler(commonSink.Context(), commonSink.Logger(),
			client, enableOverwrite,
			xio.WithCSVSkipHeader(skipHeader),
		)
	}

	o := &OSSSink{
		Sink: commonSink,
		// client:                  client,
		destinationURITemplate: tmpl,
		handlers:               handlers,
		// writeHandlers:           make(map[string]xio.WriteFlushCloser),
		// fileRecordCounters:      make(map[string]int),
		// batchSize:               batchSize,
		// enableOverwrite:         enableOverwrite,
		// skipHeader:              skipHeader,
		// maxTempFileRecordNumber: maxTempFileRecordNumber,
		// archive options
		enableArchive:       compressionType != "",
		compressionType:     compressionType,
		compressionPassword: compressionPassword,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		o.Logger().Info("closing writers")
		o.handlers.Close()
		return nil
	})

	// register sink process
	commonSink.RegisterProcess(o.process)

	return o, nil
}

func (o *OSSSink) process() error {
	recordCounter := 0
	// if o.batchSize > 0 {
	// 	logCheckPoint = o.batchSize
	// }

	for record, err := range o.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		if o.IsSpecializedMetadataRecord(record) {
			o.Logger().Debug("skip specialized metadata record")
			continue
		}

		destinationURI, err := compiler.Compile(o.destinationURITemplate, model.ToMap(record))
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to compile destination URI"))
			return errors.WithStack(err)
		}
		if o.enableArchive {
			destinationURI = "file:///tmp" + strings.TrimPrefix(destinationURI, "oss://")
		}

		// TODO: batch splitting by using templating

		// record without metadata
		recordWithoutMetadata := o.RecordWithoutMetadata(record)
		raw, err := json.MarshalWithOption(recordWithoutMetadata, json.DisableHTMLEscape())
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}

		err = o.DryRunable(func() error {
			if err := o.handlers.Write(destinationURI, raw); err != nil {
				o.Logger().Error("failed to write to file")
				return errors.WithStack(err)
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

	// flush all write handlers
	err := o.DryRunable(func() error {
		if err := o.handlers.Sync(); err != nil {
			return errors.WithStack(err)
		}
		o.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))
		return nil
	})
	if err != nil {
		return errors.WithStack(err)
	}

	if o.enableArchive {
		err := o.DryRunable(func() error {
			// TODO: archive
			// o.Compression(o.compressionType, o.compressionPassword, o.handlers.DestinationURIs())
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	_ = o.DryRunable(func() error { // ignore log when dry run
		o.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))
		return nil
	})
	return nil
}

// func getTmpPath(destinationURI string) (string, error) {
// 	targetURI, err := url.Parse(destinationURI)
// 	if err != nil {
// 		return "", errors.WithStack(err)
// 	}
// 	return filepath.Join("/tmp", filepath.Base(targetURI.Path)), nil
// }

// func getDestinationURIByBatch(destinationURI string, recordCounter, batchSize int) string {
// 	return fmt.Sprintf("%s.%d.%s",
// 		destinationURI[:len(destinationURI)-len(filepath.Ext(destinationURI))],
// 		int(recordCounter/batchSize)*batchSize,
// 		filepath.Ext(destinationURI)[1:])
// }

// func (o *OSSSink) newOSSWriter(fullPath string, shouldOverwrite bool) (io.WriteCloser, error) {
// 	// create new oss write handler
// 	targetDestinationURI, err := url.Parse(fullPath)
// 	if err != nil {
// 		o.Logger().Error(fmt.Sprintf("failed to parse destination URI: %s", fullPath))
// 		return nil, errors.WithStack(err)
// 	}
// 	if targetDestinationURI.Scheme != "oss" {
// 		o.Logger().Error(fmt.Sprintf("invalid scheme: %s", targetDestinationURI.Scheme))
// 		return nil, errors.WithStack(err)
// 	}

// 	if shouldOverwrite {
// 		err = o.DryRunable(func() error {
// 			o.Logger().Debug(fmt.Sprintf("remove object: %s", fullPath))
// 			if err := o.Retry(func() error {
// 				err := o.remove(targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
// 				return err
// 			}); err != nil {
// 				o.Logger().Error(fmt.Sprintf("failed to remove object: %s", fullPath))
// 				return errors.WithStack(err)
// 			}
// 			return nil
// 		})
// 		if err != nil {
// 			return nil, errors.WithStack(err)
// 		}
// 	}

// 	var oh io.WriteCloser
// 	err = o.Retry(func() error {
// 		var err error
// 		oh, err = oss.NewAppendFile(o.Context(), o.client, targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
// 		return errors.WithStack(err)
// 	})
// 	if err != nil {
// 		o.Logger().Error(fmt.Sprintf("failed to create oss write handler: %s", err.Error()))
// 		return nil, errors.WithStack(err)
// 	}

// 	return oh, nil
// }

// func getDestinationTempURI(destinationURI string) string {
// 	return fmt.Sprintf("%s_inprogress", destinationURI)
// }

// func (o *OSSSink) renameFile(tempURI, finalURI string) error {
// 	tempParsedURI, err := url.Parse(tempURI)
// 	if err != nil {
// 		o.Logger().Error(fmt.Sprintf("failed to parse temporary URI: %s", tempURI))
// 		return errors.WithStack(err)
// 	}

// 	finalParsedURI, err := url.Parse(finalURI)
// 	if err != nil {
// 		o.Logger().Error(fmt.Sprintf("failed to parse final URI: %s", finalURI))
// 		return errors.WithStack(err)
// 	}

// 	err = o.DryRunable(func() error {
// 		return o.Retry(func() error {
// 			if o.enableOverwrite {
// 				// Overwrite logic: Remove the existing file
// 				_, err := o.client.CopyObject(o.Context(), &oss.CopyObjectRequest{
// 					SourceBucket: oss.Ptr(tempParsedURI.Host),
// 					SourceKey:    oss.Ptr(strings.TrimLeft(tempParsedURI.Path, "/")),
// 					Bucket:       oss.Ptr(finalParsedURI.Host),
// 					Key:          oss.Ptr(strings.TrimLeft(finalParsedURI.Path, "/")),
// 				})
// 				return errors.WithStack(err)
// 			} else {
// 				// Stream-like copying logic
// 				tempFileResp, err := o.client.GetObject(o.Context(), &oss.GetObjectRequest{
// 					Bucket: oss.Ptr(tempParsedURI.Host),
// 					Key:    oss.Ptr(strings.TrimLeft(tempParsedURI.Path, "/")),
// 				})
// 				if err != nil {
// 					return errors.WithStack(err)
// 				}
// 				defer tempFileResp.Body.Close()

// 				ossWriter, err := o.newOSSWriter(finalURI, o.enableOverwrite)
// 				if err != nil {
// 					return errors.WithStack(err)
// 				}
// 				defer ossWriter.Close()

// 				_, err = io.Copy(ossWriter, tempFileResp.Body)
// 				return errors.WithStack(err)
// 			}
// 		})
// 	})
// 	if err != nil {
// 		o.Logger().Error(fmt.Sprintf("failed to copy file from %s to %s: %s", tempURI, finalURI, err.Error()))
// 		return errors.WithStack(err)
// 	}

// 	// remove the temporary file after copying
// 	if err := o.remove(tempParsedURI.Host, strings.TrimLeft(tempParsedURI.Path, "/")); err != nil {
// 		o.Logger().Error(fmt.Sprintf("failed to remove temporary file %s: %s", tempURI, err.Error()))
// 		return errors.WithStack(err)
// 	}

// 	o.Logger().Debug(fmt.Sprintf("successfully copied file from %s to %s", tempURI, finalURI))
// 	return nil
// }
