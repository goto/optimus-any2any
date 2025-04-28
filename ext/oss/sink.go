package oss

import (
	errs "errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/goccy/go-json"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
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
	writeHandlers           map[string]xio.WriteFlusher // tmp write handler
	ossHandlers             map[string]io.WriteCloser
	fileRecordCounters      map[string]int
	batchSize               int
	enableOverwrite         bool
	skipHeader              bool
	maxTempFileRecordNumber int

	partialFileRecordCounters map[string]int // map of destinationURI to the number of records
}

var _ flow.Sink = (*OSSSink)(nil)

// NewSink creates a new OSSSink
func NewSink(commonSink common.Sink,
	creds, destinationURI string,
	batchSize int, enableOverwrite bool, skipHeader bool,
	maxTempFileRecordNumber int,
	opts ...common.Option) (*OSSSink, error) {

	// create OSS client
	client, err := NewOSSClient(creds)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse destinationURI as template
	tmpl, err := compiler.NewTemplate("sink_oss_destination_uri", destinationURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}

	o := &OSSSink{
		Sink:                      commonSink,
		client:                    client,
		destinationURITemplate:    tmpl,
		writeHandlers:             make(map[string]xio.WriteFlusher),
		ossHandlers:               make(map[string]io.WriteCloser),
		fileRecordCounters:        make(map[string]int),
		partialFileRecordCounters: make(map[string]int),
		batchSize:                 batchSize,
		enableOverwrite:           enableOverwrite,
		skipHeader:                skipHeader,
		maxTempFileRecordNumber:   maxTempFileRecordNumber,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		o.Logger().Info("close oss files")
		var e error
		for destinationURI, oh := range o.ossHandlers {
			o.Logger().Debug(fmt.Sprintf("close file: %s", destinationURI))
			err := oh.Close()
			e = errs.Join(e, err)
		}
		return e
	})
	commonSink.AddCleanFunc(func() error {
		o.Logger().Info("remove tmp files")
		var e error
		for tmpPath := range o.writeHandlers {
			o.Logger().Debug(fmt.Sprintf("close tmp file: %s", tmpPath))

			o.Logger().Debug(fmt.Sprintf("remove tmp file: %s", tmpPath))
			err := os.Remove(tmpPath)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
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
		destinationURI, err = compiler.Compile(o.destinationURITemplate, model.ToMap(record))
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to compile destination URI"))
			return errors.WithStack(err)
		}

		if o.batchSize > 0 {
			// use uri with batch size for its suffix if batch size is set
			destinationURI = getDestinationURIByBatch(destinationURI, recordCounter, o.batchSize)
		}

		// stream to tmp file
		tmpPath, err := getTmpPath(destinationURI)
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to get tmp URI: %s", destinationURI))
			return errors.WithStack(err)
		}
		wh, ok := o.writeHandlers[tmpPath]
		if !ok {
			// create new tmp write handler
			wh, err = xio.NewWriteHandler(o.Logger(), tmpPath)
			if err != nil {
				o.Logger().Error(fmt.Sprintf("failed to create tmp write handler: %s", err.Error()))
				return errors.WithStack(err)
			}

			// create new oss write handler
			targetDestinationURI, err := url.Parse(destinationURI)
			if err != nil {
				o.Logger().Error(fmt.Sprintf("failed to parse destination URI: %s", destinationURI))
				return errors.WithStack(err)
			}
			if targetDestinationURI.Scheme != "oss" {
				o.Logger().Error(fmt.Sprintf("invalid scheme: %s", targetDestinationURI.Scheme))
				return errors.WithStack(err)
			}
			// remove object if overwrite is enabled
			if _, ok := o.ossHandlers[destinationURI]; !ok && o.enableOverwrite {
				o.Logger().Info(fmt.Sprintf("remove object: %s", destinationURI))
				if err := o.Retry(func() error {
					err := o.remove(targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
					return err
				}); err != nil {
					o.Logger().Error(fmt.Sprintf("failed to remove object: %s", destinationURI))
					return errors.WithStack(err)
				}
			}
			var oh io.WriteCloser
			if err := o.Retry(func() (err error) {
				oh, err = oss.NewAppendFile(o.Context(), o.client, targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
				return
			}); err != nil {
				o.Logger().Error(fmt.Sprintf("failed to create oss write handler: %s", err.Error()))
				return errors.WithStack(err)
			}

			// dual handlers for both tmp file and oss
			o.writeHandlers[tmpPath] = wh
			o.ossHandlers[destinationURI] = oh
		}

		// record without metadata
		recordWithoutMetadata := o.RecordWithoutMetadata(record)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}

		_, err = wh.Write(append(raw, '\n'))
		if err != nil {
			o.Logger().Error(fmt.Sprintf("failed to write to file"))
			return errors.WithStack(err)
		}

		recordCounter++
		o.fileRecordCounters[tmpPath]++
		o.partialFileRecordCounters[destinationURI]++
		if recordCounter%logCheckPoint == 0 {
			o.Logger().Info(fmt.Sprintf("written %d records to tmp file: %s", o.fileRecordCounters[tmpPath], tmpPath))
		}

		// EXPERIMENTAL: flush and upload to OSS if maximum number of records in tmp file is reached.
		// a method of partial upload is implemented to avoid having large number of tmp files which leads to high disk usage.
		// as long as the streamed records from source are guaranteed to be uniform (same header & value ordering), this should work.
		if o.maxTempFileRecordNumber > 0 && o.partialFileRecordCounters[destinationURI] >= o.maxTempFileRecordNumber {
			o.Logger().Debug(fmt.Sprintf("maximum number of temp file records %d reached. flushing %s to OSS", o.maxTempFileRecordNumber, tmpPath))
			if err := wh.Flush(); err != nil {
				o.Logger().Error(fmt.Sprintf("failed to flush tmp file: %s", tmpPath))
				return errors.WithStack(err)
			}
			o.Logger().Debug(fmt.Sprintf("flushed tmp file: %s", tmpPath))

			if err := o.flush(destinationURI, o.ossHandlers[destinationURI]); err != nil {
				o.Logger().Error(fmt.Sprintf("failed to upload to OSS: %s", destinationURI))
				return errors.WithStack(err)
			}

			// reset counters and handlers for the current batch
			// except for oss handler which is reused to upload the next part of the file
			o.partialFileRecordCounters[destinationURI] = 0
			delete(o.writeHandlers, tmpPath)

			if err := os.Remove(tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				o.Logger().Error(fmt.Sprintf("failed to remove tmp file: %s", tmpPath))
				return errors.WithStack(err)
			}
		}
	}

	if recordCounter == 0 {
		o.Logger().Info(fmt.Sprintf("no records to write"))
		return nil
	}

	// flush remaining tmp files
	for tmpPath, wh := range o.writeHandlers {
		if err := wh.Flush(); err != nil {
			o.Logger().Error(fmt.Sprintf("failed to flush tmp file: %s", tmpPath))
			return errors.WithStack(err)
		}
		o.Logger().Info(fmt.Sprintf("flushed tmp file: %s", tmpPath))
	}

	// flush remaining records concurrently
	funcs := []func() error{}
	for uri := range o.ossHandlers {
		funcs = append(funcs, func() error {
			return o.flush(uri, o.ossHandlers[uri])
		})
	}
	if err := o.ConcurrentTasks(o.Context(), 4, funcs); err != nil {
		return errors.WithStack(err)
	}

	o.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))
	return nil
}

func (o *OSSSink) remove(bucket, path string) error {
	response, err := o.client.DeleteObject(o.Context(), &oss.DeleteObjectRequest{
		Bucket: oss.Ptr(bucket),
		Key:    oss.Ptr(path),
	})
	if err != nil {
		return errors.WithStack(err)
	}
	if response.StatusCode >= 400 {
		err := errors.New(fmt.Sprintf("failed to delete object: %d", response.StatusCode))
		return errors.WithStack(err)
	}
	o.Logger().Info(fmt.Sprintf("delete %s objects", path))
	return nil
}

func (o *OSSSink) flush(destinationURI string, oh io.WriteCloser) error {
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
	// skipHeader := o.skipHeader || (o.maxTempFileRecordNumber > 0 && o.fileRecordCounters[tmpPath] > o.maxTempFileRecordNumber)

	// convert to appropriate format if necessary
	cleanUpFn := func() error { return nil }
	switch filepath.Ext(destinationURI) {
	case ".json":
		// do nothing
	case ".csv":
	// 	tmpReader, cleanUpFn, err = helper.FromJSONToCSV(o.Logger(), tmpReader, skipHeader)
	// case ".tsv":
	// 	tmpReader, cleanUpFn, err = helper.FromJSONToCSV(o.Logger(), tmpReader, skipHeader, rune('\t'))
	// case ".xlsx":
	// 	tmpReader, cleanUpFn, err = helper.FromJSONToXLSX(o.Logger(), tmpReader, skipHeader)
	default:
		o.Logger().Warn(fmt.Sprintf("unsupported file format: %s, use default (json)", filepath.Ext(destinationURI)))
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

	// previously there was a retry when flushing the records to OSS.
	// but retrying on a failed write (e.g. network error) may cause the reader to skip some records
	// as the read pointer is not reset to the beginning of the file.
	// will get back into this later until we find a better solution.
	o.Logger().Info(fmt.Sprintf("upload tmp file %s to oss %s", tmpPath, destinationURI))
	n, err := io.Copy(oh, tmpReader)
	if err != nil {
		return errors.WithStack(err)
	}

	o.Logger().Debug(fmt.Sprintf("uploaded %d bytes to oss %s", n, destinationURI))
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
