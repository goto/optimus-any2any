package maxcompute

import (
	"fmt"
	"io"
	"iter"
	"log/slog"
	"maps"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

type RecordReaderCloser interface {
	common.RecordReader
	io.Closer
}

type mcRecordReader struct {
	l                      *slog.Logger
	client                 *odps.Odps
	tunnel                 *tunnel.Tunnel
	instance               *odps.Instance
	readerId               string
	query                  string
	additionalHints        map[string]string
	logViewRetentionInDays int
	retryFunc              func(f func() error) error
	batchSize              int
}

var _ RecordReaderCloser = (*mcRecordReader)(nil)

func NewRecordReader(l *slog.Logger, client *odps.Odps, tunnel *tunnel.Tunnel, query string) (*mcRecordReader, error) {
	return &mcRecordReader{
		l:        l,
		client:   client,
		tunnel:   tunnel,
		query:    query,
		instance: nil,
		// default values
		readerId:               "",
		additionalHints:        map[string]string{},
		logViewRetentionInDays: 2,
		retryFunc:              func(f func() error) error { return common.Retry(l, 3, 1000, f) },
		batchSize:              1000,
	}, nil
}

func (r *mcRecordReader) SetReaderId(id string) {
	r.readerId = id
}

func (r *mcRecordReader) SetLogViewRetentionInDays(days int) {
	r.logViewRetentionInDays = days
}

func (r *mcRecordReader) SetBatchSize(size int) {
	r.batchSize = size
}

func (r *mcRecordReader) ReadRecord() iter.Seq2[*model.Record, error] {
	// prepare hints
	hints := map[string]string{}
	maps.Copy(hints, r.additionalHints)
	if strings.Contains(r.query, ";") {
		hints["odps.sql.submit.mode"] = "script"
	}
	return func(yield func(*model.Record, error) bool) {
		if r.query == "" {
			yield(model.NewRecord(), nil)
			return
		}

		// run query
		r.l.Info(fmt.Sprintf("reader(%s): running query:\n%s", r.readerId, r.query))
		r.l.Info(fmt.Sprintf("reader(%s): executing the query", r.readerId))
		var instance *odps.Instance
		if err := r.retryFunc(func() (err error) {
			instance, err = r.client.ExecSQl(r.query, hints)
			return
		}); err != nil {
			r.l.Error(fmt.Sprintf("failed to run query: %s", r.query))
			yield(nil, errors.WithStack(err))
			return
		}
		r.instance = instance

		// generate log view
		var url string
		if err := r.retryFunc(func() (err error) {
			url, err = generateLogView(r.l, r.client, instance, r.logViewRetentionInDays)
			return
		}); err != nil {
			r.l.Error(fmt.Sprintf("reader(%s): failed to generate log view", r.readerId))
			yield(nil, errors.WithStack(err))
			return
		}
		r.l.Info(fmt.Sprintf("reader(%s): log view url: %s", r.readerId, url))

		// wait for query to finish
		r.l.Info(fmt.Sprintf("reader(%s): waiting for query to finish", r.readerId))
		r.l.Info(fmt.Sprintf("reader(%s): instanceId: %s", r.readerId, instance.Id()))
		if err := r.retryFunc(instance.WaitForSuccess); err != nil {
			r.l.Error(fmt.Sprintf("reader(%s): query failed: logview detail: %s", r.readerId, url))
			yield(nil, errors.WithStack(err))
			return
		}
		r.l.Info(fmt.Sprintf("reader(%s): query finished", r.readerId))

		// create session for reading records
		r.l.Info(fmt.Sprintf("reader(%s): creating session for reading records", r.readerId))
		var session *tunnel.InstanceResultDownloadSession
		if err := r.retryFunc(func() (err error) {
			session, err = r.tunnel.CreateInstanceResultDownloadSession(r.client.DefaultProjectName(), instance.Id())
			return
		}); err != nil {
			r.l.Error(fmt.Sprintf("reader(%s): failed to create session for reading records", r.readerId))
			yield(nil, errors.WithStack(err))
			return
		}

		recordCount := session.RecordCount()
		r.l.Info(fmt.Sprintf("reader(%s): record count: %d", r.readerId, recordCount))
		// read records
		i := 0
		for i < recordCount {
			// read records
			var reader *tunnel.RecordProtocReader
			if err := r.retryFunc(func() (err error) {
				reader, err = session.OpenRecordReader(i, r.batchSize, 0, nil)
				return
			}); err != nil {
				r.l.Error(fmt.Sprintf("reader(%s): failed to open record reader", r.readerId))
				yield(nil, errors.WithStack(err))
				return
			}

			count := 0
			for {
				// read record
				record, err := reader.Read()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					r.l.Error(fmt.Sprintf("reader(%s): failed to read record", r.readerId))
					yield(nil, errors.WithStack(err))
					return
				}

				// process record
				r.l.Debug(fmt.Sprintf("reader(%s): record: %s", r.readerId, record))
				v, err := fromRecord(r.l, record, session.Schema())
				if err != nil {
					r.l.Error(fmt.Sprintf("reader(%s): failed to process record", r.readerId))
					yield(nil, errors.WithStack(err))
					return
				}
				count++
				if !yield(v, nil) {
					return
				}
			}
			if err := reader.Close(); err != nil {
				r.l.Error(fmt.Sprintf("reader(%s): failed to close record reader: %s", r.readerId, err.Error()))
				yield(nil, errors.WithStack(err))
				return
			}

			i += count
			r.l.Info(fmt.Sprintf("reader(%s): send %d records", r.readerId, count))
		}
	}
}

func (r *mcRecordReader) Close() error {
	if r.instance == nil {
		return nil
	}
	if err := r.retryFunc(r.instance.Load); err != nil {
		r.l.Error(fmt.Sprintf("failed to load instance %s: %s", r.instance.Id(), err.Error()))
		return errors.WithStack(err)
	}
	if r.instance.Status() == odps.InstanceTerminated { // instance is terminated, no need to terminate again
		r.l.Info(fmt.Sprintf("reader(%s): success terminating instance %s", r.readerId, r.instance.Id()))
		return nil
	}
	r.l.Info(fmt.Sprintf("reader(%s): trying to terminate instance %s", r.readerId, r.instance.Id()))
	if err := r.retryFunc(r.instance.Terminate); err != nil {
		r.l.Error(fmt.Sprintf("failed to terminate instance %s: %s", r.instance.Id(), err.Error()))
		return errors.WithStack(err)
	}
	r.l.Info(fmt.Sprintf("reader(%s): success terminating instance %s", r.readerId, r.instance.Id()))
	return nil
}
