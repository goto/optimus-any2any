package maxcompute

import (
	"fmt"
	"io"
	"iter"
	"log/slog"
	"maps"
	"strings"
	"sync/atomic"

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
	readerId               atomic.Uint32
	query                  string
	additionalHints        map[string]string
	logViewRetentionInDays int
	instance               *odps.Instance
}

var _ RecordReaderCloser = (*mcRecordReader)(nil)

func (r *mcRecordReader) ReadRecord() iter.Seq2[*model.Record, error] {
	id := r.readerId.Add(1)
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
		r.l.Info(fmt.Sprintf("reader id: %d, running query:\n%s", id, r.query))
		r.l.Info(fmt.Sprintf("reader id: %d, execution the query", id))
		instance, err := r.client.ExecSQl(r.query, hints)
		if err != nil {
			r.l.Error(fmt.Sprintf("failed to run query: %s", r.query))
			yield(nil, errors.WithStack(err))
			return
		}
		r.instance = instance

		// generate log view
		url, err := odps.NewLogView(r.client).GenerateLogView(instance, r.logViewRetentionInDays*24)
		if err != nil {
			r.l.Error(fmt.Sprintf("reader id: %d, failed to generate log view", id))
			yield(nil, errors.WithStack(err))
			return
		}
		r.l.Info(fmt.Sprintf("reader id: %d, log view url: %s", id, url))

		// wait for query to finish
		r.l.Info(fmt.Sprintf("reader id: %d, waiting for query to finish", id))
		r.l.Info(fmt.Sprintf("reader id: %d, taskId: %s", id, instance.Id()))
		if err := instance.WaitForSuccess(); err != nil {
			r.l.Error(fmt.Sprintf("reader id: %d, query failed", id))
			yield(nil, errors.WithStack(err))
			return
		}

		// create session for reading records
		r.l.Info(fmt.Sprintf("reader id: %d, creating session for reading records", id))
		session, err := r.tunnel.CreateInstanceResultDownloadSession(r.client.DefaultProjectName(), instance.Id())
		if err != nil {
			r.l.Error(fmt.Sprintf("reader id: %d, failed to create session for reading records", id))
			yield(nil, errors.WithStack(err))
			return
		}

		recordCount := session.RecordCount()
		r.l.Info(fmt.Sprintf("reader id: %d, record count: %d", id, recordCount))
		// read records
		i := 0
		step := 1000 // batch size for reading records
		for i < recordCount {
			reader, err := session.OpenRecordReader(i, step, 0, nil)
			if err != nil {
				r.l.Error(fmt.Sprintf("reader id: %d, failed to open record reader", id))
				yield(nil, errors.WithStack(err))
				return
			}
			defer reader.Close()

			count := 0
			for {
				record, err := reader.Read()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					r.l.Error(fmt.Sprintf("reader id: %d, failed to read record", id))
					yield(nil, errors.WithStack(err))
					return
				}

				// process record
				r.l.Debug(fmt.Sprintf("reader id: %d, record: %s", id, record))
				v, err := fromRecord(r.l, record, session.Schema())
				if err != nil {
					r.l.Error(fmt.Sprintf("reader id: %d, failed to process record", id))
					yield(nil, errors.WithStack(err))
					return
				}
				count++
				if !yield(v, nil) {
					return
				}
			}
			i += count
			r.l.Info(fmt.Sprintf("reader id: %d, send %d records", id, count))
		}
	}
}

func (r *mcRecordReader) Close() error {
	if r.instance == nil {
		return nil
	}
	if err := r.instance.Load(); err != nil {
		r.l.Error(fmt.Sprintf("failed to load instance %s: %s", r.instance.Id(), err.Error()))
		return errors.WithStack(err)
	}
	if r.instance.Status() == odps.InstanceTerminated { // instance is terminated, no need to terminate again
		return nil
	}
	if err := r.instance.Terminate(); err != nil {
		r.l.Error(fmt.Sprintf("failed to terminate instance %s: %s", r.instance.Id(), err.Error()))
		return errors.WithStack(err)
	}
	return nil
}
