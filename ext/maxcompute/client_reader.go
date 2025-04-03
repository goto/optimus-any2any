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

type mcRecordReader struct {
	l               *slog.Logger
	client          *odps.Odps
	tunnel          *tunnel.Tunnel
	query           string
	additionalHints map[string]string
}

var _ common.RecordReader = (*mcRecordReader)(nil)

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
		r.l.Info(fmt.Sprintf("running query:\n%s", r.query))
		instance, err := r.client.ExecSQl(r.query, hints)
		if err != nil {
			r.l.Error(fmt.Sprintf("failed to run query: %s", r.query))
			yield(nil, errors.WithStack(err))
			return
		}

		// wait for query to finish
		r.l.Info(fmt.Sprintf("waiting for query to finish"))
		if err := instance.WaitForSuccess(); err != nil {
			r.l.Error(fmt.Sprintf("query failed"))
			yield(nil, errors.WithStack(err))
			return
		}

		// create session for reading records
		r.l.Info(fmt.Sprintf("creating session for reading records"))
		session, err := r.tunnel.CreateInstanceResultDownloadSession(r.client.DefaultProjectName(), instance.Id())
		if err != nil {
			r.l.Error(fmt.Sprintf("failed to create session for reading records"))
			yield(nil, errors.WithStack(err))
			return
		}

		recordCount := session.RecordCount()
		r.l.Info(fmt.Sprintf("record count: %d", recordCount))
		// read records
		i := 0
		step := 1000 // batch size for reading records
		for i < recordCount {
			reader, err := session.OpenRecordReader(i, step, 0, nil)
			if err != nil {
				r.l.Error(fmt.Sprintf("failed to open record reader"))
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
					r.l.Error(fmt.Sprintf("failed to read record"))
					yield(nil, errors.WithStack(err))
					return
				}

				// process record
				r.l.Debug(fmt.Sprintf("record: %s", record))
				v, err := fromRecord(r.l, record, session.Schema())
				if err != nil {
					r.l.Error(fmt.Sprintf("failed to process record"))
					yield(nil, errors.WithStack(err))
					return
				}
				count++
				if !yield(v, nil) {
					return
				}
			}
			i += count
			r.l.Info(fmt.Sprintf("send %d records", count))
		}
	}
}
