// Description: This package contains common helper functions that are used by multiple packages.
package helper

import (
	"bufio"
	"io"
	"iter"

	"github.com/goccy/go-json"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

type RecordReader struct {
	r io.Reader
}

var _ common.RecordReader = (*RecordReader)(nil)

func NewRecordReader(r io.Reader) *RecordReader {
	return &RecordReader{r: r}
}

func (r *RecordReader) ReadRecord() iter.Seq2[*model.Record, error] {
	return func(yield func(*model.Record, error) bool) {
		sc := bufio.NewScanner(r.r)
		buf := make([]byte, 0, 4*1024)
		sc.Buffer(buf, 1024*1024)

		for sc.Scan() {
			raw := sc.Bytes()
			line := make([]byte, len(raw))
			copy(line, raw)

			var record model.Record
			if err := json.Unmarshal(line, &record); err != nil {
				yield(nil, errors.WithStack(err))
				return
			}

			if !yield(&record, nil) {
				return
			}
		}
		if err := sc.Err(); err != nil {
			yield(nil, errors.WithStack(err))
		}
	}
}
