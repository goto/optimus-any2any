// Description: This package contains common helper functions that are used by multiple packages.
package helper

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"math"
	"os"
	"strconv"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/goccy/go-json"
	"github.com/google/uuid"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

func FromJSONToCSV(l *slog.Logger, reader io.ReadSeeker, skipHeader bool, delimiter ...rune) io.Reader {
	id, err := uuid.NewV7()
	if err != nil {
		l.Error(fmt.Sprintf("failed to generate uuid: %v, skip converting", err))
		return reader
	}
	f, err := os.OpenFile(fmt.Sprintf("/tmp/%s.csv", id), os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		l.Error(fmt.Sprintf("failed to open file: %v, skip converting", err))
		return reader
	}
	l.Info(fmt.Sprintf("converting json to csv to tmp file: %s", f.Name()))
	if err := ToCSV(l, f, reader, skipHeader, delimiter...); err != nil {
		l.Error(fmt.Sprintf("failed to convert json to csv: %v", err))
	}
	return f
}

func FromCSVToJSON(l *slog.Logger, reader io.ReadSeeker, skipHeader bool, delimiter ...rune) io.Reader {
	csvReader := csv.NewReader(reader)
	if len(delimiter) > 0 {
		csvReader.Comma = delimiter[0]
	}

	buf := buffer.New(32 * 1024)
	r, w := nio.Pipe(buf)
	go func(w io.WriteCloser) {
		defer w.Close()

		headers := []string{}
		isHeader := true
		for record, err := csvReader.Read(); err == nil; record, err = csvReader.Read() {
			if isHeader {
				isHeader = false
				if !skipHeader {
					headers = record
					continue
				}
				for i := range record {
					headers = append(headers, fmt.Sprintf("%d", i))
				}
			}
			recordResult := model.NewRecord()
			for i, header := range headers {
				var val interface{}
				if raw, err := strconv.ParseBool(record[i]); err == nil {
					val = raw
				} else if raw, err := strconv.ParseFloat(record[i], 64); err == nil {
					val = raw
				} else if raw, err := strconv.ParseInt(record[i], 10, 64); err == nil {
					val = raw
				} else {
					val = record[i]
				}
				recordResult.Set(header, val)
			}
			raw, err := json.Marshal(recordResult)
			if err != nil {
				l.Error(fmt.Sprintf("failed to marshal json: %v", err))
				continue
			}
			if _, err := w.Write(append(raw, '\n')); err != nil {
				l.Error(fmt.Sprintf("failed to write to pipe: %v", err))
				continue
			}
		}
	}(w)

	return r
}

type recordReadSeeker struct {
	r io.ReadSeeker
}

var _ common.RecordReader = (*recordReadSeeker)(nil)

func (r *recordReadSeeker) ReadRecord() iter.Seq2[*model.Record, error] {
	return func(yield func(*model.Record, error) bool) {
		sc := bufio.NewScanner(r.r)
		for sc.Scan() {
			raw := sc.Bytes()
			line := make([]byte, len(raw))
			copy(line, raw)

			var record model.Record
			if err := json.Unmarshal(line, &record); err != nil {
				yield(nil, errors.WithStack(err))
				break
			}

			if !yield(&record, nil) {
				break
			}
		}
	}
}

// ToCSV converts the records to CSV.
func ToCSV(l *slog.Logger, w io.Writer, r io.ReadSeeker, skipHeader bool, delimiter ...rune) error {
	reader := &recordReadSeeker{r: r}
	// get the header
	headerMap := model.NewRecord()
	for record, err := range reader.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		for k := range record.AllFromFront() {
			headerMap.Set(k, true)
		}
	}
	header := make([]string, 0, headerMap.Len())
	for k := range headerMap.AllFromFront() {
		header = append(header, k)
	}

	l.Debug(fmt.Sprintf("common: csv header: %v", header))
	csvWriter := csv.NewWriter(w)
	if len(delimiter) > 0 {
		csvWriter.Comma = delimiter[0]
	}
	defer csvWriter.Flush()

	if !skipHeader {
		if err := csvWriter.Write(header); err != nil {
			return errors.WithStack(err)
		}
	}

	// reset the reader
	_, err := reader.r.Seek(0, io.SeekStart)
	if err != nil {
		return errors.WithStack(err)
	}
	// convert the records to string
	for record, err := range reader.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		mapString, err := convertRecordToMapString(record)
		if err != nil {
			return errors.WithStack(err)
		}
		recordString := []string{}
		for _, k := range header {
			if v := mapString.GetOrDefault(k, nil); v != nil {
				if val, ok := v.(string); ok {
					recordString = append(recordString, val)
					continue
				}
				recordString = append(recordString, "")
			}
		}
		l.Debug(fmt.Sprintf("common: csv record: %v", recordString))
		if err := csvWriter.Write(recordString); err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func convertRecordToMapString(record *model.Record) (*model.Record, error) {
	recordString := model.NewRecord()
	for k, v := range record.AllFromFront() {
		val, err := convertValueToString(v)
		if err != nil {
			return recordString, errors.WithStack(err)
		}
		recordString.Set(k, val)
	}
	return recordString, nil
}

func convertValueToString(v interface{}) (string, error) {
	switch val := v.(type) {
	case bool:
		return fmt.Sprintf("%t", val), nil
	case int:
		return fmt.Sprintf("%d", val), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case float64:
		// Check if it's a whole number
		if val == math.Trunc(val) {
			return fmt.Sprintf("%.0f", val), nil
		}
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case float32:
		if float64(val) == math.Trunc(float64(val)) {
			return fmt.Sprintf("%.0f", val), nil
		}
		return strconv.FormatFloat(float64(val), 'f', -1, 32), nil
	case string:
		return val, nil
	case map[string]interface{}, []interface{}:
		b, err := json.Marshal(val)
		if err != nil {
			return "", err
		}
		return string(b), nil
	case nil:
		return "", nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}
