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
	"github.com/xuri/excelize/v2"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

func FromJSONToXLSX(l *slog.Logger, reader io.ReadSeeker, skipHeader bool) (io.Reader, func() error, error) {
	r, c, err := FromJSONToCSV(l, reader, skipHeader)
	if err != nil {
		l.Error(fmt.Sprintf("failed to convert json to csv: %v, skip converting", err))
		return reader, func() error { return nil }, errors.WithStack(err)
	}
	cleanUpFn := func() error { return c() }
	csvReader := csv.NewReader(r)
	csvReader.Comma = ','

	f, err := os.CreateTemp(os.TempDir(), "xlsx-*")
	l.Debug(fmt.Sprintf("converting csv to xlsx to tmp file: %s", f.Name()))
	if err != nil {
		l.Error(fmt.Sprintf("failed to create temp file: %v, skip converting", err))
		return reader, cleanUpFn, errors.WithStack(err)
	}
	xlsxFile := excelize.NewFile()
	cleanUpFn = func() error {
		c()
		f.Close()
		os.Remove(f.Name())
		xlsxFile.Close()
		l.Debug(fmt.Sprintf("clean up tmp file: %s", f.Name()))
		return nil
	}

	streamWriter, err := xlsxFile.NewStreamWriter("Sheet1")
	if err != nil {
		l.Error(fmt.Sprintf("failed to add sheet: %v, skip converting", err))
		return reader, cleanUpFn, errors.WithStack(err)
	}

	rowIdx := 0
	for record, err := csvReader.Read(); err == nil; record, err = csvReader.Read() {
		cell, err := excelize.CoordinatesToCellName(1, rowIdx+1)
		if err != nil {
			l.Error(fmt.Sprintf("failed to convert coordinates to cell name: %v, skip converting", err))
			return reader, cleanUpFn, errors.WithStack(err)
		}
		rowIdx++
		values := make([]interface{}, len(record))
		for i, field := range record {
			values[i] = field
		}
		if err := streamWriter.SetRow(cell, values); err != nil {
			l.Error(fmt.Sprintf("failed to set row: %v, skip converting", err))
			return reader, cleanUpFn, errors.WithStack(err)
		}
	}

	if err := streamWriter.Flush(); err != nil {
		l.Error(fmt.Sprintf("failed to flush stream writer: %v, skip converting", err))
		return reader, cleanUpFn, errors.WithStack(err)
	}
	if err := xlsxFile.Write(f); err != nil {
		l.Error(fmt.Sprintf("failed to write xlsx file: %v, skip converting", err))
		return reader, cleanUpFn, errors.WithStack(err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		l.Error(fmt.Sprintf("failed to reset seek: %v, skip converting", err))
		return reader, cleanUpFn, errors.WithStack(err)
	}

	return f, cleanUpFn, nil
}

func FromJSONToCSV(l *slog.Logger, reader io.ReadSeeker, skipHeader bool, delimiter ...rune) (io.Reader, func() error, error) {
	cleanUpFn := func() error { return nil }
	f, err := os.CreateTemp(os.TempDir(), "csv-*")
	if err != nil {
		l.Error(fmt.Sprintf("failed to open file: %v, skip converting", err))
		return reader, cleanUpFn, errors.WithStack(err)
	}
	l.Debug(fmt.Sprintf("converting json to csv to tmp file: %s", f.Name()))
	if err := ToCSV(l, f, reader, skipHeader, delimiter...); err != nil {
		l.Error(fmt.Sprintf("failed to convert json to csv: %v, skip converting", err))
		f.Close()
		return reader, cleanUpFn, errors.WithStack(err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		l.Error(fmt.Sprintf("failed to reset seek: %v, skip converting", err))
		f.Close()
		return reader, cleanUpFn, errors.WithStack(err)
	}
	cleanUpFn = func() error {
		f.Close()
		os.Remove(f.Name())
		l.Debug(fmt.Sprintf("clean up tmp file: %s", f.Name()))
		return nil
	}
	return f, cleanUpFn, nil
}

func FromCSVToJSON(l *slog.Logger, reader io.ReadSeeker, skipHeader bool, skipRows int, delimiter ...rune) io.Reader {
	if skipRows > 0 {
		rowOffset := 0

		sc := bufio.NewScanner(reader)
		buf := make([]byte, 0, 4*1024)
		sc.Buffer(buf, 1024*1024)

		for skipRows > 0 && sc.Scan() {
			raw := sc.Bytes()
			rowOffset += len(raw)
			skipRows--
		}
		if err := sc.Err(); err != nil {
			l.Error(fmt.Sprintf("failed to read csv: %v, skip converting", err))
			return reader
		}
		if _, err := reader.Seek(int64(rowOffset), io.SeekStart); err != nil {
			l.Error(fmt.Sprintf("failed to reset seek: %v, skip converting", err))
			return reader
		}
	}

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
				recordResult.Set(header, record[i])
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

// ToCSV converts the records to CSV.
func ToCSV(l *slog.Logger, w io.Writer, r io.ReadSeeker, skipHeader bool, delimiter ...rune) error {
	reader := &RecordReader{r: r}
	// get the header
	headerMap := model.NewRecord()
	for record, err := range reader.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		if record.Len() == headerMap.Len() {
			continue
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
	_, err := r.Seek(0, io.SeekStart)
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
