package fileconverter

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strconv"

	"github.com/goto/optimus-any2any/internal/helper"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

func JSON2CSV(l *slog.Logger, src io.ReadSeeker, skipHeader bool, delimiter rune) (*os.File, error) {
	// Create a temporary file to write the CSV data
	dst, err := os.CreateTemp(os.TempDir(), "csv-*")
	if err != nil {
		l.Error(fmt.Sprintf("failed to open file: %v", err))
		return nil, errors.WithStack(err)
	}
	l.Debug(fmt.Sprintf("converting json to csv to tmp file: %s", dst.Name()))

	// reset the src file first
	if _, err = src.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}
	reader := helper.NewRecordReader(src)
	// get the header
	headerMap := model.NewRecord()
	for record, err := range reader.ReadRecord() {
		if err != nil {
			return nil, errors.WithStack(err)
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
	csvWriter := csv.NewWriter(dst)
	csvWriter.Comma = delimiter

	if !skipHeader {
		if err := csvWriter.Write(header); err != nil {
			return nil, errors.WithStack(err)
		}
	}

	// reset the src again to ensure we read from the start
	if _, err = src.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}
	// convert the records to string
	for record, err := range reader.ReadRecord() {
		if err != nil {
			return nil, errors.WithStack(err)
		}
		mapString, err := convertRecordToMapString(record)
		if err != nil {
			return nil, errors.WithStack(err)
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
		if err := csvWriter.Write(recordString); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if err := csvWriter.Error(); err != nil {
		l.Error(fmt.Sprintf("failed to write csv: %v", err))
		return nil, errors.WithStack(err)
	}

	// flush the csv writer to ensure all data is written to the file
	csvWriter.Flush()

	// reset the file pointer to the beginning of the file
	if _, err = dst.Seek(0, io.SeekStart); err != nil {
		l.Error(fmt.Sprintf("failed to reset seek: %v", err))
		return nil, errors.WithStack(err)
	}

	return dst, nil
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
