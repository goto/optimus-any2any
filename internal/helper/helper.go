// Description: This package contains common helper functions that are used by multiple packages.
package helper

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strconv"

	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

func FromJSONToCSV(l *slog.Logger, reader io.Reader, skipHeader bool, delimiter ...rune) io.ReadCloser {
	records := make([]model.Record, 0)
	sc := bufio.NewScanner(reader)
	hasError := false
	for sc.Scan() {
		raw := sc.Bytes()
		line := make([]byte, len(raw))
		copy(line, raw)

		var record model.Record
		if err := json.Unmarshal(line, &record); err != nil {
			if !hasError {
				l.Error(fmt.Sprintf("failed to unmarshal json: %v", err))
				hasError = true
			}
			continue
		}

		records = append(records, record)
	}

	r, w := io.Pipe()
	go func(w io.WriteCloser) {
		defer w.Close()
		if err := ToCSV(l, w, records, skipHeader, delimiter...); err != nil {
			l.Error(fmt.Sprintf("failed to convert json to csv: %v", err))
		}
	}(w)
	return r
}

func FromCSVToJSON(l *slog.Logger, reader io.Reader, skipHeader bool, delimiter ...rune) io.ReadCloser {
	csvReader := csv.NewReader(reader)
	if len(delimiter) > 0 {
		csvReader.Comma = delimiter[0]
	}

	r, w := io.Pipe()
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

// ToCSV converts the records to CSV.
func ToCSV(l *slog.Logger, w io.Writer, records []model.Record, skipHeader bool, delimiter ...rune) error {
	if len(records) == 0 {
		return nil
	}
	// get the header
	headerMap := model.NewRecord()
	for _, record := range records {
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

	// convert the records to string
	for _, record := range records {
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

func convertRecordToMapString(record model.Record) (model.Record, error) {
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
