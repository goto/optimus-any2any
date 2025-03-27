// Description: This package contains common helper functions that are used by multiple packages.
package extcommon

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var builtinValueFuns = map[string]func() string{
	"uuid": func() string {
		return uuid.New().String()
	},
	"timestamp_utc": func() string {
		return fmt.Sprintf("%d", time.Now().Unix())
	},
}

// RecordWithoutMetadata returns the record without given metadata prefix.
func RecordWithoutMetadata(record map[string]interface{}, metadataPrefix string) map[string]interface{} {
	recordWithoutMetadata := make(map[string]interface{})
	for k, v := range record {
		if strings.HasPrefix(k, metadataPrefix) {
			continue
		}
		recordWithoutMetadata[k] = v
	}
	return recordWithoutMetadata
}

func FromJSONToRecords(l *slog.Logger, reader io.Reader) ([]map[string]interface{}, error) {
	records := make([]map[string]interface{}, 0)
	sc := bufio.NewScanner(reader)
	for sc.Scan() {
		raw := sc.Bytes()
		line := make([]byte, len(raw))
		copy(line, raw)

		var record map[string]interface{}
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, errors.WithStack(err)
		}
		records = append(records, record)
	}
	return records, nil
}

func FromCSVToRecords(l *slog.Logger, reader io.Reader) ([]map[string]interface{}, error) {
	records := make([]map[string]interface{}, 0)
	r := csv.NewReader(reader)
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// csv to json records
	for i, row := range rows {
		if i == 0 {
			continue
		}
		record := make(map[string]interface{})
		if len(row) != len(rows[0]) {
			l.Warn(fmt.Sprintf("record %d has different column count", i))
			l.Debug(fmt.Sprintf("record %d: %v", i, row))
			continue
		}
		for j, value := range row {
			record[rows[0][j]] = value
		}
		records = append(records, record)
	}
	return records, nil
}

func FromJSONToCSV(l *slog.Logger, reader io.Reader, skipHeader bool, delimiter ...rune) io.ReadCloser {
	records := make([]map[string]interface{}, 0)
	sc := bufio.NewScanner(reader)
	hasError := false
	for sc.Scan() {
		raw := sc.Bytes()
		line := make([]byte, len(raw))
		copy(line, raw)

		var record map[string]interface{}
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
	go func() {
		defer w.Close()
		if err := ToCSV(l, w, records, skipHeader, delimiter...); err != nil {
			l.Error(fmt.Sprintf("failed to convert json to csv: %v", err))
		}
	}()
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
			recordResult := map[string]interface{}{}
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
				recordResult[header] = val
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
func ToCSV(l *slog.Logger, w io.Writer, records []map[string]interface{}, skipHeader bool, delimiter ...rune) error {
	if len(records) == 0 {
		return nil
	}
	// get the header
	headerMap := make(map[string]bool)
	for _, record := range records {
		for k := range record {
			headerMap[k] = true
		}
	}
	header := make([]string, 0, len(headerMap))
	for k := range headerMap {
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
			if v, ok := mapString[k]; ok {
				recordString = append(recordString, v)
			} else {
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

func convertRecordToMapString(record map[string]interface{}) (map[string]string, error) {
	recordString := make(map[string]string)
	for k, v := range record {
		val, err := convertValueToString(v)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		recordString[k] = val
	}
	return recordString, nil
}

func convertValueToString(v interface{}) (string, error) {
	switch val := v.(type) {
	case bool:
		return fmt.Sprintf("%t", val), nil
	case int, int64:
		return fmt.Sprintf("%d", val), nil
	case float32, float64:
		return fmt.Sprintf("%f", val), nil
	case string:
		return val, nil
	case map[string]interface{}, []interface{}:
		b := []byte{}
		if err := json.Unmarshal(b, val); err != nil {
			return "", errors.WithStack(err)
		}
		return string(b), nil
	case nil:
		return "", nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}
