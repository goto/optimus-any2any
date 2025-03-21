// Description: This package contains common helper functions that are used by multiple packages.
package extcommon

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
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

// RenderFilename renders the filename pattern with the values.
func RenderFilename(filenamePattern string, values map[string]string) string {
	// Replace the filename pattern with the values
	mergedValues := make(map[string]string)
	for k, v := range builtinValueFuns {
		mergedValues[k] = v()
	}
	for k, v := range values {
		mergedValues[k] = v
	}
	for k, v := range mergedValues {
		filenamePattern = strings.ReplaceAll(filenamePattern, fmt.Sprintf("{%s}", k), v)
	}
	return filenamePattern
}

// GetColumnMap reads the column map from the file.
func GetColumnMap(columnMapFilePath string) (map[string]string, error) {
	if columnMapFilePath == "" {
		return nil, nil
	}
	columnMapRaw, err := os.ReadFile(columnMapFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	columnMap := make(map[string]string)
	if err = json.Unmarshal(columnMapRaw, &columnMap); err != nil {
		return nil, errors.WithStack(err)
	}
	return columnMap, nil
}

// KeyMapping maps the keys of the value according to the keyMap.
func KeyMapping(keyMap map[string]string, value map[string]interface{}) map[string]interface{} {
	if keyMap == nil {
		return value
	}
	mappedValue := make(map[string]interface{})
	for key, val := range value {
		if mappedKey, ok := keyMap[key]; ok {
			mappedValue[mappedKey] = val
		} else {
			mappedValue[key] = val
		}
	}
	return mappedValue
}

// GroupRecordsByKey groups the records by the key.
func GroupRecordsByKey(groupedKey string, records [][]byte) (map[string][][]byte, error) {
	groupRecords := map[string][][]byte{}
	for _, raw := range records {
		// parse the record
		var v map[string]interface{}
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, errors.WithStack(err)
		}
		groupKeyRaw, ok := v[groupedKey]
		if !ok {
			return nil, errors.New(fmt.Sprintf("group by column not found: %s", groupedKey))
		}
		groupKey := fmt.Sprintf("%v", groupKeyRaw)
		groupRecords[groupKey] = append(groupRecords[groupKey], raw)
	}
	return groupRecords, nil
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

func FromJSONToCSV(l *slog.Logger, reader io.Reader, delimiter ...rune) io.Reader {
	records := make([]map[string]interface{}, 0)
	sc := bufio.NewScanner(reader)
	for sc.Scan() {
		raw := sc.Bytes()
		line := make([]byte, len(raw))
		copy(line, raw)

		var record map[string]interface{}
		if err := json.Unmarshal(line, &record); err != nil {
			l.Error(fmt.Sprintf("failed to unmarshal json: %v", err))
			continue
		}

		records = append(records, record)
	}

	r, w := io.Pipe()
	go func() {
		defer w.Close()
		if err := ToCSV(l, w, records, delimiter...); err != nil {
			l.Error(fmt.Sprintf("failed to convert json to csv: %v", err))
		}
	}()
	return r
}

// ToCSV converts the records to CSV.
func ToCSV(l *slog.Logger, w io.Writer, records []map[string]interface{}, delimiter ...rune) error {
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
	if err := csvWriter.Write(header); err != nil {
		return errors.WithStack(err)
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
	default:
		return fmt.Sprintf("%v", val), nil
	}
}
