// Description: This package contains common helper functions that are used by multiple packages.
package extcommon

import (
	"encoding/json"
	"fmt"
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
