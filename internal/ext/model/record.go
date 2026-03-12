package model

import (
	"strings"

	"github.com/GitRowin/orderedmapjson"
)

type Record = orderedmapjson.AnyOrderedMap

func NewRecord() *Record {
	record := orderedmapjson.NewAnyOrderedMap()
	record.SetEscapeHTML(false)
	return record
}

func NewRecordFromMap(m map[string]interface{}) *Record {
	record := NewRecord()
	for k, v := range m {
		record.Set(k, v)
	}
	return record
}

// ToMap converts a record to a map.
func ToMap(record *Record) map[string]interface{} {
	recordMap := make(map[string]interface{})
	for k, v := range record.AllFromFront() {
		recordMap[k] = v
	}
	return recordMap
}

// ToListMap converts a list of records to a list of maps.
func ToListMap(records []*Record) []map[string]interface{} {
	listMap := make([]map[string]interface{}, 0, len(records))
	for _, record := range records {
		listMap = append(listMap, ToMap(record))
	}
	return listMap
}

// HasAnyPrefix checks if any key in the record starts with the given prefix.
func HasAnyPrefix(record *Record, prefix string) bool {
	for key := range record.Keys() {
		if !strings.HasPrefix(key, prefix) {
			return false
		}
	}
	return true
}
