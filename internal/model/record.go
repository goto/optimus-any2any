package model

import (
	"github.com/GitRowin/orderedmapjson"
)

type Record = orderedmapjson.AnyOrderedMap

func NewRecord() *Record {
	return orderedmapjson.NewAnyOrderedMap()
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
