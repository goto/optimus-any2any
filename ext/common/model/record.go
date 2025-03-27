package model

import "github.com/GitRowin/orderedmapjson"

type Record = orderedmapjson.AnyOrderedMap

type KeyValue struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

func NewRecord() Record {
	return *orderedmapjson.NewAnyOrderedMap()
}

// ToMap converts a record to a map.
func ToMap(record Record) map[string]interface{} {
	recordMap := make(map[string]interface{})
	for k, v := range record.AllFromFront() {
		recordMap[k] = v
	}
	return recordMap
}

// ToOrderedSlice converts an ordered map to a slice of key-value pairs.
func ToOrderedSlice(m orderedmapjson.AnyOrderedMap) []map[string]interface{} {
	var result []map[string]interface{}
	for k, v := range m.AllFromFront() {
		result = append(result, map[string]interface{}{
			"key":   k,
			"value": v,
		})
	}
	return result
}
