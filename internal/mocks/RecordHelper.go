// Code generated by mockery v2.53.3. DO NOT EDIT.

package mocks

import (
	orderedmapjson "github.com/GitRowin/orderedmapjson"
	mock "github.com/stretchr/testify/mock"
)

// RecordHelper is an autogenerated mock type for the RecordHelper type
type RecordHelper struct {
	mock.Mock
}

// RecordWithMetadata provides a mock function with given fields: record
func (_m *RecordHelper) RecordWithMetadata(record *orderedmapjson.AnyOrderedMap) *orderedmapjson.AnyOrderedMap {
	ret := _m.Called(record)

	if len(ret) == 0 {
		panic("no return value specified for RecordWithMetadata")
	}

	var r0 *orderedmapjson.AnyOrderedMap
	if rf, ok := ret.Get(0).(func(*orderedmapjson.AnyOrderedMap) *orderedmapjson.AnyOrderedMap); ok {
		r0 = rf(record)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*orderedmapjson.AnyOrderedMap)
		}
	}

	return r0
}

// RecordWithoutMetadata provides a mock function with given fields: record
func (_m *RecordHelper) RecordWithoutMetadata(record *orderedmapjson.AnyOrderedMap) *orderedmapjson.AnyOrderedMap {
	ret := _m.Called(record)

	if len(ret) == 0 {
		panic("no return value specified for RecordWithoutMetadata")
	}

	var r0 *orderedmapjson.AnyOrderedMap
	if rf, ok := ret.Get(0).(func(*orderedmapjson.AnyOrderedMap) *orderedmapjson.AnyOrderedMap); ok {
		r0 = rf(record)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*orderedmapjson.AnyOrderedMap)
		}
	}

	return r0
}

// NewRecordHelper creates a new instance of RecordHelper. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewRecordHelper(t interface {
	mock.TestingT
	Cleanup(func())
}) *RecordHelper {
	mock := &RecordHelper{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
