// Code generated by mockery v2.53.3. DO NOT EDIT.

package mocks

import (
	iter "iter"

	orderedmapjson "github.com/GitRowin/orderedmapjson"
	mock "github.com/stretchr/testify/mock"
)

// RecordReader is an autogenerated mock type for the RecordReader type
type RecordReader struct {
	mock.Mock
}

// ReadRecord provides a mock function with no fields
func (_m *RecordReader) ReadRecord() iter.Seq2[*orderedmapjson.AnyOrderedMap, error] {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for ReadRecord")
	}

	var r0 iter.Seq2[*orderedmapjson.AnyOrderedMap, error]
	if rf, ok := ret.Get(0).(func() iter.Seq2[*orderedmapjson.AnyOrderedMap, error]); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(iter.Seq2[*orderedmapjson.AnyOrderedMap, error])
		}
	}

	return r0
}

// NewRecordReader creates a new instance of RecordReader. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewRecordReader(t interface {
	mock.TestingT
	Cleanup(func())
}) *RecordReader {
	mock := &RecordReader{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
