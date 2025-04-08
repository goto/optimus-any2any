// Code generated by mockery v2.53.3. DO NOT EDIT.

package mocks

import (
	iter "iter"

	mock "github.com/stretchr/testify/mock"

	orderedmapjson "github.com/GitRowin/orderedmapjson"
)

// RecordReaderCloser is an autogenerated mock type for the RecordReaderCloser type
type RecordReaderCloser struct {
	mock.Mock
}

// Close provides a mock function with no fields
func (_m *RecordReaderCloser) Close() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Close")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReadRecord provides a mock function with no fields
func (_m *RecordReaderCloser) ReadRecord() iter.Seq2[*orderedmapjson.AnyOrderedMap, error] {
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

// NewRecordReaderCloser creates a new instance of RecordReaderCloser. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewRecordReaderCloser(t interface {
	mock.TestingT
	Cleanup(func())
}) *RecordReaderCloser {
	mock := &RecordReaderCloser{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
