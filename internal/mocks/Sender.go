// Code generated by mockery v2.53.3. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// Sender is an autogenerated mock type for the Sender type
type Sender struct {
	mock.Mock
}

// Send provides a mock function with given fields: v
func (_m *Sender) Send(v []byte) {
	_m.Called(v)
}

// NewSender creates a new instance of Sender. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewSender(t interface {
	mock.TestingT
	Cleanup(func())
}) *Sender {
	mock := &Sender{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
