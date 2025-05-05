package net_test

import (
	"testing"

	xnet "github.com/goto/optimus-any2any/internal/net"
	"github.com/stretchr/testify/assert"
)

func TestConnCheck(t *testing.T) {
	t.Run("when address use url format", func(t *testing.T) {
		err := xnet.ConnCheck("http://localhost/hello")
		assert.Error(t, err)
		assert.ErrorContains(t, err, "dial tcp [::1]:80: connect: connection refused")
	})
	t.Run("when address use host:port format", func(t *testing.T) {
		err := xnet.ConnCheck("localhost:8080")
		assert.Error(t, err)
		assert.ErrorContains(t, err, "dial tcp [::1]:8080: connect: connection refused")
	})
	t.Run("when address use host format", func(t *testing.T) {
		err := xnet.ConnCheck("http://localhost:8081")
		assert.Error(t, err)
		assert.ErrorContains(t, err, "dial tcp [::1]:8081: connect: connection refused")
	})
}
