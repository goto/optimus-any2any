package http

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetByPath(t *testing.T) {
	tests := []struct {
		name    string
		data    interface{}
		path    string
		want    interface{}
		wantErr bool
	}{
		{
			name:    "empty path returns whole data",
			data:    map[string]interface{}{"a": 1},
			path:    "",
			want:    map[string]interface{}{"a": 1},
			wantErr: false,
		},
		{
			name:    "dot path returns whole data",
			data:    map[string]interface{}{"a": 1},
			path:    ".",
			want:    map[string]interface{}{"a": 1},
			wantErr: false,
		},
		{
			name:    "simple key",
			data:    map[string]interface{}{"cursor": "abc123"},
			path:    "cursor",
			want:    "abc123",
			wantErr: false,
		},
		{
			name: "nested path",
			data: map[string]interface{}{
				"data": map[string]interface{}{
					"items": []interface{}{1, 2, 3},
				},
			},
			path:    "data.items",
			want:    []interface{}{1, 2, 3},
			wantErr: false,
		},
		{
			name: "deep nested path",
			data: map[string]interface{}{
				"response": map[string]interface{}{
					"pagination": map[string]interface{}{
						"next_cursor": "tok_456",
					},
				},
			},
			path:    "response.pagination.next_cursor",
			want:    "tok_456",
			wantErr: false,
		},
		{
			name:    "missing key returns error",
			data:    map[string]interface{}{"a": 1},
			path:    "missing",
			want:    nil,
			wantErr: true,
		},
		{
			name: "non-object at path segment returns error",
			data: map[string]interface{}{
				"data": "not-an-object",
			},
			path:    "data.items",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "path with empty segments is ignored",
			data:    map[string]interface{}{"a": map[string]interface{}{"b": 42}},
			path:    "a..b",
			want:    42,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getByPath(tt.data, tt.path)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSensitiveHeader(t *testing.T) {
	sensitive := []string{"Authorization", "authorization", "Cookie", "X-Api-Key", "X-Auth-Token", "Client-Secret"}
	for _, h := range sensitive {
		assert.Truef(t, sensitiveHeader(h), "expected %q to be treated as sensitive", h)
	}

	notSensitive := []string{"Content-Type", "Accept", "User-Agent", "Content-Length"}
	for _, h := range notSensitive {
		assert.Falsef(t, sensitiveHeader(h), "expected %q to not be treated as sensitive", h)
	}
}

func TestMaxLoggedBodyBytesTruncation(t *testing.T) {
	// body under the limit is not truncated
	small := strings.Repeat("a", 100)
	assert.LessOrEqual(t, len(small), maxLoggedBodyBytes)

	// body over the limit is truncated to exactly the limit
	large := strings.Repeat("b", maxLoggedBodyBytes+512)
	loggedBody := large
	truncated := false
	if len(loggedBody) > maxLoggedBodyBytes {
		loggedBody = loggedBody[:maxLoggedBodyBytes]
		truncated = true
	}
	assert.True(t, truncated)
	assert.Len(t, loggedBody, maxLoggedBodyBytes)
}
