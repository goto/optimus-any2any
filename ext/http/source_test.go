package http

import (
	"net/http"
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

func TestExtractResponseHeaders(t *testing.T) {
	tests := []struct {
		name   string
		header http.Header
		names  []string
		want   map[string][]string
	}{
		{
			name:   "no allowlist configured returns nil",
			header: http.Header{"X-Test-Header": []string{"foo"}},
			names:  nil,
			want:   nil,
		},
		{
			name:   "no matching headers returns nil",
			header: http.Header{"X-Other-Header": []string{"foo"}},
			names:  []string{"X-Test-Header"},
			want:   nil,
		},
		{
			name: "mix of present, absent, and differently-cased names",
			header: http.Header{
				"X-Test-Header": []string{"foo"},
				"Content-Type":  []string{"application/json"},
				"X-Rate-Limit":  []string{"100"},
			},
			names: []string{"x-test-header", "X-Missing-Header", "content-type"},
			want: map[string][]string{
				"X-Test-Header": {"foo"},
				"Content-Type":  {"application/json"},
			},
		},
		{
			name: "multi-value header preserves all values",
			header: http.Header{
				"X-Multi": []string{"a", "b", "c"},
			},
			names: []string{"X-Multi"},
			want: map[string][]string{
				"X-Multi": {"a", "b", "c"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractResponseHeaders(tt.header, tt.names)
			assert.Equal(t, tt.want, got)
		})
	}
}
