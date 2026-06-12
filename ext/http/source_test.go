package http

import (
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
