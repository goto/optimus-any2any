package kafka

import (
	"encoding/base64"
	"testing"

	"github.com/goto/optimus-any2any/internal/ext/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractProtoField(t *testing.T) {
	tests := []struct {
		name      string
		record    *model.Record
		field     string
		wantBytes []byte
		wantErr   bool
	}{
		{
			name:      "valid base64 field",
			record:    model.NewRecordFromMap(map[string]interface{}{"__proto_value_b64__": base64.StdEncoding.EncodeToString([]byte("hello"))}),
			field:     "__proto_value_b64__",
			wantBytes: []byte("hello"),
			wantErr:   false,
		},
		{
			name:    "missing field",
			record:  model.NewRecordFromMap(map[string]interface{}{"other": "value"}),
			field:   "__proto_value_b64__",
			wantErr: true,
		},
		{
			name:    "non-string field",
			record:  model.NewRecordFromMap(map[string]interface{}{"__proto_value_b64__": 12345}),
			field:   "__proto_value_b64__",
			wantErr: true,
		},
		{
			name:    "invalid base64",
			record:  model.NewRecordFromMap(map[string]interface{}{"__proto_value_b64__": "not-valid-base64!!!"}),
			field:   "__proto_value_b64__",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := tt.record.Get(tt.field)
			if !ok {
				require.True(t, tt.wantErr, "field missing but expected no error")
				return
			}

			valStr, isString := val.(string)
			if !isString {
				require.True(t, tt.wantErr, "field not a string but expected no error")
				return
			}

			got, err := base64.StdEncoding.DecodeString(valStr)
			if tt.wantErr {
				require.Error(t, err, "expected base64 decode to fail")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantBytes, got)
		})
	}
}
