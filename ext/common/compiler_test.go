package extcommon_test

import (
	"testing"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/stretchr/testify/assert"
)

func TestCompileByRecordAsTemplate(t *testing.T) {
	t.Run("compile value based on the record as a template", func(t *testing.T) {

		tmplString := "column1: [[ .Record.column1 ]], column2: [[ .Record.column2 ]]"
		expected := "column1: value1, column2: value2"
		record := map[string]interface{}{
			"column1": "value1",
			"column2": "value2",
		}

		actual, err := extcommon.CompileByRecordAsTemplate(record, tmplString)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
	t.Run("compile value based on the nested record as a template", func(t *testing.T) {

		tmplString := "column1: [[ .Record.column1 ]], column2: [[ .Record.nested.column2 ]]"
		expected := "column1: value1, column2: value2"
		record := map[string]interface{}{
			"column1": "value1",
			"nested": map[string]interface{}{
				"column2": "value2",
			},
		}

		actual, err := extcommon.CompileByRecordAsTemplate(record, tmplString)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
	t.Run("compile value based on the record as a template and omit non record template", func(t *testing.T) {

		tmplString := "column1: [[ .Record.column1 ]], column2: [[ .Record.column2 ]], no render {{ .DSTART }}"
		expected := "column1: value1, column2: value2, no render {{.DSTART}}"
		record := map[string]interface{}{
			"column1": "value1",
			"column2": "value2",
		}

		actual, err := extcommon.CompileByRecordAsTemplate(record, tmplString)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
}
