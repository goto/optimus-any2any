package extcommon_test

import (
	"testing"
	"time"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/stretchr/testify/assert"
)

func TestCompile(t *testing.T) {
	t.Run("compile value based on the record as a template", func(t *testing.T) {

		tmplString := "column1: [[ .column1 ]], column2: [[ .column2 ]]"
		expected := "column1: value1, column2: value2"
		record := map[string]interface{}{
			"column1": "value1",
			"column2": "value2",
		}

		tmpl, err := extcommon.NewTemplate("test", tmplString)
		assert.NoError(t, err)
		actual, err := extcommon.Compile(tmpl, record)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
	t.Run("compile value based on the nested record as a template", func(t *testing.T) {

		tmplString := "column1: [[ .column1 ]], column2: [[ .nested.column2 ]]"
		expected := "column1: value1, column2: value2"
		record := map[string]interface{}{
			"column1": "value1",
			"nested": map[string]interface{}{
				"column2": "value2",
			},
		}

		tmpl, err := extcommon.NewTemplate("test", tmplString)
		assert.NoError(t, err)
		actual, err := extcommon.Compile(tmpl, record)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
	t.Run("compile value based on the record as a template and omit non record template", func(t *testing.T) {

		tmplString := "column1: [[ .column1 ]], column2: [[ .column2 ]], no render {{ .DSTART }}"
		expected := "column1: value1, column2: value2, no render {{ .DSTART }}"
		record := map[string]interface{}{
			"column1": "value1",
			"column2": "value2",
		}

		tmpl, err := extcommon.NewTemplate("test", tmplString)
		assert.NoError(t, err)
		actual, err := extcommon.Compile(tmpl, record)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
	t.Run("compile value with function now", func(t *testing.T) {

		tmplString := `column1: [[ .column1 ]]_[[ now.Format "2006-01-02" ]], column2: [[ .column2 ]], no render {{ .DSTART }}`
		expected := "column1: value1_" + time.Now().Format("2006-01-02") + ", column2: value2, no render {{ .DSTART }}"
		record := map[string]interface{}{
			"column1": "value1",
			"column2": "value2",
		}

		tmpl, err := extcommon.NewTemplate("test", tmplString)
		assert.NoError(t, err)
		actual, err := extcommon.Compile(tmpl, record)
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
}
