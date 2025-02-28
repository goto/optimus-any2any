package extcommon

import (
	"html/template"
	"strings"
)

func NewTemplate(name, raw string) (*template.Template, error) {
	return template.New(name).Delims("[[", "]]").Parse(raw)
}

// CompileByRecordAsTemplate compiles a string based on the record as a template.
// The template uses the delimiters "[[" and "]]".
func CompileByRecordAsTemplate(record map[string]interface{}, raw string) (string, error) {
	t, err := template.New("record").Delims("[[", "]]").Parse(raw)
	if err != nil {
		return "", err
	}
	return Compile(t, struct {
		Record map[string]interface{}
	}{
		Record: record,
	})
}

func Compile(tmpl *template.Template, values interface{}) (string, error) {
	var builder strings.Builder
	err := tmpl.Execute(&builder, values)
	if err != nil {
		return "", err
	}
	return builder.String(), nil
}
