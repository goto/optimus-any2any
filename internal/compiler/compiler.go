package compiler

import (
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
)

func NewTemplate(name, raw string) (*template.Template, error) {
	return template.New(name).
		Delims("[[", "]]").
		Funcs(sprig.TxtFuncMap()).
		Parse(raw)
}

func Compile(tmpl *template.Template, values interface{}) (string, error) {
	var builder strings.Builder
	err := tmpl.Execute(&builder, values)
	if err != nil {
		return "", err
	}
	return builder.String(), nil
}
