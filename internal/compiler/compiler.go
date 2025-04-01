package compiler

import (
	"strings"
	"text/template"
	"time"

	"github.com/goccy/go-json"

	"github.com/google/uuid"
)

func NewTemplate(name, raw string) (*template.Template, error) {
	return template.New(name).
		Delims("[[", "]]").
		Funcs(template.FuncMap{
			"uuid": func() string { return uuid.New().String() },
			"now":  func() time.Time { return time.Now().UTC() },
			"tojson": func(v interface{}) string {
				b, _ := json.Marshal(v) // TODO: handle error
				return string(b)
			},
		}).
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
