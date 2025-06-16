package compiler

import (
	"encoding/json"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/google/uuid"
)

func NewTemplate(name, raw string) (*template.Template, error) {
	return template.New(name).
		Delims("[[", "]]").
		Funcs(funcsMap()).
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

func funcsMap() template.FuncMap {
	funcsMap := sprig.TxtFuncMap()
	funcsMap["uuid"] = func() string { return uuid.New().String() }
	funcsMap["now"] = func() time.Time { return time.Now().UTC() }
	funcsMap["tojson"] = func(v interface{}) string { // TODO: deprecate this since it's supported by sprig
		b, _ := json.Marshal(v) // TODO: handle error
		return string(b)
	}
	return funcsMap
}
