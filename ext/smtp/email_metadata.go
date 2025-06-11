package smtp

import (
	"fmt"
	"strings"
	"text/template"

	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/pkg/errors"
)

type emailMetadataTemplate struct {
	from           *template.Template
	to             []*template.Template
	cc             []*template.Template
	bcc            []*template.Template
	subject        *template.Template
	body           *template.Template
	attachmentBody *template.Template
	attachment     *template.Template
}

func newEmailMetadataTemplate(to, from, subject, body, attachment string) (*emailMetadataTemplate, error) {
	// parse email metadata as template
	m := emailMetadataTemplate{}

	// parse "to" to "to", "cc", "bcc"
	// to:email@domain.com,email2@domain.com;cc:sample@domain.com,sample2@domain.com;bcc:another@domain.com,another2@domain.com
	toParts := strings.Split(to, ";")
	partsMap := map[string][]string{}
	for _, part := range toParts {
		parts := strings.Split(part, ":")
		if len(parts) != 2 {
			return nil, errors.New(fmt.Sprintf("invalid to format: %s", part))
		}
		partsMap[parts[0]] = strings.Split(parts[1], ",")
	}
	to = strings.Join(partsMap["to"], ",")
	cc := strings.Join(partsMap["cc"], ",")
	bcc := strings.Join(partsMap["bcc"], ",")
	if to == "" {
		return nil, errors.New("to is required")
	}
	for i, t := range strings.Split(to, ",") {
		m.to = append(m.to, template.Must(compiler.NewTemplate(fmt.Sprintf("sink_smtp_email_metadata_to_%d", i), t)))
	}
	for i, c := range strings.Split(cc, ",") {
		m.cc = append(m.cc, template.Must(compiler.NewTemplate(fmt.Sprintf("sink_smtp_email_metadata_cc_%d", i), c)))
	}
	for i, b := range strings.Split(bcc, ",") {
		m.bcc = append(m.bcc, template.Must(compiler.NewTemplate(fmt.Sprintf("sink_smtp_email_metadata_bcc_%d", i), b)))
	}

	// parse from, subject, body, and attachment
	m.from = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_from", from))
	m.subject = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_subject", subject))
	m.body = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_body", body))
	m.attachment = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_attachment", attachment))

	return &m, nil
}
