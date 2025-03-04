package smtp

import (
	"context"
	"fmt"
	"html/template"
	"log/slog"
	"strings"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/pkg/errors"
)

type emailMetadataTemplate struct {
	from       *template.Template
	to         []*template.Template
	subject    *template.Template
	body       *template.Template
	attachment *template.Template
}

type SMTPSink struct {
	*common.Sink
	ctx    context.Context
	client *SMTPClient

	emailMetadataTemplate emailMetadataTemplate
}

// NewSink creates a new SMTPSink
func NewSink(ctx context.Context, l *slog.Logger, metadataPrefix string,
	address, username, password string,
	from, to, subject, body, attachment string,
	opts ...common.Option) (*SMTPSink, error) {

	// create common sink
	commonSink := common.NewSink(l, metadataPrefix, opts...)

	// create SMTP client
	client, err := NewSMTPClient(address, username, password)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse email metadata as template
	m := emailMetadataTemplate{}
	m.from = template.Must(extcommon.NewTemplate("sink_smtp_email_metadata_from", from))
	for i, t := range strings.Split(to, ",") {
		m.to = append(m.to, template.Must(extcommon.NewTemplate(fmt.Sprintf("sink_smtp_email_metadata_to_%d", i), t)))
	}
	m.subject = template.Must(extcommon.NewTemplate("sink_smtp_email_metadata_subject", subject))
	m.body = template.Must(extcommon.NewTemplate("sink_smtp_email_metadata_body", body))
	m.attachment = template.Must(extcommon.NewTemplate("sink_smtp_email_metadata_attachment", attachment))

	smtpSink := &SMTPSink{
		Sink:   commonSink,
		ctx:    ctx,
		client: client,
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		commonSink.Logger.Debug("sink(smtp): close record writer")
		_ = client.Close()
	})

	// register sink process
	commonSink.RegisterProcess(smtpSink.process)

	return smtpSink, nil
}

func (s *SMTPSink) process() {
	for msg := range s.Read() {

	}
}
