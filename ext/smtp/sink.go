package smtp

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/pkg/errors"
)

const (
	tmpFolder = "/tmp"
)

type emailMetadataTemplate struct {
	from       *template.Template
	to         []*template.Template
	cc         []*template.Template
	bcc        []*template.Template
	subject    *template.Template
	body       *template.Template
	attachment *template.Template
}

type emailMetadata struct {
	from           string
	to             []string
	cc             []string
	bcc            []string
	subject        string
	body           string
	attachment     string
	attachmentPath string
}

type emailHandler struct {
	emailMetadata emailMetadata
	fileHandler   extcommon.FileHandler
}

type SMTPSink struct {
	*common.Sink
	ctx    context.Context
	client *SMTPClient

	emailMetadataTemplate emailMetadataTemplate
	emailHandlers         map[string]emailHandler
}

// NewSink creates a new SMTPSink
func NewSink(ctx context.Context, l *slog.Logger, metadataPrefix string,
	address, username, password string,
	from, to, subject, bodyFilePath, attachment string,
	opts ...common.Option) (*SMTPSink, error) {

	// create common sink
	commonSink := common.NewSink(l, metadataPrefix, opts...)

	// create SMTP client
	client, err := NewSMTPClient(address, username, password)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// parse "to" to "to", "cc", "bcc"
	// to:email@domain.com,email2@domain.com;cc:sample@domain.com,sample2@domain.com;bcc:another@domain.com,another2@domain.com
	toParts := strings.Split(to, ";")
	partsMap := map[string][]string{}
	for _, part := range toParts {
		parts := strings.Split(part, ":")
		if len(parts) != 2 {
			return nil, errors.New(fmt.Sprintf("sink(smtp): invalid to format: %s", part))
		}
		partsMap[parts[0]] = strings.Split(parts[1], ",")
	}
	to = strings.Join(partsMap["to"], ",")
	cc := strings.Join(partsMap["cc"], ",")
	bcc := strings.Join(partsMap["bcc"], ",")
	if to == "" {
		return nil, errors.New("sink(smtp): to is required")
	}

	// parse email metadata as template
	m := emailMetadataTemplate{}
	m.from = template.Must(extcommon.NewTemplate("sink_smtp_email_metadata_from", from))
	for i, t := range strings.Split(to, ",") {
		m.to = append(m.to, template.Must(extcommon.NewTemplate(fmt.Sprintf("sink_smtp_email_metadata_to_%d", i), t)))
	}
	for i, c := range strings.Split(cc, ",") {
		m.cc = append(m.cc, template.Must(extcommon.NewTemplate(fmt.Sprintf("sink_smtp_email_metadata_cc_%d", i), c)))
	}
	for i, b := range strings.Split(bcc, ",") {
		m.bcc = append(m.bcc, template.Must(extcommon.NewTemplate(fmt.Sprintf("sink_smtp_email_metadata_bcc_%d", i), b)))
	}
	m.subject = template.Must(extcommon.NewTemplate("sink_smtp_email_metadata_subject", subject))
	body, err := os.ReadFile(bodyFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	m.body = template.Must(extcommon.NewTemplate("sink_smtp_email_metadata_body", string(body)))
	m.attachment = template.Must(extcommon.NewTemplate("sink_smtp_email_metadata_attachment", attachment))

	smtpSink := &SMTPSink{
		Sink:                  commonSink,
		ctx:                   ctx,
		client:                client,
		emailMetadataTemplate: m,
		emailHandlers:         make(map[string]emailHandler),
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		commonSink.Logger.Debug("sink(smtp): close record writer")
		_ = client.Close()
		for _, eh := range smtpSink.emailHandlers {
			commonSink.Logger.Debug("sink(smtp): close file handler")
			_ = eh.fileHandler.Close()
			commonSink.Logger.Debug(fmt.Sprintf("sink(smtp): remove attachment file %s", eh.emailMetadata.attachmentPath))
			_ = os.Remove(eh.emailMetadata.attachmentPath)
		}
	})

	// register sink process
	commonSink.RegisterProcess(smtpSink.process)

	return smtpSink, nil
}

func (s *SMTPSink) process() {
	for msg := range s.Read() {
		s.Logger.Debug("sink(smtp): received message")

		var record map[string]interface{}
		if err := json.Unmarshal(msg.([]byte), &record); err != nil {
			s.Logger.Error(fmt.Sprintf("sink(smtp): unmarshal error: %s", err.Error()))
			s.SetError(errors.WithStack(err))
			continue
		}

		m, err := compileMetadata(s.emailMetadataTemplate, record)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("sink(smtp): compile metadata error: %s", err.Error()))
			s.SetError(errors.WithStack(err))
			continue
		}

		hash := hashMetadata(m)
		eh, ok := s.emailHandlers[hash]
		if !ok {
			m.attachmentPath = filepath.Join(tmpFolder, hash, m.attachment)
			fh, err := file.NewStdFileHandler(s.Logger, m.attachmentPath)
			if err != nil {
				s.Logger.Error(fmt.Sprintf("sink(smtp): create file handler error: %s", err.Error()))
				s.SetError(errors.WithStack(err))
				continue
			}
			s.emailHandlers[hash] = emailHandler{
				emailMetadata: m,
				fileHandler:   fh,
			}
			eh = s.emailHandlers[hash]
		}

		recordWithoutMetadata := extcommon.RecordWithoutMetadata(record, s.MetadataPrefix)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			s.Logger.Error(fmt.Sprintf("sink(smtp): marshal error: %s", err.Error()))
			s.SetError(errors.WithStack(err))
			continue
		}

		if _, err = eh.fileHandler.Write(append(raw, '\n')); err != nil {
			s.Logger.Error(fmt.Sprintf("sink(smtp): write error: %s", err.Error()))
			s.SetError(errors.WithStack(err))
			continue
		}
	}

	// send email
	for _, eh := range s.emailHandlers {
		s.Logger.Info(fmt.Sprintf("sink(smtp): flush records to %s", eh.emailMetadata.attachmentPath))
		if err := eh.fileHandler.Flush(); err != nil {
			s.Logger.Error(fmt.Sprintf("sink(smtp): flush error: %s", err.Error()))
			s.SetError(errors.WithStack(err))
			continue
		}
		s.Logger.Info(fmt.Sprintf("sink(smtp): send email to %s", eh.emailMetadata.to))
		if err := s.client.SendMail(eh.emailMetadata.from, eh.emailMetadata.to,
			eh.emailMetadata.subject, eh.emailMetadata.body,
			eh.emailMetadata.attachment, eh.emailMetadata.attachmentPath); err != nil {
			s.Logger.Error(fmt.Sprintf("sink(smtp): send mail error: %s", err.Error()))
			s.SetError(errors.WithStack(err))
			continue
		}
	}
}

func compileMetadata(m emailMetadataTemplate, record map[string]interface{}) (emailMetadata, error) {
	em := emailMetadata{}

	from, err := extcommon.Compile(m.from, record)
	if err != nil {
		return em, errors.WithStack(err)
	}
	em.from = from

	for _, t := range m.to {
		to, err := extcommon.Compile(t, record)
		if err != nil {
			return em, errors.WithStack(err)
		}
		em.to = append(em.to, to)
	}

	for _, c := range m.cc {
		cc, err := extcommon.Compile(c, record)
		if err != nil {
			return em, errors.WithStack(err)
		}
		em.cc = append(em.cc, cc)
	}

	for _, b := range m.bcc {
		bcc, err := extcommon.Compile(b, record)
		if err != nil {
			return em, errors.WithStack(err)
		}
		em.bcc = append(em.bcc, bcc)
	}

	subject, err := extcommon.Compile(m.subject, record)
	if err != nil {
		return em, errors.WithStack(err)
	}
	em.subject = subject

	body, err := extcommon.Compile(m.body, record)
	if err != nil {
		return em, errors.WithStack(err)
	}
	em.body = body

	attachment, err := extcommon.Compile(m.attachment, record)
	if err != nil {
		return em, errors.WithStack(err)
	}
	em.attachment = attachment

	return em, nil
}

func hashMetadata(em emailMetadata) string {
	s := fmt.Sprintf("%s%s%s%s%s%s%s", em.from, strings.Join(em.to, ""), strings.Join(em.cc, ""), strings.Join(em.bcc, ""), em.subject, em.body, em.attachment)
	md5sum := md5.Sum([]byte(s))
	return fmt.Sprintf("%x", md5sum)
}
