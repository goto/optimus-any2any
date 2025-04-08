package smtp

import (
	"context"
	"crypto/md5"
	errs "errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/goccy/go-json"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/helper"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
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
	from    string
	to      []string
	cc      []string
	bcc     []string
	subject string
	body    string
}

type emailHandler struct {
	emailMetadata emailMetadata
	fileHandlers  map[string]io.WriteCloser
}

type SMTPSink struct {
	*common.CommonSink
	ctx    context.Context
	client *SMTPClient

	emailMetadataTemplate emailMetadataTemplate
	emailHandlers         map[string]emailHandler
}

// NewSink creates a new SMTPSink
func NewSink(ctx context.Context, l *slog.Logger,
	connectionDSN string, from, to, subject, bodyFilePath, attachment string,
	opts ...common.Option) (*SMTPSink, error) {

	// create common sink
	commonSink := common.NewCommonSink(l, "smtp", opts...)

	// create SMTP client
	client, err := NewSMTPClient(connectionDSN)
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

	// parse email metadata as template
	m := emailMetadataTemplate{}
	m.from = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_from", from))
	for i, t := range strings.Split(to, ",") {
		m.to = append(m.to, template.Must(compiler.NewTemplate(fmt.Sprintf("sink_smtp_email_metadata_to_%d", i), t)))
	}
	for i, c := range strings.Split(cc, ",") {
		m.cc = append(m.cc, template.Must(compiler.NewTemplate(fmt.Sprintf("sink_smtp_email_metadata_cc_%d", i), c)))
	}
	for i, b := range strings.Split(bcc, ",") {
		m.bcc = append(m.bcc, template.Must(compiler.NewTemplate(fmt.Sprintf("sink_smtp_email_metadata_bcc_%d", i), b)))
	}
	m.subject = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_subject", subject))
	body, err := os.ReadFile(bodyFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	m.body = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_body", string(body)))
	m.attachment = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_attachment", attachment))

	s := &SMTPSink{
		CommonSink:            commonSink,
		ctx:                   ctx,
		client:                client,
		emailMetadataTemplate: m,
		emailHandlers:         make(map[string]emailHandler),
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s.Logger().Info(fmt.Sprintf("close smtp client"))
		return client.Close()
	})
	commonSink.AddCleanFunc(func() error {
		var e error

		for _, eh := range s.emailHandlers {
			for attachment, fh := range eh.fileHandlers {
				s.Logger().Info("close file handler")
				fh.Close()

				attachmentPath := getAttachmentPath(eh.emailMetadata, attachment)
				s.Logger().Info(fmt.Sprintf("remove tmp attachment file %s", attachmentPath))
				err := os.Remove(attachmentPath)
				e = errs.Join(e, err)
			}
		}

		return e
	})

	// register sink process
	commonSink.RegisterProcess(s.process)

	return s, nil
}

func (s *SMTPSink) process() error {
	for v := range s.Read() {
		s.Logger().Debug(fmt.Sprintf("received message"))

		var record model.Record
		if err := json.Unmarshal(v, &record); err != nil {
			s.Logger().Error(fmt.Sprintf("unmarshal error: %s", err.Error()))
			return errors.WithStack(err)
		}

		m, err := compileMetadata(s.emailMetadataTemplate, model.ToMap(&record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("compile metadata error: %s", err.Error()))
			return errors.WithStack(err)
		}

		hash := hashMetadata(m)
		eh, ok := s.emailHandlers[hash]
		if !ok {
			s.emailHandlers[hash] = emailHandler{
				emailMetadata: m,
				fileHandlers:  make(map[string]io.WriteCloser),
			}
			eh = s.emailHandlers[hash]
		}

		attachment, err := compiler.Compile(s.emailMetadataTemplate.attachment, model.ToMap(&record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("compile attachment error: %s", err.Error()))
			return errors.WithStack(err)
		}
		fh, ok := eh.fileHandlers[attachment]
		if !ok {
			attachmentPath := getAttachmentPath(m, attachment)
			fh, err = xio.NewWriteHandler(s.Logger(), attachmentPath)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("create file handler error: %s", err.Error()))
				return errors.WithStack(err)
			}
			eh.fileHandlers[attachment] = fh
		}

		recordWithoutMetadata := s.RecordWithoutMetadata(&record)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			s.Logger().Error(fmt.Sprintf("marshal error: %s", err.Error()))
			return errors.WithStack(err)
		}

		if _, err = fh.Write(append(raw, '\n')); err != nil {
			s.Logger().Error(fmt.Sprintf("write error: %s", err.Error()))
			return errors.WithStack(err)
		}
	}

	// send email
	for _, eh := range s.emailHandlers {
		attachmentReaders := map[string]io.ReadCloser{}
		defer func() {
			for _, r := range attachmentReaders {
				r.Close()
			}
		}()

		for attachment, fh := range eh.fileHandlers {
			// close file handler first
			if err := fh.Close(); err != nil {
				s.Logger().Error(fmt.Sprintf("close file handler error: %s", err.Error()))
				return errors.WithStack(err)
			}
			// open attachment file from tmp folder
			attachmentPath := getAttachmentPath(eh.emailMetadata, attachment)
			f, err := os.OpenFile(attachmentPath, os.O_RDONLY, 0644)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("open attachment file error: %s", err.Error()))
				return errors.WithStack(err)
			}

			// convert attachment file to desired format if necessary
			var tmpReader io.ReadCloser
			switch filepath.Ext(attachment) {
			case ".json":
				tmpReader = f
			case ".csv":
				tmpReader = helper.FromJSONToCSV(s.Logger(), f, false) // no skip header by default
			case ".tsv":
				tmpReader = helper.FromJSONToCSV(s.Logger(), f, false, rune('\t'))
			default:
				s.Logger().Warn(fmt.Sprintf("unsupported file format: %s, use default (json)", filepath.Ext(attachment)))
				tmpReader = f
			}

			attachmentReaders[attachment] = tmpReader
		}

		s.Logger().Info(fmt.Sprintf("send email to %s, cc %s, bcc %s", eh.emailMetadata.to, eh.emailMetadata.cc, eh.emailMetadata.bcc))
		if err := s.Retry(func() error {
			return s.client.SendMail(
				eh.emailMetadata.from,
				eh.emailMetadata.to,
				eh.emailMetadata.cc,
				eh.emailMetadata.bcc,
				eh.emailMetadata.subject,
				eh.emailMetadata.body,
				attachmentReaders,
			)
		}); err != nil {
			s.Logger().Error(fmt.Sprintf("send mail error: %s", err.Error()))
			return errors.WithStack(err)
		}
	}
	return nil
}

func compileMetadata(m emailMetadataTemplate, record map[string]interface{}) (emailMetadata, error) {
	em := emailMetadata{}

	from, err := compiler.Compile(m.from, record)
	if err != nil {
		return em, errors.WithStack(err)
	}
	em.from = from

	for _, t := range m.to {
		to, err := compiler.Compile(t, record)
		if err != nil {
			return em, errors.WithStack(err)
		}
		em.to = append(em.to, to)
	}

	for _, c := range m.cc {
		cc, err := compiler.Compile(c, record)
		if err != nil {
			return em, errors.WithStack(err)
		}
		em.cc = append(em.cc, cc)
	}

	for _, b := range m.bcc {
		bcc, err := compiler.Compile(b, record)
		if err != nil {
			return em, errors.WithStack(err)
		}
		em.bcc = append(em.bcc, bcc)
	}

	subject, err := compiler.Compile(m.subject, record)
	if err != nil {
		return em, errors.WithStack(err)
	}
	em.subject = subject

	body, err := compiler.Compile(m.body, record)
	if err != nil {
		return em, errors.WithStack(err)
	}
	em.body = body

	return em, nil
}

func hashMetadata(em emailMetadata) string {
	s := fmt.Sprintf("%s%s%s%s%s%s", em.from, strings.Join(em.to, ""), strings.Join(em.cc, ""), strings.Join(em.bcc, ""), em.subject, em.body)
	md5sum := md5.Sum([]byte(s))
	return fmt.Sprintf("%x", md5sum)
}

func getAttachmentPath(em emailMetadata, attachment string) string {
	return filepath.Join(tmpFolder, hashMetadata(em), attachment)
}
