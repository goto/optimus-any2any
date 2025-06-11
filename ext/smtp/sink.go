package smtp

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/goto/optimus-any2any/ext/file"
	osssink "github.com/goto/optimus-any2any/ext/oss"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/config"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

const (
	tmpFolder                 = "/tmp"
	attachmentBodyPlaceholder = "<!-- ATTACHMENTS_PLACEHOLDER -->"
)

var (
	attachmentBodyPattern = regexp.MustCompile(`(?s)\[\[\s*range\s*\.Attachments\s*\]\](.*?)\[\[\s*end\s*\]\]`)
)

type SMTPStorage interface {
	Process() error
	Close() error
}

// Sample interface for a component processor
type ComponentProcessor interface {
	Process() error
}

// type emailMetadataTemplate struct {
// 	from           *template.Template
// 	to             []*template.Template
// 	cc             []*template.Template
// 	bcc            []*template.Template
// 	subject        *template.Template
// 	body           *template.Template
// 	attachmentBody *template.Template
// 	attachment     *template.Template
// }

type attachmentBody struct {
	Filename string
	URI      string
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
	writeHandlers map[string]xio.WriteFlushCloser
}

type emailWithAttachment struct {
	emailMetadata emailMetadata
	// map filename to url path
	attachments []string
}

type SMTPSink struct {
	common.Sink
	client *SMTPClient

	emailMetadataTemplate emailMetadataTemplate
	fileSink              *file.FileSink
	ossSink               *osssink.OSSSink

	emailHandlers map[string]emailHandler

	// TODO move this to shared package
	ossclient            *oss.Client
	writeHandlers        map[string]xio.WriteFlushCloser
	fileRecordCounters   map[string]int
	enableOverwrite      bool
	skipHeader           bool
	ossDestinationDir    string
	ossLinkExpiration    time.Duration
	emailWithAttachments map[string]emailWithAttachment
	storageConfig        StorageConfig

	enableArchive       bool
	compressionType     string
	compressionPassword string
}

// NewSink creates a new SMTPSink
func NewSink(commonSink common.Sink,
	connectionDSN string, from, to, subject, bodyFilePath, attachment string, ossConfig config.SinkOSSConfig,
	storageConfig StorageConfig,
	compressionType string, compressionPassword string, storageProcessor ComponentProcessor,
	opts ...common.Option) (*SMTPSink, error) {

	// create SMTP client
	client, err := NewSMTPClient(commonSink.Context(), connectionDSN)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// create email metadata template
	body, err := os.ReadFile(bodyFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	m, err := newEmailMetadataTemplate(to, from, subject, string(body), attachment)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if 2 == 2 { // oss storage
		m, err = newEmailMetadataTemplate(to, from, subject, string(body), ossConfig.DestinationURI)
	}

	s := &SMTPSink{
		Sink:                  commonSink,
		client:                client,
		emailMetadataTemplate: *m,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s.Logger().Info(fmt.Sprintf("close smtp client"))
		return client.Close()
	})

	commonSink.RegisterProcess(s.process)
	if 1 == 1 { // file storage
		commonSink.RegisterProcess(s.fileSink.Process)
	} else { // oss storage
		commonSink.RegisterProcess(s.ossSink.Process)
	}

	return s, nil
}

func separateBodyAndAttachmentBodyTemplate(bodyTemplateStr string) (*template.Template, *template.Template, error) {
	var attachmentContent string

	matches := attachmentBodyPattern.FindStringSubmatch(bodyTemplateStr)
	if len(matches) > 1 {
		attachmentContent = matches[1]
	}

	// replace the attachment template with a placeholder
	processedTemplate := attachmentBodyPattern.ReplaceAllString(bodyTemplateStr, attachmentBodyPlaceholder)

	attachmentContent = fmt.Sprintf("[[ range .Attachments ]]%s[[ end ]]", attachmentContent)

	bodyTmpl, err := compiler.NewTemplate("sink_smtp_email_metadata_body", processedTemplate)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	attachmentTmpl, err := compiler.NewTemplate("sink_smtp_email_metadata_attachment_body", attachmentContent)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	return bodyTmpl, attachmentTmpl, nil
}

func compileAttachmentBodyTemplate(body string, attachmentTmpl *template.Template, attachments []attachmentBody) (string, error) {
	attachmentBody, err := compiler.Compile(attachmentTmpl, map[string]interface{}{
		"Attachments": attachments,
	})
	if err != nil {
		return "", errors.WithStack(err)
	}

	// replace the placeholder with the actual attachment body
	bodyWithAttachment := strings.ReplaceAll(body, attachmentBodyPlaceholder, attachmentBody)
	return bodyWithAttachment, nil
}

func (s *SMTPSink) process() error {
	for record, err := range s.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}

		m, err := compileMetadata(s.emailMetadataTemplate, model.ToMap(record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("compile metadata error: %s", err.Error()))
			return errors.WithStack(err)
		}

		hash := hashMetadata(m)
		eh, ok := s.emailWithAttachments[hash]
		if !ok {
			s.emailWithAttachments[hash] = emailWithAttachment{
				emailMetadata: m,
				attachments:   []string{},
			}
			eh = s.emailWithAttachments[hash]
		}

		attachment, err := compiler.Compile(s.emailMetadataTemplate.attachment, model.ToMap(record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("compile attachment error: %s", err.Error()))
			return errors.WithStack(err)
		}

		eh.attachments = append(eh.attachments, attachment)
	}

	// send email
	for _, eh := range s.emailWithAttachments {
		linkAttachments := []string{}
		fileAttachments := []string{}
		for _, attachment := range eh.attachments {
			if strings.HasPrefix(attachment, "oss://") {
				// oss attachment
				// remove oss:// prefix
				attachment = strings.TrimPrefix(attachment, "oss://")
				linkAttachments = append(linkAttachments, attachment)
			} else {
				// local file attachment
				fileAttachments = append(fileAttachments, attachment)
			}
		}

		// TODO: attach oss links to body
		if len(linkAttachments) > 0 {
			// compile body with attachment links
		}

		attachmentReaders := make(map[string]io.Reader, len(fileAttachments))
		for _, attachment := range fileAttachments {
			f, err := os.Open(attachment)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to open attachment file %s: %s", attachment, err.Error()))
				return errors.WithStack(err)
			}
			attachmentReaders[attachment] = f
		}

		s.Logger().Info(fmt.Sprintf("send email to %s, cc %s, bcc %s", eh.emailMetadata.to, eh.emailMetadata.cc, eh.emailMetadata.bcc))
		if err := s.Retry(func() error {
			return s.DryRunable(func() error {
				return s.client.SendMail(
					eh.emailMetadata.from,
					eh.emailMetadata.to,
					eh.emailMetadata.cc,
					eh.emailMetadata.bcc,
					eh.emailMetadata.subject,
					eh.emailMetadata.body,
					attachmentReaders,
				)
			}, func() error {
				// in dry run mode, we don't need to send the request
				// we just need to check the endpoint connectivity
				err := s.client.DialWithContext(s.Context())
				if err != nil {
					return errors.WithStack(err)
				}
				defer s.client.Close()
				return nil
			})
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
