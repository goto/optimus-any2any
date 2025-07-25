package smtp

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"

	"github.com/goccy/go-json"
	"github.com/goto/optimus-any2any/ext/file"
	osssink "github.com/goto/optimus-any2any/ext/oss"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/fs"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

const (
	attachmentBodyPlaceholder = "<!-- ATTACHMENTS_PLACEHOLDER -->"
)

var (
	attachmentBodyPattern = regexp.MustCompile(`(?s)\[\[\s*range\s*\.Attachments\s*\]\](.*?)\[\[\s*end\s*\]\]`)
)

type SMTPStorage interface {
	Process() error
	Close() error
}

type emailMetadataTemplate struct {
	from           *template.Template
	to             *template.Template
	cc             *template.Template
	bcc            *template.Template
	subject        *template.Template
	body           *template.Template
	bodyNoRecord   *template.Template // TODO: refactor thins, this should not be here, it always contains static content
	attachmentBody *template.Template
	attachment     *template.Template
}

type attachmentBody struct {
	Filename string
	URI      string
}

type emailMetadata struct {
	from         string
	to           []string
	cc           []string
	bcc          []string
	subject      string
	body         string
	bodyNoRecord string
}

type emailHandler struct {
	emailMetadata emailMetadata
	handlers      fs.WriteHandler
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
	emailHandlers         map[string]emailHandler
	newHandlers           func() (fs.WriteHandler, error)

	storageConfig StorageConfig
	ossclient     *osssink.Client
}

// NewSink creates a new SMTPSink
func NewSink(commonSink common.Sink,
	connectionDSN string, from, to, subject, bodyFilePath, bodyNoRecordFilePath, attachment string,
	storageConfig StorageConfig,
	skipHeader bool,
	compressionType string, compressionPassword string,
	opts ...common.Option) (*SMTPSink, error) {

	// create SMTP client
	client, err := NewSMTPClient(commonSink.Context(), connectionDSN)
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
	m.to = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_to", to))
	m.cc = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_cc", cc))
	m.bcc = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_bcc", bcc))

	m.subject = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_subject", subject))
	body, err := os.ReadFile(bodyFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	m.body, m.attachmentBody, err = separateBodyAndAttachmentBodyTemplate(string(body))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if bodyNoRecordFilePath != "" {
		bodyNoRecord, err := os.ReadFile(bodyNoRecordFilePath)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		m.bodyNoRecord = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_body_no_record", string(bodyNoRecord)))
	}

	m.attachment = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_attachment", attachment))

	s := &SMTPSink{
		Sink:                  commonSink,
		client:                client,
		emailMetadataTemplate: m,
		emailHandlers:         make(map[string]emailHandler),
		storageConfig:         storageConfig,
	}

	commonSink.Logger().Info(fmt.Sprintf("using smtp sink with storage: %s", storageConfig.Mode))

	// prepare handlers
	if storageConfig.Mode == "oss" {
		ossclient, err := osssink.NewOSSClient(commonSink.Context(), storageConfig.Credentials, osssink.OSSClientConfig{})
		if err != nil {
			return nil, errors.WithStack(err)
		}
		s.ossclient = ossclient
		// create new oss write handler
		s.newHandlers = func() (fs.WriteHandler, error) {
			return osssink.NewOSSHandler(commonSink.Context(), commonSink.Logger(), ossclient, true,
				fs.WithWriteConcurrentFunc(commonSink.ConcurrentTasks),
				fs.WithWriteCompression(compressionType),
				fs.WithWriteCompressionStaticDestinationURI(storageConfig.DestinationDir),
				fs.WithWriteCompressionPassword(compressionPassword),
				fs.WithWriteChunkOptions(xio.WithCSVSkipHeader(skipHeader)),
			)
		}
	} else {
		// create new file write handler
		s.newHandlers = func() (fs.WriteHandler, error) {
			return file.NewFileHandler(commonSink.Context(), commonSink.Logger(),
				fs.WithWriteConcurrentFunc(commonSink.ConcurrentTasks),
				fs.WithWriteCompression(compressionType),
				fs.WithWriteCompressionStaticDestinationURI(storageConfig.DestinationDir),
				fs.WithWriteCompressionPassword(compressionPassword),
				fs.WithWriteChunkOptions(xio.WithCSVSkipHeader(skipHeader)),
			)
		}
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s.Logger().Info(fmt.Sprintf("close smtp client"))
		s.client.Close()
		s.Logger().Info(fmt.Sprintf("closing email handlers"))
		for _, eh := range s.emailHandlers {
			eh.handlers.Close()
		}
		return nil
	})

	// register sink process
	commonSink.RegisterProcess(s.process)

	return s, nil
}

func (s *SMTPSink) process() error {
	recordCounter := 0
	for record, err := range s.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}

		m, err := compileMetadata(s.emailMetadataTemplate, model.ToMap(record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("compile metadata error: %s", err.Error()))
			return errors.WithStack(err)
		}

		attachment, err := compiler.Compile(s.emailMetadataTemplate.attachment, model.ToMap(record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("compile attachment error: %s", err.Error()))
			return errors.WithStack(err)
		}

		hash := hashMetadata(m)
		eh, ok := s.emailHandlers[hash]
		if !ok {
			handlers, err := s.newHandlers()
			if err != nil {
				s.Logger().Error(fmt.Sprintf("create new handlers error: %s", err.Error()))
				return errors.WithStack(err)
			}

			s.emailHandlers[hash] = emailHandler{emailMetadata: m, handlers: handlers}
			eh = s.emailHandlers[hash]
		}

		if s.IsSpecializedMetadataRecord(record) {
			s.Logger().Debug("skip specialized metadata record")
			continue
		}

		// construct destination URI
		destinationURI := constructFileURI(m, attachment)
		if s.storageConfig.Mode == "oss" {
			destinationURI = constructOSSURI(m, attachment, s.storageConfig.DestinationDir)
		}

		recordWithoutMetadata := s.RecordWithoutMetadata(record)
		raw, err := json.MarshalWithOption(recordWithoutMetadata, json.DisableHTMLEscape())
		if err != nil {
			s.Logger().Error(fmt.Sprintf("marshal error: %s", err.Error()))
			return errors.WithStack(err)
		}

		err = s.DryRunable(func() error {
			if err := eh.handlers.Write(destinationURI, append(raw, '\n')); err != nil {
				s.Logger().Error(fmt.Sprintf("write error: %s", err.Error()))
				return errors.WithStack(err)
			}
			recordCounter++
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	if recordCounter == 0 {
		s.Logger().Info(fmt.Sprintf("no records to write"))
		return nil
	}

	// sync all handlers
	for _, eh := range s.emailHandlers {
		if err := s.DryRunable(eh.handlers.Sync); err != nil {
			return errors.WithStack(err)
		}
	}

	// create email with attachments
	emailWithAttachments := make(map[string]*emailWithAttachment)
	for hash, eh := range s.emailHandlers {
		emailWithAttachments[hash] = &emailWithAttachment{
			emailMetadata: eh.emailMetadata,
			attachments:   eh.handlers.DestinationURIs(),
		}
	}

	// send email
	for hash, eh := range s.emailHandlers {
		s.Logger().Info(fmt.Sprintf("send email to %s, cc %s, bcc %s", eh.emailMetadata.to, eh.emailMetadata.cc, eh.emailMetadata.bcc))

		if err := s.Retry(func() error {
			return s.DryRunable(func() error {
				body := eh.emailMetadata.body

				attachmentReaders := map[string]io.ReadSeeker{}
				attachmentLinks := []attachmentBody{}

				// modify email body to include attachments if storage mode is oss
				if s.storageConfig.Mode == "oss" {
					for _, ossURI := range emailWithAttachments[hash].attachments {
						url, err := s.ossclient.GeneratePresignURL(ossURI, s.storageConfig.LinkExpiration)
						if err != nil {
							s.Logger().Error(fmt.Sprintf("failed to generate presigned URL for %s: %s", ossURI, err.Error()))
							return errors.WithStack(err)
						}
						s.Logger().Debug(fmt.Sprintf("generated presigned URL for %s: %s", filepath.Base(ossURI), url))
						attachmentLinks = append(attachmentLinks, attachmentBody{
							Filename: filepath.Base(ossURI),
							URI:      url,
						})
					}

					// compile body with attachment links
					var err error
					body, err = compileAttachmentBodyTemplate(eh.emailMetadata.body, s.emailMetadataTemplate.attachmentBody, attachmentLinks)
					if err != nil {
						s.Logger().Warn(fmt.Sprintf("failed to compile body with attachment links: %s. Using previous body email", err.Error()))
					}
				} else {
					// for file storage, we need to open the files and pass them as readers
					for _, fileURI := range emailWithAttachments[hash].attachments {
						r, err := openFileURI(fileURI)
						if err != nil {
							return errors.WithStack(err)
						}
						attachmentReaders[filepath.Base(fileURI)] = r
					}
				}

				if len(attachmentReaders) == 0 && len(attachmentLinks) == 0 && eh.emailMetadata.bodyNoRecord != "" {
					// when there is no attachment, use bodyNoRecord
					body = eh.emailMetadata.bodyNoRecord
				}

				return s.client.SendMail(
					eh.emailMetadata.from,
					eh.emailMetadata.to,
					eh.emailMetadata.cc,
					eh.emailMetadata.bcc,
					eh.emailMetadata.subject,
					body,
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
