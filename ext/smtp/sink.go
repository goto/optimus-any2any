package smtp

import (
	"crypto/md5"
	errs "errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/goccy/go-json"
	osssink "github.com/goto/optimus-any2any/ext/oss"
	"github.com/goto/optimus-any2any/internal/archive"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
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

type emailMetadataTemplate struct {
	from           *template.Template
	to             []*template.Template
	cc             []*template.Template
	bcc            []*template.Template
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
	emailHandlers         map[string]emailHandler

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
	connectionDSN string, from, to, subject, bodyFilePath, bodyNoRecordFilePath, attachment string,
	storageConfig StorageConfig,
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
		writeHandlers:         make(map[string]xio.WriteFlushCloser),
		emailWithAttachments:  make(map[string]emailWithAttachment),
		fileRecordCounters:    make(map[string]int),
		// skipheader is set to false by default
		skipHeader: false,
		// remove existing data by default
		enableOverwrite: true,
		storageConfig:   storageConfig,
		// archive options
		enableArchive:       compressionType != "",
		compressionType:     compressionType,
		compressionPassword: compressionPassword,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s.Logger().Info(fmt.Sprintf("close smtp client"))
		return client.Close()
	})

	// register sink process
	s.Logger().Info(fmt.Sprintf("using smtp sink with storage: %s", storageConfig.Mode))
	if strings.ToLower(storageConfig.Mode) == "oss" {
		client, err := osssink.NewOSSClient(storageConfig.Credentials, osssink.OSSClientConfig{})
		if err != nil {
			return nil, errors.WithStack(err)
		}
		s.ossclient = client
		s.ossDestinationDir = storageConfig.DestinationDir
		s.ossLinkExpiration = time.Duration(storageConfig.LinkExpiration) * time.Second

		commonSink.AddCleanFunc(func() error {
			s.Logger().Info("close oss files")
			var e error
			for destinationURI, oh := range s.writeHandlers {
				s.Logger().Debug(fmt.Sprintf("close file: %s", destinationURI))
				err := oh.Close()
				e = errs.Join(e, err)
			}
			return e
		})

		commonSink.RegisterProcess(s.processWithOSS)
	} else {
		commonSink.AddCleanFunc(func() error {
			var e error

			for _, eh := range s.emailHandlers {
				for attachment, _ := range eh.writeHandlers {
					attachmentPath := getAttachmentPath(eh.emailMetadata, attachment)
					s.Logger().Info(fmt.Sprintf("remove tmp attachment file %s", attachmentPath))
					err := os.Remove(attachmentPath)
					e = errs.Join(e, err)
				}
			}

			return e
		})

		commonSink.RegisterProcess(s.process)
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

func (s *SMTPSink) processWithOSS() error {
	logCheckPoint := 1000
	recordCounter := 0

	for record, err := range s.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		if s.IsSpecializedMetadataRecord(record) {
			s.Logger().Debug("skip specialized metadata record")
			continue
		}

		attachment, err := compiler.Compile(s.emailMetadataTemplate.attachment, model.ToMap(record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("compile attachment error: %s", err.Error()))
			return errors.WithStack(err)
		}

		m, err := compileMetadata(s.emailMetadataTemplate, model.ToMap(record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("compile metadata error: %s", err.Error()))
			return errors.WithStack(err)
		}

		// get hash
		hash := hashMetadata(m)
		eh, ok := s.emailWithAttachments[hash]
		if !ok {
			s.emailWithAttachments[hash] = emailWithAttachment{
				emailMetadata: m,
				attachments:   []string{},
			}
			eh = s.emailWithAttachments[hash]
		}

		if s.IsSpecializedMetadataRecord(record) {
			s.Logger().Debug("skip specialized metadata record")
			continue
		}

		// TODO: if the processes below is exported into a new struct, we only need to provide the relative attachment path
		// and the new struct can figure out where to put the tmp file & the oss file
		var attachmentPath string
		if s.enableArchive {
			attachmentPath = getAttachmentPath(m, attachment)
		} else {
			attachmentPath = getOSSPath(m, attachment, s.ossDestinationDir)
		}
		eh.attachments = append(eh.attachments, attachmentPath)
		wh, ok := s.writeHandlers[attachmentPath]
		if !ok {
			// remove object if overwrite is enabled
			var oh io.Writer
			if s.enableArchive {
				oh, err = xio.NewWriteHandler(s.Logger(), attachmentPath)
				if err != nil {
					s.Logger().Error(fmt.Sprintf("failed to create write handler: %s", err.Error()))
					return errors.WithStack(err)
				}
			} else {
				// create new oss write handler
				oh, err = s.newOSSWriter(attachmentPath, s.enableOverwrite)
				if err != nil {
					s.Logger().Error(fmt.Sprintf("failed to create oss write handler: %s", err.Error()))
					return errors.WithStack(err)
				}
			}

			wh = xio.NewChunkWriter(
				s.Logger(), oh,
				xio.WithExtension(filepath.Ext(attachmentPath)),
				xio.WithCSVSkipHeader(s.skipHeader),
			)

			s.writeHandlers[attachmentPath] = wh
			s.emailWithAttachments[hash] = eh
		}

		recordWithoutMetadata := s.RecordWithoutMetadata(record)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			s.Logger().Error(fmt.Sprintf("marshal error: %s", err.Error()))
			return errors.WithStack(err)
		}

		err = s.DryRunable(func() error {
			if _, err = wh.Write(append(raw, '\n')); err != nil {
				s.Logger().Error(fmt.Sprintf("write error: %s", err.Error()))
				return errors.WithStack(err)
			}

			recordCounter++
			s.fileRecordCounters[attachmentPath]++
			if recordCounter%logCheckPoint == 0 {
				s.Logger().Info(fmt.Sprintf("written %d records to file: %s", s.fileRecordCounters[attachmentPath], attachmentPath))
			}

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

	// flush remaining records concurrently
	funcs := []func() error{}
	for destinationURI, wh := range s.writeHandlers {
		funcs = append(funcs, func() error {
			err := s.DryRunable(func() error {
				if err := wh.Flush(); err != nil {
					s.Logger().Error(fmt.Sprintf("failed to flush to %s", destinationURI))
					return errors.WithStack(err)
				}
				s.Logger().Info(fmt.Sprintf("flushed %d records to file: %s", s.fileRecordCounters[destinationURI], destinationURI))
				return nil
			})
			return errors.WithStack(err)
		})
	}
	if err := s.ConcurrentTasks(funcs); err != nil {
		return errors.WithStack(err)
	}

	s.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))

	if s.enableArchive {
		err := s.DryRunable(func() error {
			for localFileURI, e := range s.emailWithAttachments {
				pathsToArchive := e.attachments

				s.Logger().Info(fmt.Sprintf("compressing %d files: %s", len(pathsToArchive), strings.Join(pathsToArchive, ", ")))

				archivePaths, err := s.archiveToOSS(pathsToArchive, e.emailMetadata)
				if err != nil {
					s.Logger().Error(fmt.Sprintf("failed to compress files: %s", err.Error()))
					return errors.WithStack(err)
				}
				s.Logger().Info(fmt.Sprintf("successfully uploaded archive file to OSS"))

				existing := s.emailWithAttachments[localFileURI]
				s.emailWithAttachments[localFileURI] = emailWithAttachment{
					emailMetadata: existing.emailMetadata,
					attachments:   archivePaths,
				}
			}

			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Generate presigned URLs for all files in ossHandlers
	// Presigned URLs must be generated after all files are finished uploading
	presignedURLs := map[string]string{}
	for uri, e := range s.emailWithAttachments {
		for _, attachmentURI := range e.attachments {
			targetDestinationURI, err := url.Parse(attachmentURI)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to parse destination URI: %s", uri))
				return errors.WithStack(err)
			}
			if targetDestinationURI.Scheme != "oss" {
				s.Logger().Error(fmt.Sprintf("invalid scheme: %s", targetDestinationURI.Scheme))
				return errors.WithStack(err)
			}

			// generate presigned URL
			err = s.DryRunable(func() error {
				presignedURL, err := s.ossclient.Presign(s.Context(), &oss.GetObjectRequest{
					Bucket: oss.Ptr(targetDestinationURI.Host),
					Key:    oss.Ptr(strings.TrimLeft(targetDestinationURI.Path, "/")),
				}, oss.PresignExpiration(time.Now().Add(s.ossLinkExpiration))) // 7 days
				if err != nil {
					s.Logger().Error(fmt.Sprintf("failed to generate presigned URL for: %s", uri))
					return errors.WithStack(err)
				}

				presignedURLs[attachmentURI] = presignedURL.URL
				s.Logger().Info(fmt.Sprintf("generated presigned URL for %s: %s", uri, presignedURL.URL))
				return nil
			})
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}

	// send email
	for _, eh := range s.emailWithAttachments {
		s.Logger().Info(fmt.Sprintf("send email to %s, cc %s, bcc %s", eh.emailMetadata.to, eh.emailMetadata.cc, eh.emailMetadata.bcc))

		attachmentLinks := []attachmentBody{}
		for _, attachment := range eh.attachments {
			fileName := filepath.Base(attachment)
			presignedURL, ok := presignedURLs[attachment]
			if !ok {
				s.Logger().Warn(fmt.Sprintf("failed to get presigned URL for %s. Skipping attachment...", attachment))
				continue
			}

			attachmentLinks = append(attachmentLinks, attachmentBody{
				Filename: fileName,
				URI:      presignedURL,
			})
		}

		// compile body with attachment links
		newBody, err := compileAttachmentBodyTemplate(eh.emailMetadata.body, s.emailMetadataTemplate.attachmentBody, attachmentLinks)
		if err != nil {
			s.Logger().Warn(fmt.Sprintf("failed to compile body with attachment links: %s. Using previous body email", err.Error()))
			newBody = eh.emailMetadata.body
		}
		if len(attachmentLinks) == 0 && eh.emailMetadata.bodyNoRecord != "" {
			// if there are no attachments, use bodyNoRecord
			newBody = eh.emailMetadata.bodyNoRecord
		}

		if err := s.Retry(func() error {
			return s.DryRunable(func() error {
				return s.client.SendMail(
					eh.emailMetadata.from,
					eh.emailMetadata.to,
					eh.emailMetadata.cc,
					eh.emailMetadata.bcc,
					eh.emailMetadata.subject,
					newBody,
					nil,
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

func (s *SMTPSink) remove(bucket, path string) error {
	var response *oss.DeleteObjectResult
	var err error
	// remove object
	err = s.DryRunable(func() error {
		response, err = s.ossclient.DeleteObject(s.Context(), &oss.DeleteObjectRequest{
			Bucket: oss.Ptr(bucket),
			Key:    oss.Ptr(path),
		})
		return errors.WithStack(err)
	})
	if err != nil {
		return errors.WithStack(err)
	}
	if response.StatusCode >= 400 {
		err := errors.New(fmt.Sprintf("failed to delete object: %d", response.StatusCode))
		return errors.WithStack(err)
	}
	s.Logger().Info(fmt.Sprintf("delete %s objects", path))
	return nil
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

		hash := hashMetadata(m)
		eh, ok := s.emailHandlers[hash]
		if !ok {
			s.emailHandlers[hash] = emailHandler{
				emailMetadata: m,
				writeHandlers: make(map[string]xio.WriteFlushCloser),
			}
			eh = s.emailHandlers[hash]
		}

		if s.IsSpecializedMetadataRecord(record) {
			s.Logger().Debug("skip specialized metadata record")
			continue
		}

		attachment, err := compiler.Compile(s.emailMetadataTemplate.attachment, model.ToMap(record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("compile attachment error: %s", err.Error()))
			return errors.WithStack(err)
		}

		attachmentPath := getAttachmentPath(m, attachment)
		wh, ok := eh.writeHandlers[attachment]
		if !ok {
			fh, err := xio.NewWriteHandler(s.Logger(), attachmentPath)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("create write handler error: %s", err.Error()))
				return errors.WithStack(err)
			}
			wh = xio.NewChunkWriter(
				s.Logger(), fh,
				xio.WithExtension(filepath.Ext(attachment)),
			)
			eh.writeHandlers[attachment] = wh
		}

		recordWithoutMetadata := s.RecordWithoutMetadata(record)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			s.Logger().Error(fmt.Sprintf("marshal error: %s", err.Error()))
			return errors.WithStack(err)
		}

		err = s.DryRunable(func() error {
			if _, err = wh.Write(append(raw, '\n')); err != nil {
				s.Logger().Error(fmt.Sprintf("write error: %s", err.Error()))
				return errors.WithStack(err)
			}
			recordCounter++
			s.fileRecordCounters[attachmentPath]++
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

	// send email
	for _, eh := range s.emailHandlers {
		attachmentReaders := map[string]io.Reader{}

		for _, wh := range eh.writeHandlers {
			// flush write handler first
			if err := s.DryRunable(wh.Flush); err != nil {
				s.Logger().Error(fmt.Sprintf("flush write handler error: %s", err.Error()))
				return errors.WithStack(err)
			}
		}

		if s.enableArchive {
			pathsToArchive := []string{}
			for attachment := range eh.writeHandlers {
				attachmentPath := getAttachmentPath(eh.emailMetadata, attachment)
				pathsToArchive = append(pathsToArchive, attachmentPath)
			}
			s.Logger().Info(fmt.Sprintf("compressing %d files: %s", len(pathsToArchive), strings.Join(pathsToArchive, ", ")))

			// archive files
			var archivePaths []string
			err := s.DryRunable(func() error {
				var err error
				archivePaths, err = s.archive(pathsToArchive, eh.emailMetadata)
				if err != nil {
					s.Logger().Error(fmt.Sprintf("failed to archive files: %s", err.Error()))
					return errors.WithStack(err)
				}
				return nil
			})
			if err != nil {
				return errors.WithStack(err)
			}

			// upload archive files to oss
			for _, archivePath := range archivePaths {
				tmpReader, err := os.OpenFile(archivePath, os.O_RDONLY, 0644)
				if err != nil {
					s.Logger().Error(fmt.Sprintf("open attachment file error: %s", err.Error()))
					return errors.WithStack(err)
				}

				attachmentReaders[filepath.Base(archivePath)] = tmpReader
			}
		} else {
			// open attachment file from tmp folder
			for attachment := range eh.writeHandlers {
				attachmentPath := getAttachmentPath(eh.emailMetadata, attachment)
				tmpReader, err := os.OpenFile(attachmentPath, os.O_RDONLY, 0644)
				if err != nil {
					s.Logger().Error(fmt.Sprintf("open attachment file error: %s", err.Error()))
					return errors.WithStack(err)
				}

				attachmentReaders[attachment] = tmpReader
			}
		}

		s.Logger().Info(fmt.Sprintf("send email to %s, cc %s, bcc %s", eh.emailMetadata.to, eh.emailMetadata.cc, eh.emailMetadata.bcc))
		if err := s.Retry(func() error {
			return s.DryRunable(func() error {
				body := eh.emailMetadata.body
				if len(attachmentReaders) == 0 && eh.emailMetadata.bodyNoRecord != "" {
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

	if m.bodyNoRecord != nil {
		bodyNoRecord, err := compiler.Compile(m.bodyNoRecord, record)
		if err != nil {
			return em, errors.WithStack(err)
		}
		em.bodyNoRecord = bodyNoRecord
	}

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

func getOSSPath(em emailMetadata, attachment string, ossDestinationDir string) string {
	return fmt.Sprintf("%s/%s/%s", strings.TrimRight(ossDestinationDir, "/"), hashMetadata(em), attachment)
}

func getOSS(em emailMetadata, attachment string, ossDestinationDir string) string {
	return fmt.Sprintf("%s/%s/%s", strings.TrimRight(ossDestinationDir, "/"), hashMetadata(em), attachment)
}

func (s *SMTPSink) archive(filesToArchive []string, em emailMetadata) ([]string, error) {
	archiveDir := filepath.Join(tmpFolder, hashMetadata(em))

	var archiveDestinationPaths []string
	switch s.compressionType {
	case "gz":
		for _, filePath := range filesToArchive {
			fileName := fmt.Sprintf("%s.gz", filepath.Base(filePath))
			archiveDestinationPath := filepath.Join(archiveDir, fileName)
			archiveDestinationPaths = append(archiveDestinationPaths, archiveDestinationPath)

			archiveWriter, err := xio.NewWriteHandler(s.Logger(), archiveDestinationPath)
			if err != nil {
				return archiveDestinationPaths, errors.WithStack(err)
			}

			archiver := archive.NewFileArchiver(s.Logger(), archive.WithExtension("gz"))
			if err := archiver.Archive([]string{filePath}, archiveWriter); err != nil {
				return nil, errors.WithStack(err)
			}

			if err := archiveWriter.Flush(); err != nil {
				s.Logger().Error(fmt.Sprintf("failed to flush archive to %s: %s", archiveDestinationPath, err.Error()))
				return nil, errors.WithStack(err)
			}
		}
	case "zip", "tar.gz":
		// for zip & tar.gz file, the whole file is archived into a single archive file
		// whose file name is deferred from the destination URI
		re := strings.NewReplacer("{{", "", "}}", "", "{{ ", "", " }}", "")
		baseFileName := strings.TrimSuffix(re.Replace(s.emailMetadataTemplate.attachment.Root.String()), filepath.Ext(s.emailMetadataTemplate.attachment.Root.String()))
		fileName := fmt.Sprintf("%s.%s", baseFileName, s.compressionType)
		archiveDestinationPath := filepath.Join(archiveDir, fileName)
		archiveDestinationPaths = append(archiveDestinationPaths, archiveDestinationPath)

		archiveWriter, err := xio.NewWriteHandler(s.Logger(), archiveDestinationPath)
		if err != nil {
			return archiveDestinationPaths, errors.WithStack(err)
		}

		archiver := archive.NewFileArchiver(s.Logger(), archive.WithExtension(s.compressionType), archive.WithPassword(s.compressionPassword))
		if err := archiver.Archive(filesToArchive, archiveWriter); err != nil {
			return nil, errors.WithStack(err)
		}
		if err := archiveWriter.Flush(); err != nil {
			s.Logger().Error(fmt.Sprintf("failed to flush archive to %s: %s", archiveDestinationPath, err.Error()))
			return nil, errors.WithStack(err)
		}
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", s.compressionType)
	}

	return archiveDestinationPaths, nil
}

func (s *SMTPSink) archiveToOSS(filesToArchive []string, em emailMetadata) ([]string, error) {
	destinationDir := fmt.Sprintf("%s/%s", s.ossDestinationDir, hashMetadata(em))

	var archiveDestinationPaths []string
	switch s.compressionType {
	case "gz":
		for _, filePath := range filesToArchive {
			fileName := fmt.Sprintf("%s.gz", filepath.Base(filePath))
			archiveDestinationPath := fmt.Sprintf("%s/%s", destinationDir, fileName)
			archiveDestinationPaths = append(archiveDestinationPaths, archiveDestinationPath)

			archiveWriter, err := s.newOSSWriter(archiveDestinationPath, s.enableOverwrite)
			if err != nil {
				return archiveDestinationPaths, errors.WithStack(err)
			}
			defer archiveWriter.Close()

			archiver := archive.NewFileArchiver(s.Logger(), archive.WithExtension("gz"))
			if err := archiver.Archive([]string{filePath}, archiveWriter); err != nil {
				return nil, errors.WithStack(err)
			}
		}
	case "zip", "tar.gz":
		// for zip & tar.gz file, the whole file is archived into a single archive file
		// whose file name is deferred from the destination URI
		re := strings.NewReplacer("{{", "", "}}", "", "{{ ", "", " }}", "")
		baseFileName := strings.TrimSuffix(re.Replace(s.emailMetadataTemplate.attachment.Root.String()), filepath.Ext(s.emailMetadataTemplate.attachment.Root.String()))
		fileName := fmt.Sprintf("%s.%s", baseFileName, s.compressionType)
		archiveDestinationPath := fmt.Sprintf("%s/%s", destinationDir, fileName)
		archiveDestinationPaths = append(archiveDestinationPaths, archiveDestinationPath)

		archiveWriter, err := s.newOSSWriter(archiveDestinationPath, s.enableOverwrite)
		if err != nil {
			return archiveDestinationPaths, errors.WithStack(err)
		}
		defer archiveWriter.Close()

		archiver := archive.NewFileArchiver(s.Logger(), archive.WithExtension(s.compressionType), archive.WithPassword(s.compressionPassword))
		if err := archiver.Archive(filesToArchive, archiveWriter); err != nil {
			return archiveDestinationPaths, errors.WithStack(err)
		}
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", s.compressionType)
	}

	return archiveDestinationPaths, nil
}

func (s *SMTPSink) newOSSWriter(fullPath string, shouldOverwrite bool) (io.WriteCloser, error) {
	// create new oss write handler
	targetDestinationURI, err := url.Parse(fullPath)
	if err != nil {
		s.Logger().Error(fmt.Sprintf("failed to parse destination URI: %s", fullPath))
		return nil, errors.WithStack(err)
	}
	if targetDestinationURI.Scheme != "oss" {
		s.Logger().Error(fmt.Sprintf("invalid scheme: %s", targetDestinationURI.Scheme))
		return nil, errors.WithStack(err)
	}

	if shouldOverwrite {
		err = s.DryRunable(func() error {
			s.Logger().Info(fmt.Sprintf("remove object: %s", fullPath))
			if err := s.Retry(func() error {
				err := s.remove(targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
				return err
			}); err != nil {
				s.Logger().Error(fmt.Sprintf("failed to remove object: %s", fullPath))
				return errors.WithStack(err)
			}
			return nil
		})
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	var oh io.WriteCloser
	err = s.Retry(func() error {
		var err error
		oh, err = oss.NewAppendFile(s.Context(), s.ossclient, targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
		return errors.WithStack(err)
	})
	if err != nil {
		s.Logger().Error(fmt.Sprintf("failed to create oss write handler: %s", err.Error()))
		return nil, errors.WithStack(err)
	}

	return oh, nil
}
