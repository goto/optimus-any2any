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
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/helper"
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
	attachmentBody *template.Template
	attachment     *template.Template
}

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
	writeHandlers map[string]xio.WriteFlusher
}

type emailWithAttachment struct {
	emailMetadata emailMetadata
	// map filename to url path
	attachments []string
}

type fileHandler struct {
	tmpPath    string
	tmpHandler xio.WriteFlusher
	ossPath    string
	ossHandler io.WriteCloser
}

type SMTPSink struct {
	common.Sink
	client *SMTPClient

	emailMetadataTemplate emailMetadataTemplate
	emailHandlers         map[string]emailHandler

	// TODO move this to shared package
	ossclient               *oss.Client
	writeHandlers           map[string]xio.WriteFlusher // tmp write handler
	ossHandlers             map[string]io.WriteCloser
	fileRecordCounters      map[string]int
	enableOverwrite         bool
	skipHeader              bool
	maxTempFileRecordNumber int
	ossDestinationDir       string
	ossLinkExpiration       time.Duration
	emailWithAttachments    map[string]emailWithAttachment

	partialFileRecordCounters map[string]int // map of destinationURI to the number of records
}

// NewSink creates a new SMTPSink
func NewSink(commonSink common.Sink,
	connectionDSN string, from, to, subject, bodyFilePath, attachment string,
	storageConfig StorageConfig,
	opts ...common.Option) (*SMTPSink, error) {

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
	m.body, m.attachmentBody, err = separateBodyAndAttachmentBodyTemplate(string(body))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	m.attachment = template.Must(compiler.NewTemplate("sink_smtp_email_metadata_attachment", attachment))

	s := &SMTPSink{
		Sink:                      commonSink,
		client:                    client,
		emailMetadataTemplate:     m,
		emailHandlers:             make(map[string]emailHandler),
		writeHandlers:             make(map[string]xio.WriteFlusher),
		emailWithAttachments:      make(map[string]emailWithAttachment),
		ossHandlers:               make(map[string]io.WriteCloser),
		fileRecordCounters:        make(map[string]int),
		partialFileRecordCounters: make(map[string]int),
		// TODO skipheader is set to false by default
		skipHeader: false,
		// remove existing data by default
		enableOverwrite: true,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s.Logger().Info(fmt.Sprintf("close smtp client"))
		return client.Close()
	})

	// register sink process
	s.Logger().Info(fmt.Sprintf("using smtp sink with storage: %s", storageConfig.Mode))
	if strings.ToLower(storageConfig.Mode) == "oss" {
		client, err := osssink.NewOSSClient(storageConfig.Credentials)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		s.ossclient = client
		s.ossDestinationDir = storageConfig.DestinationDir
		s.ossLinkExpiration = time.Duration(storageConfig.LinkExpiration) * time.Second

		commonSink.AddCleanFunc(func() error {
			s.Logger().Info("close oss files")
			var e error
			for destinationURI, oh := range s.ossHandlers {
				s.Logger().Debug(fmt.Sprintf("close file: %s", destinationURI))
				err := oh.Close()
				e = errs.Join(e, err)
			}
			return e
		})
		commonSink.AddCleanFunc(func() error {
			s.Logger().Info("remove tmp files")
			var e error
			for tmpPath := range s.writeHandlers {
				s.Logger().Debug(fmt.Sprintf("close tmp file: %s", tmpPath))

				s.Logger().Debug(fmt.Sprintf("remove tmp file: %s", tmpPath))
				err := os.Remove(tmpPath)
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
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

		// stream to tmp file
		// TODO: if the processes below is exported into a new struct, we only need to provide the relative attachment path
		// and the new struct can figure out where to put the tmp file & the oss file
		ossPath := getOSSPath(m, attachment, s.ossDestinationDir)
		eh.attachments = append(eh.attachments, ossPath)
		attachmentPath, err := getTmpPath(ossPath)
		if err != nil {
			s.Logger().Error(fmt.Sprintf("failed to get tmp URI: %s", ossPath))
			return errors.WithStack(err)
		}

		wh, ok := s.writeHandlers[attachmentPath]
		if !ok {
			// create new tmp write handler
			wh, err = xio.NewWriteHandler(s.Logger(), attachmentPath)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to create tmp write handler: %s", err.Error()))
				return errors.WithStack(err)
			}

			// create new oss write handler
			targetDestinationURI, err := url.Parse(ossPath)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to parse destination URI: %s", ossPath))
				return errors.WithStack(err)
			}
			if targetDestinationURI.Scheme != "oss" {
				s.Logger().Error(fmt.Sprintf("invalid scheme: %s", targetDestinationURI.Scheme))
				return errors.WithStack(err)
			}
			// remove object if overwrite is enabled
			if _, ok := s.ossHandlers[ossPath]; !ok && s.enableOverwrite {
				s.Logger().Info(fmt.Sprintf("remove object: %s", ossPath))
				if err := s.Retry(func() error {
					err := s.remove(targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
					return err
				}); err != nil {
					s.Logger().Error(fmt.Sprintf("failed to remove object: %s", ossPath))
					return errors.WithStack(err)
				}
			}
			var oh io.WriteCloser
			if err := s.Retry(func() (err error) {
				oh, err = oss.NewAppendFile(s.Context(), s.ossclient, targetDestinationURI.Host, strings.TrimLeft(targetDestinationURI.Path, "/"))
				return
			}); err != nil {
				s.Logger().Error(fmt.Sprintf("failed to create oss write handler: %s", err.Error()))
				return errors.WithStack(err)
			}

			// dual handlers for both tmp file and oss
			s.writeHandlers[attachmentPath] = wh
			s.ossHandlers[ossPath] = oh
			s.emailWithAttachments[hash] = eh
		}

		recordWithoutMetadata := s.RecordWithoutMetadata(record)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			s.Logger().Error(fmt.Sprintf("marshal error: %s", err.Error()))
			return errors.WithStack(err)
		}

		if _, err = wh.Write(append(raw, '\n')); err != nil {
			s.Logger().Error(fmt.Sprintf("write error: %s", err.Error()))
			return errors.WithStack(err)
		}

		recordCounter++
		s.fileRecordCounters[attachmentPath]++
		s.partialFileRecordCounters[ossPath]++
		if recordCounter%logCheckPoint == 0 {
			s.Logger().Info(fmt.Sprintf("written %d records to tmp file: %s", s.fileRecordCounters[attachmentPath], attachmentPath))
		}

		// EXPERIMENTAL: flush and upload to OSS if maximum number of records in tmp file is reached.
		// a method of partial upload is implemented to avoid having large number of tmp files which leads to high disk usage.
		// as long as the streamed records from source are guaranteed to be uniform (same header & value ordering), this should work.
		if s.maxTempFileRecordNumber > 0 && s.partialFileRecordCounters[ossPath] >= s.maxTempFileRecordNumber {
			s.Logger().Debug(fmt.Sprintf("maximum number of temp file records %d reached. flushing %s to OSS", s.maxTempFileRecordNumber, attachmentPath))
			if err := wh.Flush(); err != nil {
				s.Logger().Error(fmt.Sprintf("failed to flush tmp file: %s", attachmentPath))
				return errors.WithStack(err)
			}
			s.Logger().Debug(fmt.Sprintf("flushed tmp file: %s", attachmentPath))

			if err := s.flushToOSS(ossPath, s.ossHandlers[ossPath]); err != nil {
				s.Logger().Error(fmt.Sprintf("failed to upload to OSS: %s", ossPath))
				return errors.WithStack(err)
			}

			// reset counters and handlers for the current batch
			// except for oss handler which is reused to upload the next part of the file
			s.partialFileRecordCounters[ossPath] = 0
			delete(s.writeHandlers, attachmentPath)

			if err := os.Remove(attachmentPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				s.Logger().Error(fmt.Sprintf("failed to remove tmp file: %s", attachmentPath))
				return errors.WithStack(err)
			}
		}
	}

	if recordCounter == 0 {
		s.Logger().Info(fmt.Sprintf("no records to write"))
		return nil
	}

	// flush remaining tmp files
	for tmpPath, wh := range s.writeHandlers {
		if err := wh.Flush(); err != nil {
			s.Logger().Error(fmt.Sprintf("failed to flush tmp file: %s", tmpPath))
			return errors.WithStack(err)
		}
		s.Logger().Info(fmt.Sprintf("flushed tmp file: %s", tmpPath))
	}

	// flush remaining records concurrently
	funcs := []func() error{}
	for uri := range s.ossHandlers {
		funcs = append(funcs, func() error {
			return s.flushToOSS(uri, s.ossHandlers[uri])
		})
	}
	if err := s.ConcurrentTasks(s.Context(), 4, funcs); err != nil {
		return errors.WithStack(err)
	}

	s.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))

	// Generate presigned URLs for all files in ossHandlers
	// Presigned URLs must be generated after all files are finished uploading
	presignedURLs := map[string]string{}
	for uri := range s.ossHandlers {
		targetDestinationURI, err := url.Parse(uri)
		if err != nil {
			s.Logger().Error(fmt.Sprintf("failed to parse destination URI: %s", uri))
			return errors.WithStack(err)
		}
		if targetDestinationURI.Scheme != "oss" {
			s.Logger().Error(fmt.Sprintf("invalid scheme: %s", targetDestinationURI.Scheme))
			return errors.WithStack(err)
		}

		presignedURL, err := s.ossclient.Presign(s.Context(), &oss.GetObjectRequest{
			Bucket: oss.Ptr(targetDestinationURI.Host),
			Key:    oss.Ptr(strings.TrimLeft(targetDestinationURI.Path, "/")),
		}, oss.PresignExpiration(time.Now().Add(24*7*time.Hour))) // 7 days
		if err != nil {
			s.Logger().Error(fmt.Sprintf("failed to generate presigned URL for: %s", uri))
			return errors.WithStack(err)
		}

		presignedURLs[uri] = presignedURL.URL
		s.Logger().Info(fmt.Sprintf("generated presigned URL for %s: %s", uri, presignedURL.URL))
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

		if err := s.Retry(func() error {
			return s.client.SendMail(
				eh.emailMetadata.from,
				eh.emailMetadata.to,
				eh.emailMetadata.cc,
				eh.emailMetadata.bcc,
				eh.emailMetadata.subject,
				newBody,
				nil,
			)
		}); err != nil {
			s.Logger().Error(fmt.Sprintf("send mail error: %s", err.Error()))
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *SMTPSink) remove(bucket, path string) error {
	response, err := s.ossclient.DeleteObject(s.Context(), &oss.DeleteObjectRequest{
		Bucket: oss.Ptr(bucket),
		Key:    oss.Ptr(path),
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
				writeHandlers: make(map[string]xio.WriteFlusher),
			}
			eh = s.emailHandlers[hash]
		}

		attachment, err := compiler.Compile(s.emailMetadataTemplate.attachment, model.ToMap(record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("compile attachment error: %s", err.Error()))
			return errors.WithStack(err)
		}
		fh, ok := eh.writeHandlers[attachment]
		if !ok {
			attachmentPath := getAttachmentPath(m, attachment)
			fh, err = xio.NewWriteHandler(s.Logger(), attachmentPath)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("create write handler error: %s", err.Error()))
				return errors.WithStack(err)
			}
			eh.writeHandlers[attachment] = fh
		}

		recordWithoutMetadata := s.RecordWithoutMetadata(record)
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
		attachmentReaders := map[string]io.Reader{}

		for attachment, fh := range eh.writeHandlers {
			// flush write handler first
			if err := fh.Flush(); err != nil {
				s.Logger().Error(fmt.Sprintf("flush write handler error: %s", err.Error()))
				return errors.WithStack(err)
			}
			// open attachment file from tmp folder
			var tmpReader io.ReadSeekCloser
			attachmentPath := getAttachmentPath(eh.emailMetadata, attachment)
			tmpReader, err := os.OpenFile(attachmentPath, os.O_RDONLY, 0644)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("open attachment file error: %s", err.Error()))
				return errors.WithStack(err)
			}

			// convert attachment file to desired format if necessary
			cleanUpFn := func() error { return nil }
			switch filepath.Ext(attachment) {
			case ".json":
				// do nothing
			case ".csv":
				tmpReader, cleanUpFn, err = helper.FromJSONToCSV(s.Logger(), tmpReader, false) // no skip header by default
			case ".tsv":
				tmpReader, cleanUpFn, err = helper.FromJSONToCSV(s.Logger(), tmpReader, false, rune('\t'))
			case ".xlsx":
				tmpReader, cleanUpFn, err = helper.FromJSONToXLSX(s.Logger(), tmpReader, false) // no skip header by default
			default:
				s.Logger().Warn(fmt.Sprintf("unsupported file format: %s, use default (json)", filepath.Ext(attachment)))
				// do nothing
			}
			if err != nil {
				return errors.WithStack(err)
			}
			s.AddCleanFunc(func() error {
				tmpReader.Close()
				cleanUpFn()
				return nil
			})

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

// TODO need to ensure that TMP path has hash metadata in it
func getTmpPath(destinationURI string) (string, error) {
	targetURI, err := url.Parse(destinationURI)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return filepath.Join("/tmp", targetURI.Path), nil
}

func getOSSPath(em emailMetadata, attachment string, ossDestinationDir string) string {
	return fmt.Sprintf("%s/%s/%s", strings.TrimRight(ossDestinationDir, "/"), hashMetadata(em), attachment)
}

func (s *SMTPSink) flushToOSS(destinationURI string, oh io.WriteCloser) error {
	// open tmp file for reading
	tmpPath, err := getTmpPath(destinationURI)
	if err != nil {
		return errors.WithStack(err)
	}

	var tmpReader io.ReadSeekCloser
	tmpReader, err = os.OpenFile(tmpPath, os.O_RDONLY, 0644)
	if err != nil {
		return errors.WithStack(err)
	}
	// header is skipped if SKIP_HEADER is explicitly set to true OR if file has been partially uploaded previously
	skipHeader := s.skipHeader || (s.maxTempFileRecordNumber > 0 && s.fileRecordCounters[tmpPath] > s.maxTempFileRecordNumber)

	cleanUpFn := func() error { return nil }
	// convert to appropriate format if necessary
	switch filepath.Ext(destinationURI) {
	case ".json":
		// do nothing
	case ".csv":
		tmpReader, cleanUpFn, err = helper.FromJSONToCSV(s.Logger(), tmpReader, skipHeader) // no skip header by default
	case ".tsv":
		tmpReader, cleanUpFn, err = helper.FromJSONToCSV(s.Logger(), tmpReader, skipHeader, rune('\t'))
	case ".xlsx":
		tmpReader, cleanUpFn, err = helper.FromJSONToXLSX(s.Logger(), tmpReader, skipHeader) // no skip header by default
	default:
		s.Logger().Warn(fmt.Sprintf("unsupported file format: %s, use default (json)", filepath.Ext(destinationURI)))
		// do nothing
	}
	defer func() {
		cleanUpFn()
		tmpReader.Close()
	}()

	// remove tmp file if destination is csv or tsv
	if filepath.Ext(destinationURI) == ".csv" || filepath.Ext(destinationURI) == ".tsv" {
		if err := os.Remove(tmpPath); err != nil {
			return errors.WithStack(err)
		}
	}

	// previously there was a retry when flushing the records to OSS.
	// but retrying on a failed write (e.g. network error) may cause the reader to skip some records
	// as the read pointer is not reset to the beginning of the file.
	// will get back into this later until we find a better solution.
	s.Logger().Info(fmt.Sprintf("upload tmp file %s to oss %s", tmpPath, destinationURI))
	n, err := io.Copy(oh, tmpReader)
	if err != nil {
		return errors.WithStack(err)
	}

	s.Logger().Debug(fmt.Sprintf("uploaded %d bytes to oss %s", n, destinationURI))
	return nil
}
