// TODO: refactor this file to a common package
package smtp

import (
	"crypto/md5"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/pkg/errors"
)

func openFileURI(fileURI string) (io.ReadSeekCloser, error) {
	u, err := url.Parse(fileURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return os.OpenFile(u.Path, os.O_RDONLY, 0644)
}

func compileMetadata(m emailMetadataTemplate, record map[string]interface{}) (emailMetadata, error) {
	em := emailMetadata{}

	from, err := compiler.Compile(m.from, record)
	if err != nil {
		return em, errors.WithStack(err)
	}
	em.from = from

	to, err := compiler.Compile(m.to, record)
	if err != nil {
		return em, errors.WithStack(err)
	}
	for _, t := range strings.Split(to, ",") {
		em.to = append(em.to, strings.TrimSpace(t))
	}

	cc, err := compiler.Compile(m.cc, record)
	if err != nil {
		return em, errors.WithStack(err)
	}
	for _, c := range strings.Split(cc, ",") {
		em.cc = append(em.cc, strings.TrimSpace(c))
	}

	bcc, err := compiler.Compile(m.bcc, record)
	if err != nil {
		return em, errors.WithStack(err)
	}
	for _, b := range strings.Split(bcc, ",") {
		em.bcc = append(em.bcc, strings.TrimSpace(b))
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
	s := fmt.Sprintf("%s%s%s%s%s%s%s", em.from, strings.Join(em.to, ""), strings.Join(em.cc, ""), strings.Join(em.bcc, ""), em.subject, em.body, em.bodyNoRecord)
	md5sum := md5.Sum([]byte(s))
	return fmt.Sprintf("%x", md5sum)
}

func constructFileURI(em emailMetadata, attachment string) string {
	return "file://" + filepath.Join("/tmp", hashMetadata(em), attachment)
}

func constructOSSURI(em emailMetadata, attachment string, ossDestinationDir string) string {
	ossDestinationDir = strings.TrimRight(ossDestinationDir, "/")
	return fmt.Sprintf("%s/%s/%s", ossDestinationDir, hashMetadata(em), attachment)
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
