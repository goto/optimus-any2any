package smtp

import (
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/gomail.v2"
)

// SMTPClient is a wrapper around gomail.SendCloser
type SMTPClient struct {
	sender gomail.SendCloser
}

// NewSMTPClient creates a new SMTPClient
func NewSMTPClient(connectionDSN string) (*SMTPClient, error) {
	dsn, err := url.Parse(connectionDSN)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if dsn.Scheme != "smtp" {
		return nil, fmt.Errorf("invalid scheme: %s", dsn.Scheme)
	}
	username := dsn.User.Username()
	password, _ := dsn.User.Password()
	address := dsn.Host

	splittedAddr := strings.Split(address, ":")
	host := splittedAddr[0]
	port := 587
	if len(splittedAddr) > 1 {
		p, err := strconv.Atoi(splittedAddr[1])
		if err != nil {
			return nil, errors.WithStack(err)
		}
		port = p
	}

	dialer := gomail.NewDialer(host, port, username, password)
	c, err := dialer.Dial()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &SMTPClient{sender: c}, nil
}

// Close closes the underlying connection
func (c *SMTPClient) Close() error {
	return c.sender.Close()
}

// SendMail sends an email
func (c *SMTPClient) SendMail(from string, to, cc, bcc []string, subject string, msg string, readers map[string]io.Reader) error {
	m := gomail.NewMessage()
	m.SetHeader("From", from)
	m.SetHeader("To", to...)
	if len(cc) > 0 {
		m.SetHeader("Cc", cc...)
	}
	if len(bcc) > 0 {
		m.SetHeader("Bcc", bcc...)
	}
	m.SetHeader("Subject", subject)
	m.SetBody("text/html", msg)

	// attach file from readers
	for attachment, reader := range readers {
		m.Attach(attachment, gomail.SetCopyFunc(func(w io.Writer) error {
			_, err := io.Copy(w, reader)
			return err
		}))
	}

	return c.sender.Send(from, to, m)
}
