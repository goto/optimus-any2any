package smtp

import (
	"github.com/pkg/errors"
	"gopkg.in/gomail.v2"
)

// SMTPClient is a wrapper around gomail.SendCloser
type SMTPClient struct {
	sender gomail.SendCloser
}

// NewSMTPClient creates a new SMTPClient
func NewSMTPClient(address, username, password string) (*SMTPClient, error) {
	dialer := gomail.NewDialer(address, 587, username, password)
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
func (c *SMTPClient) SendMail(from string, to []string, msg []byte, attachment string) error {
	m := gomail.NewMessage()
	m.SetHeader("From", from)
	m.SetHeader("To", to...)
	m.SetBody("text/html", string(msg))
	m.Attach(attachment)
	return c.sender.Send(from, to, m)
}
