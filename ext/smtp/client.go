package smtp

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/wneessen/go-mail"
)

const (
	// default connection timeout set to 2 minutes
	defaultConnTimeout = 120 * time.Second
)

// SMTPClient is a wrapper around gomail.SendCloser
type SMTPClient struct {
	*mail.Client
	ctx context.Context
}

// NewSMTPClient creates a new SMTPClient
func NewSMTPClient(ctx context.Context, connectionDSN string, connectionTimeout int) (*SMTPClient, error) {
	dsn, err := url.Parse(connectionDSN)
	if err != nil {
		err = fmt.Errorf("error parsing connection dsn")
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

	connectionTimeoutDuration := defaultConnTimeout
	if connectionTimeout > 0 {
		connectionTimeoutDuration = time.Duration(connectionTimeout) * time.Second
	}

	client, err := mail.NewClient(host,
		mail.WithTimeout(connectionTimeoutDuration),
		mail.WithTLSPortPolicy(mail.TLSMandatory),
		mail.WithSMTPAuth(mail.SMTPAuthPlain),
		mail.WithUsername(username),
		mail.WithPassword(password),
		mail.WithPort(port),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// dialer := gomail.NewDialer(host, port, username, password)
	return &SMTPClient{
		Client: client,
		ctx:    ctx,
	}, nil
}

// Close closes the underlying connection
func (c *SMTPClient) Close() error {
	// no need to close the connection as it's already closed when sending
	return nil
}

// SendMail sends an email
func (c *SMTPClient) SendMail(from string, to, cc, bcc []string, subject string, msg string, readers map[string]io.ReadSeeker) error {
	m := mail.NewMsg()
	m.From(from)
	m.To(to...)
	if len(cc) > 0 {
		m.Cc(cc...)
	}
	if len(bcc) > 0 {
		m.Bcc(bcc...)
	}
	m.Subject(subject)
	m.SetBodyString(mail.TypeTextHTML, msg)
	// attach file from readers
	for attachment, reader := range readers {
		m.AttachReadSeeker(attachment, reader)
	}

	return errors.WithStack(c.DialAndSendWithContext(c.ctx, m))
}
