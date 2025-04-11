package sftp

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"text/template"

	"github.com/goccy/go-json"

	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
)

// SFTPSink is a sink that writes data to a SFTP server.
type SFTPSink struct {
	common.Sink

	client                 *sftp.Client
	destinationURITemplate *template.Template
	fileHandlers           map[string]io.WriteCloser
}

var _ flow.Sink = (*SFTPSink)(nil)

// NewSink creates a new SFTPSink.
func NewSink(commonSink common.Sink,
	privateKey, hostFingerprint string,
	destinationURI string,
	opts ...common.Option) (*SFTPSink, error) {

	// set up SFTP client
	urlParsed, err := url.Parse(destinationURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if urlParsed.Scheme != "sftp" {
		return nil, fmt.Errorf("invalid scheme: %s", urlParsed.Scheme)
	}
	address := urlParsed.Host
	username := urlParsed.User.Username()
	password, _ := urlParsed.User.Password()
	client, err := newClient(address, username, password, privateKey, hostFingerprint)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	u := url.URL{Scheme: urlParsed.Scheme, Path: urlParsed.Path}
	t, err := compiler.NewTemplate("sink_sftp_destination_uri", u.String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}

	s := &SFTPSink{
		Sink:                   commonSink,
		client:                 client,
		destinationURITemplate: t,
		fileHandlers:           map[string]io.WriteCloser{},
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s.Logger().Info(fmt.Sprintf("close client"))
		return s.client.Close()
	})
	commonSink.AddCleanFunc(func() error {
		for _, fh := range s.fileHandlers {
			fh.Close()
		}
		s.Logger().Info("file handlers closed")
		return nil
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(s.process)

	return s, nil
}

func (s *SFTPSink) process() error {
	for record, err := range s.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		destinationURI, err := compiler.Compile(s.destinationURITemplate, model.ToMap(record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("failed to compile destination URI"))
			return errors.WithStack(err)
		}
		s.Logger().Debug(fmt.Sprintf("destination URI: %s", destinationURI))
		fh, ok := s.fileHandlers[destinationURI]
		if !ok {
			targetURI, err := url.Parse(destinationURI)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to parse destination URI"))
				return errors.WithStack(err)
			}
			if targetURI.Scheme != "sftp" {
				s.Logger().Error(fmt.Sprintf("invalid scheme"))
				return fmt.Errorf("invalid scheme: %s", targetURI.Scheme)
			}
			fh, err = s.client.OpenFile(targetURI.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to create file handler: %s", err.Error()))
				return errors.WithStack(err)
			}
			s.fileHandlers[destinationURI] = fh
		}
		raw, err := json.Marshal(record)
		if err != nil {
			s.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}
		if err := s.Retry(func() error {
			_, err := fh.Write(append(raw, '\n'))
			return err
		}); err != nil {
			s.Logger().Error(fmt.Sprintf("failed to write data"))
			return errors.WithStack(err)
		}
	}
	return nil
}
