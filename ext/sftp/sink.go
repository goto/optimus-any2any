package sftp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"text/template"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/ext/common/model"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
)

// SFTPSink is a sink that writes data to a SFTP server.
type SFTPSink struct {
	*common.Sink
	ctx context.Context

	client                 *sftp.Client
	destinationURITemplate *template.Template
	fileHandlers           map[string]extcommon.FileHandler
}

var _ flow.Sink = (*SFTPSink)(nil)

// NewSink creates a new SFTPSink.
func NewSink(ctx context.Context, l *slog.Logger, metadataPrefix string,
	privateKey, hostFingerprint string,
	destinationURI string,
	opts ...common.Option) (*SFTPSink, error) {
	// create common
	commonSink := common.NewSink(l, metadataPrefix, opts...)
	commonSink.SetName("sink(sftp)")

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
	t, err := extcommon.NewTemplate("sink_sftp_destination_uri", u.String())
	if err != nil {
		return nil, fmt.Errorf("%s: failed to parse destination URI template: %w", commonSink.Name(), err)
	}

	s := &SFTPSink{
		Sink:                   commonSink,
		ctx:                    ctx,
		client:                 client,
		destinationURITemplate: t,
		fileHandlers:           map[string]extcommon.FileHandler{},
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		_ = s.client.Close()
		for _, fh := range s.fileHandlers {
			_ = fh.Close()
		}
		commonSink.Logger.Info(fmt.Sprintf("%s: client closed", s.Name()))
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(s.process)

	return s, nil
}

func (s *SFTPSink) process() error {
	for msg := range s.Read() {
		b, ok := msg.([]byte)
		if !ok {
			s.Logger.Error(fmt.Sprintf("%s: message type assertion error: %T", s.Name(), msg))
			return fmt.Errorf("message type assertion error: %T", msg)
		}
		s.Logger.Debug(fmt.Sprintf("%s: receive message: %s", s.Name(), string(b)))

		var record model.Record
		if err := json.Unmarshal(b, &record); err != nil {
			s.Logger.Error(fmt.Sprintf("%s: invalid data format", s.Name()))
			return errors.WithStack(err)
		}
		destinationURI, err := extcommon.Compile(s.destinationURITemplate, model.ToMap(record))
		if err != nil {
			s.Logger.Error(fmt.Sprintf("%s: failed to compile destination URI", s.Name()))
			return errors.WithStack(err)
		}
		s.Logger.Debug(fmt.Sprintf("%s: destination URI: %s", s.Name(), destinationURI))
		fh, ok := s.fileHandlers[destinationURI]
		if !ok {
			targetURI, err := url.Parse(destinationURI)
			if err != nil {
				s.Logger.Error(fmt.Sprintf("%s: failed to parse destination URI", s.Name()))
				return errors.WithStack(err)
			}
			if targetURI.Scheme != "sftp" {
				s.Logger.Error(fmt.Sprintf("%s: invalid scheme", s.Name()))
				return fmt.Errorf("invalid scheme: %s", targetURI.Scheme)
			}
			fh, err = s.client.OpenFile(targetURI.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND)
			if err != nil {
				s.Logger.Error(fmt.Sprintf("%s: failed to create file handler: %s", s.Name(), err.Error()))
				return errors.WithStack(err)
			}
			s.fileHandlers[destinationURI] = fh
		}
		if err := s.Retry(func() error {
			_, err := fh.Write(append(b, '\n'))
			return err
		}); err != nil {
			s.Logger.Error(fmt.Sprintf("%s: failed to write data", s.Name()))
			return errors.WithStack(err)
		}
	}
	return nil
}
