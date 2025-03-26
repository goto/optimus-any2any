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

	// set up SFTP client
	urlParsed, err := url.Parse(destinationURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if urlParsed.Scheme != "sftp" {
		return nil, errors.New("sink(sftp): invalid scheme")
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
		return nil, fmt.Errorf("sink(sftp): failed to parse destination URI template: %w", err)
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
		commonSink.Logger.Debug("sink(sftp): close func called")
		_ = s.client.Close()
		for _, fh := range s.fileHandlers {
			_ = fh.Close()
		}
		commonSink.Logger.Info("sink(sftp): client closed")
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(s.process)

	return s, nil
}

func (s *SFTPSink) process() {
	for msg := range s.Read() {
		if s.Err() != nil {
			continue
		}
		b, ok := msg.([]byte)
		if !ok {
			s.Logger.Error(fmt.Sprintf("sink(sftp): message type assertion error: %T", msg))
			s.SetError(errors.New(fmt.Sprintf("sink(sftp): message type assertion error: %T", msg)))
			continue
		}
		s.Logger.Debug(fmt.Sprintf("sink(sftp): receive message: %s", string(b)))

		var record map[string]interface{}
		if err := json.Unmarshal(b, &record); err != nil {
			s.Logger.Error("sink(sftp): invalid data format")
			s.SetError(errors.WithStack(err))
			continue
		}
		destinationURI, err := extcommon.Compile(s.destinationURITemplate, record)
		if err != nil {
			s.Logger.Error("sink(sftp): failed to compile destination URI")
			s.SetError(errors.WithStack(err))
			continue
		}
		s.Logger.Debug(fmt.Sprintf("sink(sftp): destination URI: %s", destinationURI))
		fh, ok := s.fileHandlers[destinationURI]
		if !ok {
			targetURI, err := url.Parse(destinationURI)
			if err != nil {
				s.Logger.Error("sink(sftp): failed to parse destination URI")
				s.SetError(errors.WithStack(err))
				continue
			}
			if targetURI.Scheme != "sftp" {
				s.Logger.Error("sink(sftp): invalid scheme")
				s.SetError(errors.New("sink(sftp): invalid scheme"))
				continue
			}
			fh, err = s.client.OpenFile(targetURI.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND)
			if err != nil {
				s.Logger.Error(fmt.Sprintf("sink(sftp): failed to create file handler: %s", err.Error()))
				s.SetError(errors.WithStack(err))
				continue
			}
			s.fileHandlers[destinationURI] = fh
		}
		if _, err := fh.Write(append(b, '\n')); err != nil {
			s.Logger.Error("sink(sftp): failed to write data")
			s.SetError(errors.WithStack(err))
			continue
		}
	}
}

func (s *SFTPSink) writeFn(fh extcommon.FileHandler, b []byte) func() error {
	return func() error {
		written, err := fh.Write(append(b, '\n'))
		if err != nil {
			return err
		}

		s.Logger.Debug(fmt.Sprintf("sink(sftp): wrote %d bytes to sftp", written))
		return nil
	}
}
