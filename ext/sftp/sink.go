package sftp

import (
	"fmt"
	"net/url"
	"text/template"

	"github.com/goccy/go-json"

	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/fs"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/internal/model"
	xnet "github.com/goto/optimus-any2any/internal/net"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// SFTPSink is a sink that writes data to a SFTP server.
type SFTPSink struct {
	common.Sink

	// client                 *sftp.Client
	destinationURITemplate *template.Template
	handlers               fs.WriteHandler

	// json path selector
	jsonPathSelector string
}

var _ flow.Sink = (*SFTPSink)(nil)

// NewSink creates a new SFTPSink.
func NewSink(commonSink common.Sink,
	privateKey, hostFingerprint string,
	destinationURI string,
	compressionType string, compressionPassword string,
	jsonPathSelector string,
	opts ...common.Option) (*SFTPSink, error) {

	// prepare handlers
	u, err := url.Parse(destinationURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// create a new SFTP handler
	password, _ := u.User.Password()
	handlers, err := NewSFTPHandler(commonSink.Context(), commonSink.Logger(),
		u.Host, u.User.Username(), password,
		privateKey, hostFingerprint,
		true, // TODO: make this configurable
		fs.WithWriteCompression(compressionType),
		fs.WithWriteCompressionPassword(compressionPassword),
		fs.WithWriteChunkOptions(xio.WithCSVSkipHeader(false)), // TODO: make this configurable
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	t, err := compiler.NewTemplate("sink_sftp_destination_uri", destinationURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}

	s := &SFTPSink{
		Sink:                   commonSink,
		destinationURITemplate: t,
		handlers:               handlers,
		// jsonPath selector
		jsonPathSelector: jsonPathSelector,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s.handlers.Close()
		s.Logger().Info("file handlers closed")
		return nil
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(s.process)

	return s, nil
}

func (s *SFTPSink) process() error {
	recordCounter := 0
	for record, err := range s.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		if s.IsSpecializedMetadataRecord(record) {
			s.Logger().Debug("skip specialized metadata record")
			continue
		}

		destinationURI, err := compiler.Compile(s.destinationURITemplate, model.ToMap(record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("failed to compile destination URI"))
			return errors.WithStack(err)
		}

		// record without metadata
		recordWithoutMetadata := s.RecordWithoutMetadata(record)
		raw, err := json.MarshalWithOption(recordWithoutMetadata, json.DisableHTMLEscape())
		if err != nil {
			s.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}
		// if jsonPathSelector is provided, select the data using it
		if s.jsonPathSelector != "" {
			raw, err = s.JSONPathSelector(raw, s.jsonPathSelector)
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to select data using json path selector"))
				return errors.WithStack(err)
			}
		}

		err = s.DryRunable(func() error {
			if err := s.handlers.Write(destinationURI, append(raw, '\n')); err != nil {
				s.Logger().Error("failed to write to file")
				return errors.WithStack(err)
			}
			recordCounter++
			return nil
		}, func() error {
			// in dry run mode, we don't need to send the request
			// we just need to check the endpoint connectivity
			targetURI, _ := url.Parse(destinationURI)
			return xnet.ConnCheck(targetURI.Host)
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	if recordCounter == 0 {
		s.Logger().Info(fmt.Sprintf("no records to write"))
		return nil
	}

	// flush all write handlers
	if err := s.DryRunable(s.handlers.Sync); err != nil {
		return errors.WithStack(err)
	}

	_ = s.DryRunable(func() error { // ignore log when dry run
		s.Logger().Info(fmt.Sprintf("successfully written %d records", recordCounter))
		return nil
	})
	return nil
}
