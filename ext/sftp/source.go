package sftp

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/component/source"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
)

type SFTPSource struct {
	*source.CommonSource
	client          *sftp.Client
	path            string
	filenamePattern string
	batchSize       int
	ctx             context.Context
}

type SFTPCredentials struct {
	Address                                         string
	Username, Password, PrivateKey, HostFingerprint string
}

var _ flow.Source = (*SFTPSource)(nil)

func NewSource(ctx context.Context, l *slog.Logger,
	creds SFTPCredentials,
	path string,
	groupBatchSize int,
	filenamePattern string,
	opts ...option.Option) (*SFTPSource, error) {
	// create common
	commonSource := source.NewCommonSource(l, opts...)

	// set up SFTP client
	client, err := newClient(
		creds.Address,
		creds.Username,
		creds.Password,
		creds.PrivateKey,
		creds.HostFingerprint)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	s := &SFTPSource{
		CommonSource:    commonSource,
		client:          client,
		path:            path,
		filenamePattern: filenamePattern,
		batchSize:       groupBatchSize,
		ctx:             ctx,
	}

	// add clean func
	commonSource.AddCleanFunc(func() {
		commonSource.Logger.Debug("source(sftp): close func called")
		_ = client.Close()
		commonSource.Logger.Info("source(sftp): client closed")
	})

	// register process, it will immediately start the process
	// in a separate goroutine
	commonSource.RegisterProcess(s.process)

	return s, nil
}

func (s *SFTPSource) process() {
	files, err := s.client.ReadDir(s.path)
	if err != nil {
		s.Logger.Error(fmt.Sprintf("source(sftp): failed to read directory %s: %s", s.path, err.Error()))
		s.SetError(errors.WithStack(err))
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		matched, _ := filepath.Match(s.filenamePattern, file.Name())
		if !matched {
			continue
		}

		s.Logger.Debug(fmt.Sprintf("source(sftp): reading path %s", filepath.Join(s.path, file.Name())))
	}
}
