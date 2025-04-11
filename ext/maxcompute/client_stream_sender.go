package maxcompute

import (
	"fmt"
	"log/slog"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

type mcStreamRecordSender struct {
	l                *slog.Logger
	session          *tunnel.StreamUploadSession
	packWriter       *tunnel.RecordPackStreamWriter
	batchSizeInBytes int64
	recordCounter    int
	err              error
	retryFunc        func(func() error) error
}

var _ common.RecordSender = (*mcStreamRecordSender)(nil)

func newStreamRecordSender(l *slog.Logger, session *tunnel.StreamUploadSession, batchSizeInMB int) (*mcStreamRecordSender, error) {
	s := &mcStreamRecordSender{
		l:                l,
		session:          session,
		packWriter:       session.OpenRecordPackWriter(),
		batchSizeInBytes: int64(batchSizeInMB) * (1 << 20), // MB to bytes
		recordCounter:    0,
		err:              nil,
		retryFunc: func(f func() error) error {
			return f()
		},
	}
	return s, nil
}

func (s *mcStreamRecordSender) SendRecord(record *model.Record) error {
	if record == nil {
		return nil
	}
	// convert record to odps record
	mcRecord, err := createRecord(s.l, record, *s.session.Schema())
	if err != nil {
		return s.errStack(err)
	}
	if err := s.packWriter.Append(mcRecord); err != nil {
		return s.errStack(err)
	}
	s.recordCounter++
	if s.packWriter.DataSize() > s.batchSizeInBytes {
		if err := s.flush(); err != nil {
			return s.errStack(err)
		}
	}
	return nil
}

func (s *mcStreamRecordSender) Close() error {
	if s.err != nil {
		return s.err
	}
	return s.flush()
}

func (s *mcStreamRecordSender) errStack(err error) error {
	s.err = err
	return errors.WithStack(err)
}

func (s *mcStreamRecordSender) flush() error {
	// retryable flush
	if err := s.retryFunc(func() error {
		traceId, recordCount, bytesSend, err := s.packWriter.Flush()
		s.l.Debug(fmt.Sprintf("flush trace id: %s, record count: %d, bytes send: %d", traceId, recordCount, bytesSend))
		return err
	}); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
