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
	l             *slog.Logger
	session       *tunnel.StreamUploadSession
	packWriter    *tunnel.RecordPackStreamWriter
	recordCounter int
	err           error
}

var _ common.RecordSender = (*mcStreamRecordSender)(nil)

func newStreamRecordSender(l *slog.Logger, session *tunnel.StreamUploadSession) (*mcStreamRecordSender, error) {
	s := &mcStreamRecordSender{
		l:             l,
		session:       session,
		packWriter:    session.OpenRecordPackWriter(),
		recordCounter: 0,
		err:           nil,
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
	if s.packWriter.DataSize() > 1<<19 { // flush every ~512KB
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
	s.l.Info(fmt.Sprintf("write %d records", s.recordCounter))
	traceId, recordCount, bytesSend, err := s.packWriter.Flush()
	if err != nil {
		return errors.WithStack(err)
	}
	s.l.Debug(fmt.Sprintf("flush trace id: %s, record count: %d, bytes send: %d", traceId, recordCount, bytesSend))
	return err
}
