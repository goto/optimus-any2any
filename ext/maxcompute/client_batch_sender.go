package maxcompute

import (
	errs "errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

type recordWriterPool struct {
	l                *slog.Logger
	session          *tunnel.UploadSession
	schema           tableschema.TableSchema
	counter          atomic.Uint32
	batchSizeInBytes int64
	m                []sync.RWMutex
	recordWriters    []*tunnel.RecordProtocWriter
	closers          []io.Closer
	blockIds         []int
}

func newRecordWriterPool(l *slog.Logger, session *tunnel.UploadSession, batchSizeInBytes int64, concurrency int) (*recordWriterPool, error) {
	wp := &recordWriterPool{
		l:                l,
		session:          session,
		schema:           session.Schema(),
		batchSizeInBytes: batchSizeInBytes,
		m:                make([]sync.RWMutex, concurrency),
		recordWriters:    make([]*tunnel.RecordProtocWriter, concurrency),
		closers:          make([]io.Closer, concurrency),
		blockIds:         make([]int, concurrency),
	}
	// initialize values
	for i := range concurrency {
		recordWriter, err := wp.session.OpenRecordWriter(i)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		wp.recordWriters[i] = recordWriter
		wp.closers[i] = recordWriter
		wp.blockIds[i] = i
	}
	return wp, nil
}

func (p *recordWriterPool) send(record *model.Record) error {
	// convert record to odps record
	mcRecord, err := createRecord(p.l, record, p.schema)
	if err != nil {
		return errors.WithStack(err)
	}

	// select record writer based on round robin fashion
	i := p.counter.Add(1) % uint32(len(p.recordWriters))
	p.m[i].Lock()
	defer p.m[i].Unlock()
	recordWriter := p.recordWriters[i]

	// create new recordWriter if bytes count hits batch size limit
	if recordWriter.BytesCount() >= p.batchSizeInBytes {
		rw, err := p.newRecordWriter(i)
		if err != nil {
			return errors.WithStack(err)
		}
		recordWriter = rw
	}

	if err := recordWriter.Write(mcRecord); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (p *recordWriterPool) newRecordWriter(i uint32) (*tunnel.RecordProtocWriter, error) {
	p.l.Info(fmt.Sprintf("create new record writer %d", i))

	blockId := len(p.blockIds)
	rw, err := p.session.OpenRecordWriter(blockId)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	p.recordWriters[i] = rw
	p.blockIds = append(p.blockIds, blockId)
	p.closers = append(p.closers, rw)

	p.l.Info(fmt.Sprintf("done create new record writer %d", i))
	return rw, nil
}

func (p *recordWriterPool) close() error {
	var e error
	for _, closer := range p.closers {
		if err := closer.Close(); err != nil && !strings.Contains(err.Error(), "try to close a closed RecordProtocWriter") {
			e = errs.Join(e, err)
		}
	}
	return e
}

func (p *recordWriterPool) commit() error {
	blockIds := []string{}
	for _, blockId := range p.blockIds {
		blockIds = append(blockIds, fmt.Sprintf("%d", blockId))
	}
	p.l.Info(fmt.Sprintf("commit %d blocks: %s", len(p.blockIds), strings.Join(blockIds, ",")))
	return p.session.Commit(p.blockIds)
}

type mcBatchRecordSender struct {
	l    *slog.Logger
	wp   *recordWriterPool
	sem  chan uint8 // semaphore for concurrency control
	err  error
	errM sync.Mutex
}

var _ common.RecordSender = (*mcBatchRecordSender)(nil)

func newBatchRecordSender(l *slog.Logger, session *tunnel.UploadSession, concurrency int) (*mcBatchRecordSender, error) {
	// ref: https://www.alibabacloud.com/help/en/maxcompute/user-guide/faq-about-tunnel-commands
	batchSizeInBytes := int64(64 * (1 << 20)) // 64MB per block, recommendation 64MB - 100GB
	wp, err := newRecordWriterPool(l, session, batchSizeInBytes, concurrency)
	if err != nil {
		return nil, err
	}
	s := &mcBatchRecordSender{
		l:   l,
		wp:  wp,
		sem: make(chan uint8, concurrency),
	}
	return s, nil
}

func (s *mcBatchRecordSender) SendRecord(record *model.Record) error {
	if record == nil {
		return nil
	}
	if s.err != nil {
		return errors.WithStack(s.err)
	}

	s.sem <- 0 // acquire semaphore lock
	recordToBeProcessed := record.Copy()
	go func(recordToBeProcessed *model.Record) {
		defer func() {
			<-s.sem // release semaphore lock
		}()
		if err := s.wp.send(recordToBeProcessed); err != nil {
			s.errM.Lock()
			s.err = errors.WithStack(err)
			s.errM.Unlock()
		}
	}(recordToBeProcessed)

	return nil
}

func (s *mcBatchRecordSender) Close() error {
	if err := s.wp.close(); err != nil {
		s.errM.Lock()
		s.err = errors.WithStack(err)
		s.errM.Unlock()
		return err
	}
	return s.Commit()
}

func (s *mcBatchRecordSender) Commit() error {
	if s.err != nil {
		// no commit if error exist
		return s.err
	}
	return s.wp.commit()
}
