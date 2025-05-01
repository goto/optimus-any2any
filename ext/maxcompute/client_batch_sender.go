package maxcompute

import (
	errs "errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

const (
	// ref: https://www.alibabacloud.com/help/en/maxcompute/user-guide/faq-about-tunnel-commands
	maxBatchSizeInMB = 100 * (1 << 10) // max batch size in MB
	minBatchSizeInMB = 1 << 6          // min batch size in MB
	maxBlockId       = 20000           // max block id
)

type recordWriterPool struct {
	l                *slog.Logger
	session          *tunnel.UploadSession
	counter          atomic.Uint32
	batchSizeInBytes int64
	mutexes          []sync.Mutex
	recordWriters    []*tunnel.RecordProtocWriter
	blockIds         []int
}

func newRecordWriterPool(l *slog.Logger, session *tunnel.UploadSession, batchSizeInBytes int64, concurrency int) (*recordWriterPool, error) {
	wp := &recordWriterPool{
		l:                l,
		session:          session,
		batchSizeInBytes: batchSizeInBytes,
		mutexes:          make([]sync.Mutex, concurrency),
		recordWriters:    make([]*tunnel.RecordProtocWriter, concurrency),
		blockIds:         make([]int, concurrency),
	}
	// initialize values
	for i := range concurrency {
		recordWriter, err := wp.session.OpenRecordWriter(i)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		wp.recordWriters[i] = recordWriter
		wp.blockIds[i] = i
	}
	return wp, nil
}

func (p *recordWriterPool) send(record *model.Record) error {
	// convert record to odps record
	mcRecord, err := createRecord(p.l, record, p.session.Schema())
	if err != nil {
		return errors.WithStack(err)
	}

	// select record writer based on round robin fashion
	i := p.counter.Add(1) % uint32(len(p.recordWriters))
	p.mutexes[i].Lock()
	defer p.mutexes[i].Unlock()

	// create new recordWriter if bytes count hits batch size limit
	if p.recordWriters[i].BytesCount() >= p.batchSizeInBytes {
		// close the current record writer
		if err := p.recordWriters[i].Close(); err != nil {
			return errors.WithStack(err)
		}
		// and create a new one
		rw, err := p.newRecordWriter()
		if err != nil {
			return errors.WithStack(err)
		}
		p.recordWriters[i] = rw
	}

	if err := p.recordWriters[i].Write(mcRecord); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (p *recordWriterPool) newRecordWriter() (*tunnel.RecordProtocWriter, error) {
	// create new record writer
	blockId := len(p.blockIds)
	if blockId >= maxBlockId {
		return nil, fmt.Errorf("max block id %d reached", maxBlockId)
	}
	rw, err := p.session.OpenRecordWriter(blockId)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	p.l.Info(fmt.Sprintf("created new record writer with blockId %d", blockId))

	// update record writer pool
	p.blockIds = append(p.blockIds, blockId)

	return rw, nil
}

func (p *recordWriterPool) close() error {
	var e error
	// close remaining record writers
	for _, rw := range p.recordWriters {
		if err := rw.Close(); err != nil {
			e = errs.Join(e, err)
		}
	}
	return errors.WithStack(e)
}

func (p *recordWriterPool) commit() error {
	blockIds := []string{}
	for _, blockId := range p.blockIds {
		blockIds = append(blockIds, fmt.Sprintf("%d", blockId))
	}
	p.l.Info(fmt.Sprintf("commit %d blocks: %s", len(p.blockIds), strings.Join(blockIds, ",")))
	if err := p.session.Commit(p.blockIds); err != nil {
		return errors.WithStack(err)
	}
	p.l.Info(fmt.Sprintf("committed %d blocks", len(p.blockIds)))
	return nil
}

type mcBatchRecordSender struct {
	l    *slog.Logger
	wp   *recordWriterPool
	sem  chan uint8 // semaphore for concurrency control
	wg   sync.WaitGroup
	err  error
	errM sync.Mutex
}

var _ common.RecordSender = (*mcBatchRecordSender)(nil)

func newBatchRecordSender(l *slog.Logger, session *tunnel.UploadSession, batchSizeInMB int, concurrency int) (*mcBatchRecordSender, error) {
	if batchSizeInMB < minBatchSizeInMB || batchSizeInMB > maxBatchSizeInMB { // should be in range 64MB to 100GB
		l.Warn(fmt.Sprintf("batch size %dMB is not in range 64MB to 100GB, using default value 64MB", batchSizeInMB))
		batchSizeInMB = 64
	}
	batchSizeInBytes := int64(batchSizeInMB * (1 << 20))

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
	s.wg.Add(1)
	go func(recordToBeProcessed *model.Record) {
		defer func() {
			<-s.sem // release semaphore lock
			s.wg.Done()
		}()
		if err := s.wp.send(recordToBeProcessed); err != nil {
			s.errM.Lock()
			s.err = errors.WithStack(err)
			s.errM.Unlock()
		}
	}(record.Copy())

	return s.err
}

func (s *mcBatchRecordSender) Close() error {
	s.wg.Wait() // wait for all goroutines to finish
	if err := s.wp.close(); err != nil {
		s.errM.Lock()
		s.err = errors.WithStack(err)
		s.errM.Unlock()
		return s.err
	}
	return s.commit()
}

func (s *mcBatchRecordSender) commit() error {
	if s.err != nil {
		// no commit if error exist
		return s.err
	}
	return s.wp.commit()
}
