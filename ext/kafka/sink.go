package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaSink is a sink for Kafka
type KafkaSink struct {
	*common.Sink

	ctx    context.Context
	client *kgo.Client
}

func NewSink(ctx context.Context, l *slog.Logger,
	bootstrapServers []string, topic string,
	opts ...common.Option) (*KafkaSink, error) {
	// create common
	commonSink := common.NewSink(l, opts...)

	// create kafka client
	client, err := kgo.NewClient(kgo.SeedBrokers(bootstrapServers...), kgo.DefaultProduceTopic(topic), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ks := &KafkaSink{
		Sink:   commonSink,
		ctx:    ctx,
		client: client,
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		commonSink.Logger.Info("sink(kafka): close client")
		client.Close()
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(ks.process)

	return ks, nil
}

func (ks *KafkaSink) process() {
	var (
		wg    sync.WaitGroup
		count atomic.Int32
	)
	// read from channel
	ks.Logger.Info("sink(kafka): start reading from source")
	for v := range ks.Read() {
		ks.Logger.Debug("sink(kafka): read from source")
		if ks.Err() != nil { // skip if error
			continue
		}
		// convert to byte
		raw, ok := v.([]byte)
		if !ok {
			err := fmt.Errorf("invalid data type: %T", v)
			ks.Logger.Error(fmt.Sprintf("sink(kafka): error: %s", err.Error()))
			ks.SetError(err)
			continue
		}
		// send a record
		ks.Logger.Debug(fmt.Sprintf("sink(kafka): send record: %s", string(raw)))
		wg.Add(1)
		ks.client.Produce(ks.ctx, &kgo.Record{Value: raw}, func(r *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				ks.Logger.Error(fmt.Sprintf("sink(kafka): error: %s", err.Error()))
				ks.SetError(err)
				return
			}
			count.Add(1)
			if count.Load()%200 == 0 {
				ks.Logger.Info(fmt.Sprintf("sink(kafka): record sent: %d", count.Load()))
			}
			ks.Logger.Debug("sink(kafka): record sent")
		})
		ks.Logger.Debug("sink(kafka): record sent submitted asynchronously")
	}
	wg.Wait()
	if count.Load()%200 != 0 {
		ks.Logger.Info(fmt.Sprintf("sink(kafka): record sent: %d", count.Load()))
	}
}
