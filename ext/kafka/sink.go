package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/ext/common/model"
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

func NewSink(ctx context.Context, l *slog.Logger, metadataPrefix string,
	bootstrapServers []string, topic string,
	opts ...common.Option) (*KafkaSink, error) {
	// create common
	commonSink := common.NewSink(l, metadataPrefix, opts...)
	commonSink.SetName("sink(kafka)")

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
		commonSink.Logger.Info(fmt.Sprintf("%s: close client", ks.Name()))
		client.Close()
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(ks.process)

	return ks, nil
}

func (ks *KafkaSink) process() error {
	var (
		wg    sync.WaitGroup
		count atomic.Int32
	)
	// read from channel
	ks.Logger.Info(fmt.Sprintf("%s: start reading from source", ks.Name()))
	for v := range ks.Read() {
		raw, ok := v.([]byte)
		if !ok {
			ks.Logger.Error(fmt.Sprintf("%s: invalid data format", ks.Name()))
			return fmt.Errorf("invalid data format")
		}

		var record model.Record
		if err := json.Unmarshal(raw, &record); err != nil {
			ks.Logger.Error(fmt.Sprintf("%s: invalid data format", ks.Name()))
			return errors.WithStack(err)
		}
		recordWithoutMetadata := extcommon.RecordWithoutMetadata(record, ks.MetadataPrefix)
		raw, err := json.Marshal(recordWithoutMetadata)
		if err != nil {
			ks.Logger.Error(fmt.Sprintf("%s: failed to marshal record", ks.Name()))
			return errors.WithStack(err)
		}

		// send a record
		var e error
		wg.Add(1)
		ks.client.Produce(ks.ctx, &kgo.Record{Value: raw}, func(r *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				e = err
				return
			}
			count.Add(1)
			if count.Load()%200 == 0 {
				ks.Logger.Info(fmt.Sprintf("%s: record sent: %d", ks.Name(), count.Load()))
			}
		})
		if e != nil {
			ks.Logger.Error(fmt.Sprintf("%s: error: %s", ks.Name(), e.Error()))
			return errors.WithStack(e)
		}
	}

	wg.Wait()
	if count.Load()%200 != 0 {
		ks.Logger.Info(fmt.Sprintf("%s: record sent: %d", ks.Name(), count.Load()))
	}

	return nil
}
