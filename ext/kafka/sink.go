package kafka

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/goccy/go-json"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaSink is a sink for Kafka
type KafkaSink struct {
	common.Sink

	client *kgo.Client
}

func NewSink(commonSink common.Sink,
	bootstrapServers []string, topic string,
	opts ...common.Option) (*KafkaSink, error) {

	// create kafka client
	client, err := kgo.NewClient(kgo.SeedBrokers(bootstrapServers...), kgo.DefaultProduceTopic(topic), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	k := &KafkaSink{
		Sink:   commonSink,
		client: client,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		k.Logger().Info(fmt.Sprintf("close client"))
		client.Close()
		return nil
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(k.process)

	return k, nil
}

func (k *KafkaSink) process() error {
	var (
		wg    sync.WaitGroup
		count atomic.Int32
	)
	// read from channel
	k.Logger().Info(fmt.Sprintf("start reading from source"))
	for record, err := range k.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		recordWithoutMetadata := k.RecordWithoutMetadata(record)
		raw, err := json.MarshalWithOption(recordWithoutMetadata, json.DisableHTMLEscape())
		if err != nil {
			k.Logger().Error(fmt.Sprintf("failed to marshal record"))
			return errors.WithStack(err)
		}

		// send a record
		var e error
		wg.Add(1)
		k.client.Produce(k.Context(), &kgo.Record{Value: raw}, func(r *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				e = err
				return
			}
			count.Add(1)
			if count.Load()%200 == 0 {
				k.Logger().Info(fmt.Sprintf("record sent: %d", count.Load()))
			}
		})
		if e != nil {
			k.Logger().Error(fmt.Sprintf("error: %s", e.Error()))
			return errors.WithStack(e)
		}
	}

	wg.Wait()
	if count.Load()%200 != 0 {
		k.Logger().Info(fmt.Sprintf("record sent: %d", count.Load()))
	}

	return nil
}
