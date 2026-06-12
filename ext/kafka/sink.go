package kafka

import (
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"text/template"

	"github.com/goccy/go-json"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/ext/compiler"
	"github.com/goto/optimus-any2any/internal/ext/model"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaSink is a sink for Kafka
type KafkaSink struct {
	common.Sink

	client          *kgo.Client
	keyTemplate     *template.Template
	valueSerializer string
	keySourceField  string
	valueSourceField string
}

func NewSink(commonSink common.Sink,
	bootstrapServers []string, topic string,
	key string, valueSerializer, keySourceField, valueSourceField string,
	opts ...common.Option) (*KafkaSink, error) {

	// create kafka client
	client, err := kgo.NewClient(kgo.SeedBrokers(bootstrapServers...), kgo.DefaultProduceTopic(topic), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	k := &KafkaSink{
		Sink:             commonSink,
		client:           client,
		valueSerializer:  valueSerializer,
		keySourceField:   keySourceField,
		valueSourceField: valueSourceField,
	}

	// compile key template if provided
	if key != "" {
		tmpl, err := compiler.NewTemplate("sink_kafka_key", key)
		if err != nil {
			return nil, errors.Wrap(err, "failed to compile kafka key template")
		}
		k.keyTemplate = tmpl
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		k.Logger().Info("close client")
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
	k.Logger().Info("start reading from source")
	for record, err := range k.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}

		var keyBytes []byte
		var valueBytes []byte

		switch k.valueSerializer {
		case "proto_b64":
			// extract base64-encoded value from record
			val, ok := record.Get(k.valueSourceField)
			if !ok {
				k.Logger().Error(fmt.Sprintf("record missing field %q for proto_b64 serialization", k.valueSourceField))
				return errors.Errorf("record missing field %q for proto_b64 serialization", k.valueSourceField)
			}
			valStr, ok := val.(string)
			if !ok {
				k.Logger().Error(fmt.Sprintf("field %q is not a string", k.valueSourceField))
				return errors.Errorf("field %q is not a string for proto_b64 serialization", k.valueSourceField)
			}
			decoded, err := base64.StdEncoding.DecodeString(valStr)
			if err != nil {
				return errors.Wrap(err, "failed to decode base64 proto value")
			}
			valueBytes = decoded

			// extract base64-encoded key from record
			keyVal, ok := record.Get(k.keySourceField)
			if ok {
				keyStr, ok := keyVal.(string)
				if ok {
					decodedKey, err := base64.StdEncoding.DecodeString(keyStr)
					if err != nil {
						return errors.Wrap(err, "failed to decode base64 proto key")
					}
					keyBytes = decodedKey
				}
			}
		default:
			// default: JSON serialization
			recordWithoutMetadata := k.RecordWithoutMetadata(record)
			raw, err := json.MarshalWithOption(recordWithoutMetadata, json.DisableHTMLEscape())
			if err != nil {
				k.Logger().Error("failed to marshal record")
				return errors.WithStack(err)
			}
			valueBytes = raw

			// compile key from template if configured
			if k.keyTemplate != nil {
				recordMap := model.ToMap(recordWithoutMetadata)
				keyStr, err := compiler.Compile(k.keyTemplate, recordMap)
				if err != nil {
					k.Logger().Error("failed to compile kafka key template")
					return errors.WithStack(err)
				}
				keyBytes = []byte(keyStr)
			}
		}

		// send a record
		var e error
		wg.Add(1)
		k.client.Produce(k.Context(), &kgo.Record{Key: keyBytes, Value: valueBytes}, func(r *kgo.Record, err error) {
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
