package redis

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"text/template"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/ext/common/model"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

type RedisSink struct {
	*common.Sink
	ctx    context.Context
	client redis.Cmdable

	recordKeyTemplate   *template.Template
	recordValueTemplate *template.Template
	records             []interface{}
}

var _ flow.Sink = (*RedisSink)(nil)

// NewSink creates a new RedisSink
func NewSink(ctx context.Context, l *slog.Logger, metadataPrefix string,
	connectionDSN string, connectionTLSCert, connectionTLSCACert, connectionTLSKey string,
	recordKey, recordValue string, batchSize int, opts ...common.Option) (*RedisSink, error) {

	// create common sink
	commonSink := common.NewSink(l, metadataPrefix, opts...)
	commonSink.SetName("sink(redis)")

	// parse connectionDSN
	l.Debug(fmt.Sprintf("%s: connection DSN: %s", commonSink.Name(), connectionDSN))
	parsedConnection, err := url.Parse(connectionDSN)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if parsedConnection.Scheme != "redis" && parsedConnection.Scheme != "rediss" {
		return nil, fmt.Errorf("invalid connection DSN scheme: %s", parsedConnection.Scheme)
	}
	var tlsConfig *tls.Config
	if parsedConnection.Scheme == "rediss" {
		if connectionTLSCert == "" || connectionTLSKey == "" || connectionTLSCACert == "" {
			return nil, fmt.Errorf("missing TLS certificate, key or CA certificate")
		}
		c, err := extcommon.NewTLSConfig(connectionTLSCert, connectionTLSKey, connectionTLSCACert)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		tlsConfig = c
	}
	addresses := strings.Split(parsedConnection.Host, ",")
	username := parsedConnection.User.Username()
	password, _ := parsedConnection.User.Password()
	dbNumber := 0
	if parsedConnection.Path != "" {
		dbNumber, err = strconv.Atoi(strings.TrimPrefix(parsedConnection.Path, "/"))
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	// create Redis client
	client, err := NewRedisClient(ctx, addresses, username, password, dbNumber, tlsConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	recordKeyTemplate, err := extcommon.NewTemplate("sink_redis_record_key", recordKey)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	recordValueTemplate, err := extcommon.NewTemplate("sink_redis_record_value", recordValue)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	redisSink := &RedisSink{
		Sink:                commonSink,
		ctx:                 ctx,
		client:              client,
		recordKeyTemplate:   recordKeyTemplate,
		recordValueTemplate: recordValueTemplate,
		records:             make([]interface{}, 0, batchSize*2), // 1 records contain key-value pairs
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		commonSink.Logger.Debug(fmt.Sprintf("%s: close record writer", commonSink.Name()))
	})

	// register sink process
	commonSink.RegisterProcess(redisSink.process)

	return redisSink, nil
}

func (s *RedisSink) process() error {
	for msg := range s.Read() {
		b, ok := msg.([]byte)
		if !ok {
			s.Logger.Error(fmt.Sprintf("%s: message type assertion error: %T", s.Name(), msg))
			return fmt.Errorf("message type assertion error: %T", msg)
		}
		s.Logger.Debug(fmt.Sprintf("%s: received message: %s", s.Name(), string(b)))

		var record model.Record
		if err := json.Unmarshal(b, &record); err != nil {
			s.Logger.Error(fmt.Sprintf("%s: invalid data format", s.Name()))
			return errors.WithStack(err)
		}
		recordKey, err := extcommon.Compile(s.recordKeyTemplate, model.ToMap(record))
		if err != nil {
			s.Logger.Error(fmt.Sprintf("%s: failed to compile record key", s.Name()))
			return errors.WithStack(err)
		}
		s.Logger.Debug(fmt.Sprintf("%s: record key: %s", s.Name(), recordKey))
		recordValue, err := extcommon.Compile(s.recordValueTemplate, model.ToMap(record))
		if err != nil {
			s.Logger.Error(fmt.Sprintf("%s: failed to compile record value", s.Name()))
			return errors.WithStack(err)
		}

		s.Logger.Debug(fmt.Sprintf("%s: record value: %s", s.Name(), recordValue))
		// flush records
		if len(s.records) == cap(s.records) {
			if err := s.Retry(s.flush); err != nil {
				s.Logger.Error(fmt.Sprintf("%s: failed to set records", s.Name()))
				return errors.WithStack(err)
			}
			s.records = s.records[:0]
		}
		s.records = append(s.records, recordKey, recordValue)
	}

	// flush remaining records
	if len(s.records) > 0 {
		if err := s.Retry(s.flush); err != nil {
			s.Logger.Error(fmt.Sprintf("%s: failed to set records", s.Name()))
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *RedisSink) flush() error {
	s.Logger.Info(fmt.Sprintf("%s: flushing %d records", s.Name(), len(s.records)/2))
	if err := s.client.MSet(s.ctx, s.records...).Err(); err != nil {
		return err
	}

	return nil
}
