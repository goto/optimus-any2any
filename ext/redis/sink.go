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

	// parse connectionDSN
	l.Debug(fmt.Sprintf("sink(redis): connection DSN: %s", connectionDSN))
	parsedConnection, err := url.Parse(connectionDSN)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if parsedConnection.Scheme != "redis" && parsedConnection.Scheme != "rediss" {
		return nil, errors.New(fmt.Sprintf("sink(redis): invalid connection DSN scheme: %s", parsedConnection.Scheme))
	}
	var tlsConfig *tls.Config
	if parsedConnection.Scheme == "rediss" {
		if connectionTLSCert == "" || connectionTLSKey == "" || connectionTLSCACert == "" {
			return nil, errors.New("sink(redis): missing TLS certificate, key or CA certificate")
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
		commonSink.Logger.Debug("sink(redis): close record writer")
	})

	// register sink process
	commonSink.RegisterProcess(redisSink.process)

	return redisSink, nil
}

func (s *RedisSink) process() {
	// s.client.MSet()
	for msg := range s.Read() {
		if s.Err() != nil {
			continue
		}
		b, ok := msg.([]byte)
		if !ok {
			s.Logger.Error(fmt.Sprintf("sink(redis): message type assertion error: %T", msg))
			s.SetError(errors.New(fmt.Sprintf("sink(redis): message type assertion error: %T", msg)))
			continue
		}
		s.Logger.Debug(fmt.Sprintf("sink(redis): received message: %s", string(b)))

		var record model.Record
		if err := json.Unmarshal(b, &record); err != nil {
			s.Logger.Error("sink(redis): invalid data format")
			s.SetError(errors.WithStack(err))
			continue
		}
		recordKey, err := extcommon.Compile(s.recordKeyTemplate, model.ToMap(record))
		if err != nil {
			s.Logger.Error("sink(redis): failed to compile record key")
			s.SetError(errors.WithStack(err))
			continue
		}
		s.Logger.Debug(fmt.Sprintf("sink(redis): record key: %s", recordKey))
		recordValue, err := extcommon.Compile(s.recordValueTemplate, model.ToMap(record))
		if err != nil {
			s.Logger.Error("sink(redis): failed to compile record value")
			s.SetError(errors.WithStack(err))
			continue
		}

		s.Logger.Debug(fmt.Sprintf("sink(redis): record value: %s", recordValue))
		// flush records
		if len(s.records) == cap(s.records) {
			if err := s.Retry(s.flush); err != nil {
				s.Logger.Error("sink(redis): failed to set records")
				s.SetError(errors.WithStack(err))
				continue
			}
			s.records = s.records[:0]
		}
		s.records = append(s.records, recordKey, recordValue)
	}

	// flush remaining records
	if len(s.records) > 0 {
		if err := s.Retry(s.flush); err != nil {
			s.Logger.Error("sink(redis): failed to set records")
			s.SetError(errors.WithStack(err))
		}
	}
}

func (s *RedisSink) flush() error {
	s.Logger.Info(fmt.Sprintf("sink(redis): flushing %d records", len(s.records)/2))
	if err := s.client.MSet(s.ctx, s.records...).Err(); err != nil {
		return err
	}

	return nil
}
