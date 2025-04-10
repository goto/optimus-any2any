package redis

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"text/template"

	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

type RedisSink struct {
	*common.CommonSink
	client redis.Cmdable

	recordKeyTemplate   *template.Template
	recordValueTemplate *template.Template
	records             []interface{}
}

var _ flow.Sink = (*RedisSink)(nil)

// NewSink creates a new RedisSink
func NewSink(commonSink *common.CommonSink,
	connectionDSN string, connectionTLSCert, connectionTLSCACert, connectionTLSKey string,
	recordKey, recordValue string, batchSize int, opts ...common.Option) (*RedisSink, error) {

	// parse connectionDSN
	commonSink.Logger().Debug(fmt.Sprintf("connection DSN: %s", connectionDSN))
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
		c, err := NewTLSConfig(connectionTLSCert, connectionTLSKey, connectionTLSCACert)
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
	client, err := NewRedisClient(commonSink.Context(), addresses, username, password, dbNumber, tlsConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	recordKeyTemplate, err := compiler.NewTemplate("sink_redis_record_key", recordKey)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	recordValueTemplate, err := compiler.NewTemplate("sink_redis_record_value", recordValue)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	s := &RedisSink{
		CommonSink:          commonSink,
		client:              client,
		recordKeyTemplate:   recordKeyTemplate,
		recordValueTemplate: recordValueTemplate,
		records:             make([]interface{}, 0, batchSize*2), // 1 records contain key-value pairs
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s.Logger().Debug(fmt.Sprintf("close record writer"))
		return nil
	})

	// register sink process
	commonSink.RegisterProcess(s.process)

	return s, nil
}

func (s *RedisSink) process() error {
	for record, err := range s.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		recordKey, err := compiler.Compile(s.recordKeyTemplate, model.ToMap(record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("failed to compile record key"))
			return errors.WithStack(err)
		}
		s.Logger().Debug(fmt.Sprintf("record key: %s", recordKey))
		recordValue, err := compiler.Compile(s.recordValueTemplate, model.ToMap(record))
		if err != nil {
			s.Logger().Error(fmt.Sprintf("failed to compile record value"))
			return errors.WithStack(err)
		}

		s.Logger().Debug(fmt.Sprintf("record value: %s", recordValue))
		// flush records
		if len(s.records) == cap(s.records) {
			if err := s.Retry(s.flush); err != nil {
				s.Logger().Error(fmt.Sprintf("failed to set records"))
				return errors.WithStack(err)
			}
			s.records = s.records[:0]
		}
		s.records = append(s.records, recordKey, recordValue)
	}

	// flush remaining records
	if len(s.records) > 0 {
		if err := s.Retry(s.flush); err != nil {
			s.Logger().Error(fmt.Sprintf("failed to set records"))
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *RedisSink) flush() error {
	s.Logger().Info(fmt.Sprintf("flushing %d records", len(s.records)/2))
	if err := s.client.MSet(s.Context(), s.records...).Err(); err != nil {
		return err
	}

	return nil
}
