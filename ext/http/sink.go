package http

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"text/template"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type httpMetadataTemplate struct {
	method   *template.Template
	endpoint *template.Template
	headers  *template.Template
}

type httpMetadata struct {
	method   string
	endpoint string
	headers  map[string][]string
}

type httpHandler struct {
	httpMetadata httpMetadata
	records      []string
}

type HTTPSink struct {
	*common.Sink
	ctx    context.Context
	client *http.Client

	bodyContentTemplate  *template.Template
	httpMetadataTemplate httpMetadataTemplate
	httpHandlers         map[string]httpHandler
	batchSize            int
}

var _ flow.Sink = (*HTTPSink)(nil)

func NewSink(ctx context.Context, l *slog.Logger, metadataPrefix string,
	method, endpoint string, headers map[string]string, headerContent string,
	body, bodyContent string,
	batchSize int, opts ...common.Option) (*HTTPSink, error) {

	// prepare template
	m := httpMetadataTemplate{}
	m.method = template.Must(extcommon.NewTemplate("sink_http_method", method))
	m.endpoint = template.Must(extcommon.NewTemplate("sink_http_endpoint", endpoint))
	if headerContent != "" {
		m.headers = template.Must(extcommon.NewTemplate("sink_http_headers", headerContent))
	} else {
		headerStrBuilder := strings.Builder{}
		for k, v := range headers {
			headerStrBuilder.WriteString(fmt.Sprintf("%s: %s\n", k, v))
		}
		m.headers = template.Must(extcommon.NewTemplate("sink_http_headers", headerStrBuilder.String()))
	}
	if bodyContent != "" {
		body = bodyContent
	}
	bodyContentTemplate := template.Must(extcommon.NewTemplate("sink_http_body", body))

	// create common
	commonSink := common.NewSink(l, metadataPrefix, opts...)
	s := &HTTPSink{
		Sink:                 commonSink,
		ctx:                  ctx,
		client:               http.DefaultClient,
		bodyContentTemplate:  bodyContentTemplate,
		httpMetadataTemplate: m,
		httpHandlers:         make(map[string]httpHandler),
		batchSize:            batchSize,
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		l.Debug("sink(http): close func called")
		l.Info("sink(http): close idle connections")
		s.client.CloseIdleConnections()
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(s.process)

	return s, nil
}

func (s *HTTPSink) process() {
	// log checkpoint
	logCheckPoint := 500
	recordCounter := 0
	for msg := range s.Read() {
		if s.Err() != nil {
			s.Logger.Warn("sink(http): error occurred, skip processing")
			continue
		}

		b, ok := msg.([]byte)
		if !ok {
			s.Logger.Error(fmt.Sprintf("sink(http): invalid message type: %T", msg))
			s.SetError(fmt.Errorf("sink(http): invalid message type: %T", msg))
			continue
		}

		var record map[string]interface{}
		if err := json.Unmarshal(b, &record); err != nil {
			s.Logger.Error("sink(http): invalid data format")
			s.SetError(errors.WithStack(err))
			continue
		}

		m, err := compileMetadata(s.httpMetadataTemplate, record)
		if err != nil {
			s.Logger.Error("sink(http): compile metadata error")
			s.SetError(errors.WithStack(err))
			continue
		}

		hash := hashMetadata(m)
		_, ok = s.httpHandlers[hash]
		if !ok {
			s.httpHandlers[hash] = httpHandler{
				httpMetadata: m,
				records:      make([]string, 0, s.batchSize),
			}
		}

		if len(s.httpHandlers[hash].records) >= s.batchSize {
			if err := s.flush(m, s.httpHandlers[hash].records); err != nil {
				s.Logger.Error(fmt.Sprintf("sink(http): failed to send data to %s: %s", m.endpoint, err.Error()))
				s.SetError(errors.WithStack(err))
				continue
			}
			hh := s.httpHandlers[hash]
			hh.records = make([]string, 0, s.batchSize)
			s.httpHandlers[hash] = hh

			if s.batchSize > 1 {
				s.Logger.Info(fmt.Sprintf("sink(http): successfully send %d records %s %s", s.batchSize, m.method, m.endpoint))
			}
		}

		hh := s.httpHandlers[hash]
		hh.records = append(hh.records, string(b))
		s.httpHandlers[hash] = hh
		recordCounter++

		if s.batchSize == 1 && recordCounter%logCheckPoint == 0 {
			s.Logger.Info(fmt.Sprintf("sink(http): successfully send %d records", recordCounter))
		}
	}

	for _, hh := range s.httpHandlers {
		if len(hh.records) == 0 {
			continue
		}

		if err := s.flush(hh.httpMetadata, hh.records); err != nil {
			s.Logger.Error(fmt.Sprintf("sink(http): failed to send data to %s: %s", hh.httpMetadata.endpoint, err.Error()))
			s.SetError(errors.WithStack(err))
			return
		}

		if s.batchSize > 1 {
			s.Logger.Info(fmt.Sprintf("sink(http): successfully send %d records %s %s", s.batchSize, hh.httpMetadata.method, hh.httpMetadata.endpoint))
		}
	}

	if s.batchSize == 1 && recordCounter%logCheckPoint != 0 {
		s.Logger.Info(fmt.Sprintf("sink(http): successfully send %d records", recordCounter))
	}
}

func (s *HTTPSink) flush(m httpMetadata, records []string) error {
	var raw interface{}
	if s.batchSize == 1 {
		raw = records[0]
	} else {
		raw = records
	}
	body, err := extcommon.Compile(s.bodyContentTemplate, raw)
	if err != nil {
		return errors.WithStack(err)
	}

	u, err := url.Parse(m.endpoint)
	if err != nil {
		return errors.WithStack(err)
	}

	req := http.Request{
		Method:        m.method,
		URL:           u,
		Header:        m.headers,
		ContentLength: int64(len([]byte(body))),
		Body:          io.NopCloser(strings.NewReader(body)),
	}
	resp, err := s.client.Do(req.WithContext(s.ctx))
	if err != nil {
		return errors.WithStack(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	s.Logger.Debug(fmt.Sprintf("sink(http): sent data to %s success with status code %d", m.endpoint, resp.StatusCode))
	return nil
}

func compileMetadata(m httpMetadataTemplate, record map[string]interface{}) (httpMetadata, error) {
	metadata := httpMetadata{}

	if m.method != nil {
		method, err := extcommon.Compile(m.method, record)
		if err != nil {
			return metadata, errors.WithStack(err)
		}
		metadata.method = method
	}

	if m.endpoint != nil {
		endpoint, err := extcommon.Compile(m.endpoint, record)
		if err != nil {
			return metadata, errors.WithStack(err)
		}
		metadata.endpoint = endpoint
	}

	if m.headers != nil {
		headers, err := extcommon.Compile(m.headers, record)
		if err != nil {
			return metadata, errors.WithStack(err)
		}
		metadata.headers = make(map[string][]string)
		sc := bufio.NewScanner(strings.NewReader(headers))
		for sc.Scan() {
			h := sc.Text()
			parts := strings.Split(h, ":")
			if len(parts) != 2 {
				return metadata, fmt.Errorf("invalid header format: %s", h)
			}
			metadata.headers[parts[0]] = append(metadata.headers[parts[0]], strings.Split(parts[1], ",")...)
		}
	}

	return metadata, nil
}

func hashMetadata(m httpMetadata) string {
	s := fmt.Sprintf("%s\n%s\n%s", m.method, m.endpoint, m.headers)
	md5sum := md5.Sum([]byte(s))
	return fmt.Sprintf("%x", md5sum)
}
