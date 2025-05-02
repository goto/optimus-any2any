package http

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"text/template"

	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
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
	records      []*model.Record
}

type HTTPSink struct {
	common.Sink
	client *http.Client

	bodyContentTemplate  *template.Template
	httpMetadataTemplate httpMetadataTemplate
	httpHandlers         map[string]httpHandler
	batchSize            int
}

var _ flow.Sink = (*HTTPSink)(nil)

func NewSink(commonSink common.Sink,
	method, endpoint string, headers map[string]string, headerContent string,
	body, bodyContent string,
	batchSize int, opts ...common.Option) (*HTTPSink, error) {

	// prepare template
	m := httpMetadataTemplate{}
	m.method = template.Must(compiler.NewTemplate("sink_http_method", method))
	m.endpoint = template.Must(compiler.NewTemplate("sink_http_endpoint", endpoint))
	if headerContent != "" {
		m.headers = template.Must(compiler.NewTemplate("sink_http_headers", headerContent))
	} else {
		headerStrBuilder := strings.Builder{}
		for k, v := range headers {
			headerStrBuilder.WriteString(fmt.Sprintf("%s: %s\n", k, v))
		}
		m.headers = template.Must(compiler.NewTemplate("sink_http_headers", headerStrBuilder.String()))
	}
	if bodyContent != "" {
		body = bodyContent
	}
	bodyContentTemplate := template.Must(compiler.NewTemplate("sink_http_body", body))

	s := &HTTPSink{
		Sink:                 commonSink,
		client:               http.DefaultClient,
		bodyContentTemplate:  bodyContentTemplate,
		httpMetadataTemplate: m,
		httpHandlers:         make(map[string]httpHandler),
		batchSize:            batchSize,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s.Logger().Info(fmt.Sprintf("close idle connections"))
		s.client.CloseIdleConnections()
		return nil
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(s.process)

	return s, nil
}

func (s *HTTPSink) process() error {
	// log checkpoint
	logCheckPoint := 500
	recordCounter := 0
	for record, err := range s.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}

		m, err := compileMetadata(s.httpMetadataTemplate, record)
		if err != nil {
			s.Logger().Error(fmt.Sprintf("compile metadata error"))
			return errors.WithStack(err)
		}

		// remove metadata prefix
		record = s.RecordWithoutMetadata(record)
		// initialize http handler
		hash := hashMetadata(m)
		_, ok := s.httpHandlers[hash]
		if !ok {
			s.httpHandlers[hash] = httpHandler{
				httpMetadata: m,
				records:      make([]*model.Record, 0, s.batchSize), // TODO: use storage instead of in-memory
			}
		}

		// flush if batch size is reached
		// batch size is 1 means no batching
		if len(s.httpHandlers[hash].records) >= s.batchSize {
			err := s.Retry(func() error {
				return s.flush(m, s.httpHandlers[hash].records)
			})
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to send data to %s; %s", m.endpoint, err.Error()))
				return errors.WithStack(err)
			}
			hh := s.httpHandlers[hash]
			hh.records = make([]*model.Record, 0, s.batchSize)
			s.httpHandlers[hash] = hh
		}

		// append record to the handler
		hh := s.httpHandlers[hash]
		hh.records = append(hh.records, record)
		s.httpHandlers[hash] = hh
		recordCounter++

		if s.batchSize == 1 && recordCounter%logCheckPoint == 0 {
			_ = s.DryRunable(func() error { // ignore log when dry run
				s.Logger().Info(fmt.Sprintf("successfully sent %d records", recordCounter))
				return nil
			})
		}
	}

	for _, hh := range s.httpHandlers {
		if len(hh.records) == 0 {
			continue
		}

		err := s.Retry(func() error {
			return s.flush(hh.httpMetadata, hh.records)
		})
		if err != nil {
			s.Logger().Error(fmt.Sprintf("failed to send data to %s; %s", hh.httpMetadata.endpoint, err.Error()))
			return errors.WithStack(err)
		}
	}
	_ = s.DryRunable(func() error { // ignore log when dry run
		s.Logger().Info(fmt.Sprintf("successfully sent %d records in total", recordCounter))
		return nil
	})

	return nil
}

func (s *HTTPSink) flush(m httpMetadata, records []*model.Record) error {
	var recordCount int
	var body string
	var err error
	// prefix metadata will not be used in the body content template
	if s.batchSize == 1 {
		recordCount = 1
		body, err = compiler.Compile(s.bodyContentTemplate, records[0])
	} else {
		recordCount = len(records)
		body, err = compiler.Compile(s.bodyContentTemplate, records)
	}
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
	err = s.DryRunable(func() error {
		resp, err := s.client.Do(req.WithContext(s.Context()))
		if err != nil {
			return errors.WithStack(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		if s.batchSize > 1 {
			s.Logger().Info(fmt.Sprintf("successfully sent %d records in batch to %s", recordCount, m.endpoint))
		}
		return nil
	})

	return errors.WithStack(err)
}

func compileMetadata(m httpMetadataTemplate, record *model.Record) (httpMetadata, error) {
	metadata := httpMetadata{}

	if m.method != nil {
		method, err := compiler.Compile(m.method, model.ToMap(record))
		if err != nil {
			return metadata, errors.WithStack(err)
		}
		metadata.method = method
	}

	if m.endpoint != nil {
		endpoint, err := compiler.Compile(m.endpoint, model.ToMap(record))
		if err != nil {
			return metadata, errors.WithStack(err)
		}
		metadata.endpoint = endpoint
	}

	if m.headers != nil {
		headers, err := compiler.Compile(m.headers, model.ToMap(record))
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
