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

	xauth "github.com/goto/optimus-any2any/internal/auth"
	xclientcredentials "github.com/goto/optimus-any2any/internal/auth/clientcredentials"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	xnet "github.com/goto/optimus-any2any/internal/net"
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
	batchSize int,
	connectionTLSCert, connectionTLSCACert, connectionTLSKey string,
	clientCredentialsProvider, clientCredentialsClientID, clientCredentialsClientSecret, clientCredentialsTokenURL string,
	opts ...common.Option) (*HTTPSink, error) {

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

	client := http.DefaultClient

	if connectionTLSCert != "" && connectionTLSKey != "" && connectionTLSCACert != "" {
		commonSink.Logger().Info("using TLS for connection")
		tlsconfig, err := xauth.NewTLSConfig(connectionTLSCert, connectionTLSKey, connectionTLSCACert)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		client.Transport = &http.Transport{
			TLSClientConfig: tlsconfig,
		}
	}

	// experimental, TODO: refactor this to a proper provider interface
	if clientCredentialsProvider != "" && clientCredentialsClientID != "" && clientCredentialsClientSecret != "" && clientCredentialsTokenURL != "" {
		commonSink.Logger().Info(fmt.Sprintf("using client credentials provider: %s", clientCredentialsProvider))
		switch strings.ToLower(clientCredentialsProvider) {
		case xclientcredentials.CustomProviderA:
			ccProvider := xclientcredentials.NewProviderA(clientCredentialsClientID, clientCredentialsClientSecret, clientCredentialsTokenURL)
			client = ccProvider.Client(commonSink.Context())
		default:
			return nil, errors.New(fmt.Sprintf("unsupported client credentials provider: %s", clientCredentialsProvider))
		}
	}

	s := &HTTPSink{
		Sink:                 commonSink,
		client:               client,
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
		if s.IsSpecializedMetadataRecord(record) {
			s.Logger().Debug("skip specialized metadata record")
			continue
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
		hh, ok := s.httpHandlers[hash]
		if !ok {
			s.Logger().Info(fmt.Sprintf("create new http handler for %s", m.endpoint))
			hh = httpHandler{
				httpMetadata: m,
				records:      []*model.Record{}, // TODO: use storage instead of in-memory
			}
		}

		// flush if batch size is reached
		// batch size is 1 means no batching
		if len(hh.records) >= s.batchSize {
			err := s.Retry(func() error {
				return s.flush(m, hh.records)
			})
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to send data to %s; %s", m.endpoint, err.Error()))
				return errors.WithStack(err)
			}
			hh.records = []*model.Record{} // reset records
		}

		// append record to the handler
		hh.records = append(hh.records, record)
		recordCounter++

		// update the handler
		s.httpHandlers[hash] = hh

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
			bodyBytes, err := io.ReadAll(resp.Body)
			s.Logger().Debug(fmt.Sprintf("http request: %+v", req))
			if err != nil {
				s.Logger().Error(fmt.Sprintf("failed to read response body from %s; err: %s", m.endpoint, err.Error()))
			} else {
				s.Logger().Error(fmt.Sprintf("failed to send data to %s; status code: %d, body: %s", m.endpoint, resp.StatusCode, string(bodyBytes)))
			}
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		if s.batchSize > 1 {
			s.Logger().Info(fmt.Sprintf("successfully sent %d records in batch to %s", recordCount, m.endpoint))
		}
		return nil
	}, func() error {
		// in dry run mode, we don't need to send the request
		// we just need to check the endpoint connectivity
		return xnet.ConnCheck(m.endpoint)
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
		buf := make([]byte, 0, 4*1024)
		sc.Buffer(buf, 1024*1024)

		for sc.Scan() {
			h := sc.Text()
			parts := strings.Split(h, ":")
			if len(parts) != 2 {
				return metadata, fmt.Errorf("invalid header format: %s", h)
			}
			metadata.headers[parts[0]] = append(metadata.headers[parts[0]], strings.Split(parts[1], ",")...)
		}
		if err := sc.Err(); err != nil {
			return metadata, errors.WithStack(err)
		}
	}

	return metadata, nil
}

func hashMetadata(m httpMetadata) string {
	headerStr := strings.Builder{}
	for k, v := range m.headers {
		headerStr.WriteString(fmt.Sprintf("%s=%s;", k, strings.Join(v, ",")))
	}
	s := fmt.Sprintf("%s\n%s\n%s", m.method, m.endpoint, headerStr.String())
	md5sum := md5.Sum([]byte(s))
	return fmt.Sprintf("%x", md5sum)
}
