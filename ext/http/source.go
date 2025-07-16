package http

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"

	"github.com/goccy/go-json"
	xclientcredentials "github.com/goto/optimus-any2any/internal/auth/clientcredentials"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// HTTPSource is a source that reads data from an HTTP endpoint.
type HTTPSource struct {
	common.Source
	client   *http.Client
	endpoint string
	headers  map[string][]string
}

var _ flow.Source = (*HTTPSource)(nil)

// NewSource creates a new HTTP source.
func NewSource(commonSource common.Source, endpoint string, headerContent string,
	clientCredentialsProvider, clientCredentialsClientID, clientCredentialsClientSecret, clientCredentialsTokenURL string,
	opts ...common.Option) (*HTTPSource, error) {

	// read headers from the headerContent
	sc := bufio.NewScanner(strings.NewReader(headerContent))
	headers := make(map[string][]string)
	for sc.Scan() {
		h := sc.Text()
		parts := strings.Split(h, ":")
		if len(parts) != 2 {
			return nil, errors.New("invalid header format, expected 'key: value'")
		}
		if headers[parts[0]] == nil {
			headers[parts[0]] = []string{}
		}
		headers[parts[0]] = append(headers[parts[0]], strings.Split(parts[1], ",")...)
	}
	if err := sc.Err(); err != nil {
		return nil, errors.WithStack(err)
	}

	client := http.DefaultClient

	// experimental, TODO: refactor this to a proper provider interface
	if clientCredentialsProvider != "" && clientCredentialsClientID != "" && clientCredentialsClientSecret != "" && clientCredentialsTokenURL != "" {
		commonSource.Logger().Info(fmt.Sprintf("using client credentials provider: %s", clientCredentialsProvider))
		switch strings.ToLower(clientCredentialsProvider) {
		case xclientcredentials.CustomProviderA:
			ccProvider := xclientcredentials.NewProviderA(clientCredentialsClientID, clientCredentialsClientSecret, clientCredentialsTokenURL)
			client = ccProvider.Client(commonSource.Context())
		default:
			return nil, errors.New(fmt.Sprintf("unsupported client credentials provider: %s", clientCredentialsProvider))
		}
	}

	hs := &HTTPSource{
		Source:   commonSource,
		client:   client,
		endpoint: endpoint,
		headers:  headers,
	}

	hs.AddCleanFunc(func() error {
		hs.client.CloseIdleConnections()
		return nil
	})
	hs.RegisterProcess(hs.Process)

	return hs, nil
}

func (hs *HTTPSource) Process() error {
	req, err := http.NewRequest(http.MethodGet, hs.endpoint, nil)
	if err != nil {
		return errors.WithStack(err)
	}

	// set headers if needed
	if len(hs.headers) > 0 {
		for key, values := range hs.headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
	}

	// do the request
	resp, err := hs.client.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("unexpected status code: %d, expected: %d", resp.StatusCode, http.StatusOK)
	}

	// TODO: handle pagination if needed
	// for now, we assume the response body contains all records in a single response
	// if pagination is needed, we can add logic to handle it here

	// read the response body and send it to the channel
	sc := bufio.NewScanner(resp.Body)
	buf := make([]byte, 0, 4*1024)
	sc.Buffer(buf, 1024*1024)
	for sc.Scan() {
		raw := sc.Bytes()
		line := make([]byte, len(raw)) // Important: make a copy of the line
		copy(line, raw)

		modelRecord := model.NewRecord()
		if err := json.Unmarshal(line, &modelRecord); err != nil {
			return errors.WithStack(errors.Wrap(err, "failed to unmarshal record"))
		}
		// send to channel
		hs.SendRecord(modelRecord)
	}
	if err := sc.Err(); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
