package http

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strings"
	"text/template"

	"github.com/goccy/go-json"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/ext/compiler"
	"github.com/goto/optimus-any2any/internal/ext/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// HTTPSource is a source that reads data from an HTTP endpoint.
type HTTPSource struct {
	common.Source
	client                 *http.Client
	method                 string
	endpoint               string
	headers                map[string][]string
	body                   string
	responseFormat         string
	paginationType         string
	responseRecordsPath    string
	nextCursorPath         string
	nextBodyTemplate       *template.Template
	offsetParam            string
	limitParam             string
	pageSize               int
	responseHeadersToStore []string
}

var _ flow.Source = (*HTTPSource)(nil)

// NewSource creates a new HTTP source.
func NewSource(commonSource common.Source,
	method, endpoint string, headerContent string,
	body string,
	responseFormat string,
	paginationType string, responseRecordsPath, nextCursorPath string, nextBodyTemplate string,
	offsetParam, limitParam string, pageSize int,
	clientCredentialsProvider, clientCredentialsClientID, clientCredentialsClientSecret, clientCredentialsTokenURL string,
	responseHeadersToStore []string,
	opts ...common.Option) (*HTTPSource, error) {

	// read headers from the headerFilePath
	scanner := bufio.NewScanner(strings.NewReader(headerContent))
	headers := make(map[string][]string)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Ignore empty lines
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return nil, errors.New("invalid header format, expected 'key: value'")
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if headers[key] == nil {
			headers[key] = []string{}
		}
		headers[key] = append(headers[key], strings.Split(value, ",")...)
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.WithStack(err)
	}

	client := http.DefaultClient

	// experimental
	if isUsingOAuth2(clientCredentialsProvider, clientCredentialsClientID, clientCredentialsClientSecret, clientCredentialsTokenURL) {
		c, err := newClientWithOAuth2(commonSource.Context(), commonSource.Logger(), clientCredentialsProvider, clientCredentialsClientID, clientCredentialsClientSecret, clientCredentialsTokenURL)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		client = c
		commonSource.Logger().Info(fmt.Sprintf("using client credentials provider: %s", clientCredentialsProvider))
	}

	// compile next body template if provided
	var nextBodyTmpl *template.Template
	if nextBodyTemplate != "" {
		tmpl, err := compiler.NewTemplate("http_source_next_body", nextBodyTemplate)
		if err != nil {
			return nil, errors.Wrap(err, "failed to compile next body template")
		}
		nextBodyTmpl = tmpl
	}

	hs := &HTTPSource{
		Source:                 commonSource,
		client:                 client,
		method:                 method,
		endpoint:               endpoint,
		headers:                headers,
		body:                   body,
		responseFormat:         responseFormat,
		paginationType:         paginationType,
		responseRecordsPath:    responseRecordsPath,
		nextCursorPath:         nextCursorPath,
		nextBodyTemplate:       nextBodyTmpl,
		offsetParam:            offsetParam,
		limitParam:             limitParam,
		pageSize:               pageSize,
		responseHeadersToStore: responseHeadersToStore,
	}

	hs.AddCleanFunc(func() error {
		hs.client.CloseIdleConnections()
		return nil
	})
	hs.RegisterProcess(hs.Process)

	return hs, nil
}

func (hs *HTTPSource) Process() error {
	switch hs.paginationType {
	case "cursor":
		return hs.processCursorPagination()
	case "offset":
		return hs.processOffsetPagination()
	default:
		// backward compatible: no pagination
		switch hs.responseFormat {
		case "json":
			return hs.processJSONSinglePage()
		default:
			return hs.processNDJSON()
		}
	}
}

// newRequest creates a new HTTP request with the given body and configured headers.
func (hs *HTTPSource) newRequest(body string) (*http.Request, error) {
	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}

	req, err := http.NewRequest(hs.method, hs.endpoint, bodyReader)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	for key, values := range hs.headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	return req, nil
}

// doRequest executes the request and validates the response status.
// The caller is responsible for closing the response body on success.
func (hs *HTTPSource) doRequest(req *http.Request) (*http.Response, error) {
	resp, err := hs.client.Do(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, errors.Errorf("unexpected status code: %d, expected: %d", resp.StatusCode, http.StatusOK)
	}

	return resp, nil
}

// readJSONResponse reads and closes the response body and unmarshals it as JSON.
func (hs *HTTPSource) readJSONResponse(resp *http.Response) (interface{}, error) {
	defer resp.Body.Close()

	respBodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var responseData interface{}
	if err := json.Unmarshal(respBodyBytes, &responseData); err != nil {
		return nil, errors.WithStack(errors.Wrap(err, "failed to unmarshal response body"))
	}

	return responseData, nil
}

// extractRecords extracts the records list from response data using the configured path.
func (hs *HTTPSource) extractRecords(responseData interface{}) ([]interface{}, error) {
	records, err := getByPath(responseData, hs.responseRecordsPath)
	if err != nil {
		return nil, errors.WithStack(errors.Wrap(err, "failed to extract records from response"))
	}

	recordsList, ok := records.([]interface{})
	if !ok {
		if records != nil {
			recordsList = []interface{}{records}
		}
	}

	return recordsList, nil
}

// sendRecords sends the records downstream. If headers is non-nil, it is attached
// to every record as a metadata field.
func (hs *HTTPSource) sendRecords(records []interface{}, headers map[string][]string) {
	for _, rec := range records {
		recMap, ok := rec.(map[string]interface{})
		if !ok {
			hs.Logger().Warn("skipping non-object record in response")
			continue
		}
		modelRecord := model.NewRecordFromMap(recMap)
		hs.setResponseHeaders(modelRecord, headers)
		hs.SendRecord(modelRecord)
	}
}

// setResponseHeaders attaches the given headers to the record as a metadata field,
// if headers is non-nil.
func (hs *HTTPSource) setResponseHeaders(record *model.Record, headers map[string][]string) {
	if headers == nil {
		return
	}
	record.Set(hs.MetadataPrefix()+"http_response_headers", headers)
}

// extractResponseHeaders returns only the allowlisted headers from header, keyed by
// their canonical name. Returns nil if no allowlist is configured or none match.
func extractResponseHeaders(header http.Header, names []string) map[string][]string {
	if len(names) == 0 {
		return nil
	}
	result := make(map[string][]string)
	for _, name := range names {
		if values := header.Values(name); len(values) > 0 {
			result[http.CanonicalHeaderKey(name)] = values
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// processNDJSON is the backward-compatible path: reads the response body
// as newline-delimited JSON, sending each line as a record.
func (hs *HTTPSource) processNDJSON() error {
	req, err := hs.newRequest(hs.body)
	if err != nil {
		return errors.WithStack(err)
	}

	resp, err := hs.doRequest(req)
	if err != nil {
		return errors.WithStack(err)
	}
	defer resp.Body.Close()
	headers := extractResponseHeaders(resp.Header, hs.responseHeadersToStore)

	// read the response body and send it to the channel
	reader := bufio.NewReader(resp.Body)
	for {
		raw, err := reader.ReadBytes('\n')
		if len(raw) > 0 && raw[0] != '\n' {
			line := make([]byte, len(raw))
			copy(line, raw)

			modelRecord := model.NewRecord()
			if err := json.Unmarshal(line, &modelRecord); err != nil {
				return errors.WithStack(errors.Wrap(err, "failed to unmarshal record"))
			}
			hs.setResponseHeaders(modelRecord, headers)
			hs.SendRecord(modelRecord)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

// processJSONSinglePage reads the full response as a single JSON object,
// extracts records by path, and sends them downstream. No pagination.
func (hs *HTTPSource) processJSONSinglePage() error {
	req, err := hs.newRequest(hs.body)
	if err != nil {
		return errors.WithStack(err)
	}

	resp, err := hs.doRequest(req)
	if err != nil {
		return errors.WithStack(err)
	}
	headers := extractResponseHeaders(resp.Header, hs.responseHeadersToStore)

	responseData, err := hs.readJSONResponse(resp)
	if err != nil {
		return errors.WithStack(err)
	}

	records, err := hs.extractRecords(responseData)
	if err != nil {
		return errors.WithStack(err)
	}

	hs.sendRecords(records, headers)
	return nil
}

// processCursorPagination implements cursor-based pagination.
// First request uses HTTP__BODY, subsequent requests use NEXT_BODY_TEMPLATE with cursor.
func (hs *HTTPSource) processCursorPagination() error {
	body := hs.body

	for {
		req, err := hs.newRequest(body)
		if err != nil {
			return errors.WithStack(err)
		}

		resp, err := hs.doRequest(req)
		if err != nil {
			return errors.WithStack(err)
		}
		headers := extractResponseHeaders(resp.Header, hs.responseHeadersToStore)

		responseData, err := hs.readJSONResponse(resp)
		if err != nil {
			return errors.WithStack(err)
		}

		records, err := hs.extractRecords(responseData)
		if err != nil {
			return errors.WithStack(err)
		}

		if len(records) == 0 {
			break
		}

		hs.sendRecords(records, headers)

		// extract cursor
		if hs.nextCursorPath == "" {
			break
		}

		cursorVal, err := getByPath(responseData, hs.nextCursorPath)
		if err != nil {
			hs.Logger().Info(fmt.Sprintf("no cursor found at path %q, stopping pagination", hs.nextCursorPath))
			break
		}

		cursor, ok := cursorVal.(string)
		if !ok || cursor == "" {
			hs.Logger().Info("cursor is empty, stopping pagination")
			break
		}

		if hs.nextBodyTemplate == nil {
			hs.Logger().Info("no next body template configured, stopping pagination")
			break
		}

		body, err = compiler.Compile(hs.nextBodyTemplate, map[string]interface{}{
			"cursor": cursor,
		})
		if err != nil {
			return errors.WithStack(errors.Wrap(err, "failed to compile next body template"))
		}
	}

	return nil
}

// processOffsetPagination implements offset-based pagination.
// The body is treated as a Go template with [[ .offset ]] and [[ .limit ]] variables.
// Offset increments by PAGE_SIZE each iteration until response has fewer records.
func (hs *HTTPSource) processOffsetPagination() error {
	offset := 0

	// compile body as Go template for offset injection
	var bodyTmpl *template.Template
	if hs.body != "" {
		tmpl, err := compiler.NewTemplate("http_offset_body", hs.body)
		if err != nil {
			return errors.Wrap(err, "failed to compile offset body template")
		}
		bodyTmpl = tmpl
	}

	for {
		var body string
		if bodyTmpl != nil {
			compiled, err := compiler.Compile(bodyTmpl, map[string]interface{}{
				hs.offsetParam: offset,
				hs.limitParam:  hs.pageSize,
			})
			if err != nil {
				return errors.WithStack(errors.Wrap(err, "failed to compile offset body"))
			}
			body = compiled
		}

		req, err := hs.newRequest(body)
		if err != nil {
			return errors.WithStack(err)
		}

		resp, err := hs.doRequest(req)
		if err != nil {
			return errors.WithStack(err)
		}
		headers := extractResponseHeaders(resp.Header, hs.responseHeadersToStore)

		responseData, err := hs.readJSONResponse(resp)
		if err != nil {
			return errors.WithStack(err)
		}

		records, err := hs.extractRecords(responseData)
		if err != nil {
			return errors.WithStack(err)
		}

		if len(records) == 0 {
			break
		}

		hs.sendRecords(records, headers)

		// stop if fewer records than page size (last page)
		if len(records) < hs.pageSize {
			break
		}

		offset += hs.pageSize
	}

	return nil
}

// getByPath extracts a value from a nested JSON structure using dot-notation.
// For example, path "data" extracts response["data"], path "response.items" extracts response["response"]["items"].
// Returns the value at the path, or an error if the path cannot be traversed.
func getByPath(data interface{}, path string) (interface{}, error) {
	if path == "" || path == "." {
		return data, nil
	}

	parts := strings.Split(path, ".")
	current := data

	for _, part := range parts {
		if part == "" {
			continue
		}

		m, ok := current.(map[string]interface{})
		if !ok {
			return nil, errors.Errorf("cannot traverse non-object at path segment %q", part)
		}

		val, exists := m[part]
		if !exists {
			return nil, errors.Errorf("key %q not found", part)
		}
		current = val
	}

	return current, nil
}
