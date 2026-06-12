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
	client              *http.Client
	method              string
	endpoint            string
	headers             map[string][]string
	body                string
	responseFormat      string
	paginationType      string
	responseRecordsPath string
	nextCursorPath      string
	nextBodyTemplate    *template.Template
	offsetParam         string
	limitParam          string
	pageSize            int
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
		Source:              commonSource,
		client:              client,
		method:              method,
		endpoint:            endpoint,
		headers:             headers,
		body:                body,
		responseFormat:      responseFormat,
		paginationType:      paginationType,
		responseRecordsPath: responseRecordsPath,
		nextCursorPath:      nextCursorPath,
		nextBodyTemplate:    nextBodyTmpl,
		offsetParam:         offsetParam,
		limitParam:          limitParam,
		pageSize:            pageSize,
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

// processNDJSON is the backward-compatible path: reads the response body
// as newline-delimited JSON, sending each line as a record.
func (hs *HTTPSource) processNDJSON() error {
	var bodyReader io.Reader
	if hs.body != "" {
		bodyReader = strings.NewReader(hs.body)
	}

	req, err := http.NewRequest(hs.method, hs.endpoint, bodyReader)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(hs.headers) > 0 {
		for key, values := range hs.headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
	}

	resp, err := hs.client.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("unexpected status code: %d, expected: %d", resp.StatusCode, http.StatusOK)
	}

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
	var bodyReader io.Reader
	if hs.body != "" {
		bodyReader = strings.NewReader(hs.body)
	}

	req, err := http.NewRequest(hs.method, hs.endpoint, bodyReader)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(hs.headers) > 0 {
		for key, values := range hs.headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
	}

	resp, err := hs.client.Do(req)
	if err != nil {
		return errors.WithStack(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("unexpected status code: %d, expected: %d", resp.StatusCode, http.StatusOK)
	}

	respBodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.WithStack(err)
	}

	var responseData interface{}
	if err := json.Unmarshal(respBodyBytes, &responseData); err != nil {
		return errors.WithStack(errors.Wrap(err, "failed to unmarshal response body"))
	}

	records, err := getByPath(responseData, hs.responseRecordsPath)
	if err != nil {
		return errors.WithStack(errors.Wrap(err, "failed to extract records from response"))
	}

	recordsList, ok := records.([]interface{})
	if !ok {
		if records != nil {
			recordsList = []interface{}{records}
		}
	}

	for _, rec := range recordsList {
		recMap, ok := rec.(map[string]interface{})
		if !ok {
			hs.Logger().Warn("skipping non-object record in response")
			continue
		}
		modelRecord := model.NewRecordFromMap(recMap)
		hs.SendRecord(modelRecord)
	}

	return nil
}

// processCursorPagination implements cursor-based pagination.
// First request uses HTTP__BODY, subsequent requests use NEXT_BODY_TEMPLATE with cursor.
func (hs *HTTPSource) processCursorPagination() error {
	body := hs.body

	for {
		var bodyReader io.Reader
		if body != "" {
			bodyReader = strings.NewReader(body)
		}

		req, err := http.NewRequest(hs.method, hs.endpoint, bodyReader)
		if err != nil {
			return errors.WithStack(err)
		}

		if len(hs.headers) > 0 {
			for key, values := range hs.headers {
				for _, value := range values {
					req.Header.Add(key, value)
				}
			}
		}

		resp, err := hs.client.Do(req)
		if err != nil {
			return errors.WithStack(err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return errors.Errorf("unexpected status code: %d, expected: %d", resp.StatusCode, http.StatusOK)
		}

		respBodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return errors.WithStack(err)
		}

		var responseData interface{}
		if err := json.Unmarshal(respBodyBytes, &responseData); err != nil {
			return errors.WithStack(errors.Wrap(err, "failed to unmarshal response body"))
		}

		records, err := getByPath(responseData, hs.responseRecordsPath)
		if err != nil {
			return errors.WithStack(errors.Wrap(err, "failed to extract records from response"))
		}

		recordsList, ok := records.([]interface{})
		if !ok {
			if records != nil {
				recordsList = []interface{}{records}
			}
		}

		if len(recordsList) == 0 {
			break
		}

		for _, rec := range recordsList {
			recMap, ok := rec.(map[string]interface{})
			if !ok {
				hs.Logger().Warn("skipping non-object record in response")
				continue
			}
			modelRecord := model.NewRecordFromMap(recMap)
			hs.SendRecord(modelRecord)
		}

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

		var bodyReader io.Reader
		if body != "" {
			bodyReader = strings.NewReader(body)
		}

		req, err := http.NewRequest(hs.method, hs.endpoint, bodyReader)
		if err != nil {
			return errors.WithStack(err)
		}

		if len(hs.headers) > 0 {
			for key, values := range hs.headers {
				for _, value := range values {
					req.Header.Add(key, value)
				}
			}
		}

		resp, err := hs.client.Do(req)
		if err != nil {
			return errors.WithStack(err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return errors.Errorf("unexpected status code: %d, expected: %d", resp.StatusCode, http.StatusOK)
		}

		respBodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return errors.WithStack(err)
		}

		var responseData interface{}
		if err := json.Unmarshal(respBodyBytes, &responseData); err != nil {
			return errors.WithStack(errors.Wrap(err, "failed to unmarshal response body"))
		}

		records, err := getByPath(responseData, hs.responseRecordsPath)
		if err != nil {
			return errors.WithStack(errors.Wrap(err, "failed to extract records from response"))
		}

		recordsList, ok := records.([]interface{})
		if !ok {
			if records != nil {
				recordsList = []interface{}{records}
			}
		}

		if len(recordsList) == 0 {
			break
		}

		for _, rec := range recordsList {
			recMap, ok := rec.(map[string]interface{})
			if !ok {
				hs.Logger().Warn("skipping non-object record in response")
				continue
			}
			modelRecord := model.NewRecordFromMap(recMap)
			hs.SendRecord(modelRecord)
		}

		// stop if fewer records than page size (last page)
		if len(recordsList) < hs.pageSize {
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
