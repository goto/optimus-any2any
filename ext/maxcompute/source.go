package maxcompute

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"text/template"

	"maps"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// MaxcomputeSource is the source component for MaxCompute.
type MaxcomputeSource struct {
	*common.Source

	client          *odps.Odps
	tunnel          *tunnel.Tunnel
	additionalHints map[string]string
	metadataPrefix  string
	preQuery        string
	queryTemplate   *template.Template
}

var _ flow.Source = (*MaxcomputeSource)(nil)

// NewSource creates a new MaxcomputeSource.
func NewSource(l *slog.Logger, metadataPrefix string, creds string, queryFilePath string, prequeryFilePath string, executionProject string, additionalHints map[string]string, opts ...common.Option) (*MaxcomputeSource, error) {
	// create commonSource source
	commonSource := common.NewSource(l, opts...)

	// create client for maxcompute
	client, err := NewClient(creds)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if executionProject != "" {
		client.SetDefaultProjectName(executionProject)
	}

	// read pre-query from file
	var rawPreQuery []byte
	if prequeryFilePath != "" {
		rawPreQuery, err = os.ReadFile(prequeryFilePath)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	// read query from file
	rawQuery, err := os.ReadFile(queryFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	queryTemplate, err := extcommon.NewTemplate("source_mc_query", string(rawQuery))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// create tunnel
	t, err := tunnel.NewTunnelFromProject(client.DefaultProject())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// add additional hints
	hints := make(map[string]string)
	maps.Copy(hints, additionalHints)

	mc := &MaxcomputeSource{
		Source:          commonSource,
		client:          client,
		tunnel:          t,
		additionalHints: hints,
		metadataPrefix:  metadataPrefix,
		queryTemplate:   queryTemplate,
		preQuery:        string(rawPreQuery),
	}

	// add clean function
	commonSource.AddCleanFunc(func() {
		commonSource.Logger.Debug("source(mc): cleaning up")
	})

	commonSource.RegisterProcess(mc.process)

	return mc, nil
}

// process is the process function for MaxcomputeSource.
func (mc *MaxcomputeSource) process() {
	preRecordReader := mc.getRecordReader(mc.preQuery)
	defer preRecordReader.Close()

	scPreQuery := bufio.NewScanner(preRecordReader)
	for scPreQuery.Scan() {
		rawPreRecord := scPreQuery.Bytes()
		linePreRecord := make([]byte, len(rawPreRecord)) // Important: make a copy of the line before processing
		copy(linePreRecord, rawPreRecord)

		var preRecord map[string]interface{}
		if err := json.Unmarshal(linePreRecord, &preRecord); err != nil {
			mc.Logger.Error("source(mc): invalid data format")
			mc.SetError(errors.WithStack(err))
			return
		}
		// add prefix for every key
		for k := range preRecord {
			preRecord[fmt.Sprintf("%s%s", mc.metadataPrefix, k)] = preRecord[k]
			delete(preRecord, k)
		}

		// compile query
		query, err := extcommon.Compile(mc.queryTemplate, preRecord)
		if err != nil {
			mc.Logger.Error("source(mc): failed to compile query")
			mc.SetError(errors.WithStack(err))
			return
		}

		recordReader := mc.getRecordReader(query)
		defer recordReader.Close()

		scQuery := bufio.NewScanner(recordReader)
		for scQuery.Scan() {
			rawRecord := scQuery.Bytes()
			lineRecord := make([]byte, len(rawRecord)) // Important: make a copy of the line before processing
			copy(lineRecord, rawRecord)

			var record map[string]interface{}
			if err := json.Unmarshal(lineRecord, &record); err != nil {
				mc.Logger.Error("source(mc): invalid data format")
				mc.SetError(errors.WithStack(err))
				return
			}
			// merge with pre-record
			for k := range preRecord {
				if _, ok := record[k]; !ok {
					record[k] = preRecord[k]
				}
			}

			raw, err := json.Marshal(record)
			if err != nil {
				mc.Logger.Error("source(mc): failed to marshal record")
				mc.SetError(errors.WithStack(err))
				return
			}
			mc.Send(raw)
		}
	}
}

func (mc *MaxcomputeSource) getRecordReader(query string) io.ReadCloser {
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		if query == "" {
			w.Write([]byte("{}\n"))
			return
		}
		// run query
		mc.Logger.Info(fmt.Sprintf("source(mc): running query:\n%s", query))
		instance, err := mc.client.ExecSQl(query, mc.additionalHints)
		if err != nil {
			mc.Logger.Error(fmt.Sprintf("source(mc): failed to run query: %s", query))
			mc.SetError(errors.WithStack(err))
			return
		}

		// wait for query to finish
		mc.Logger.Info("source(mc): waiting for query to finish")
		if err := instance.WaitForSuccess(); err != nil {
			mc.Logger.Error("source(mc): query failed")
			mc.SetError(errors.WithStack(err))
			return
		}

		// create session for reading records
		mc.Logger.Info("source(mc): creating session for reading records")
		session, err := mc.tunnel.CreateInstanceResultDownloadSession(mc.client.DefaultProjectName(), instance.Id())
		if err != nil {
			mc.Logger.Error("source(mc): failed to create session for reading records")
			mc.SetError(errors.WithStack(err))
			return
		}

		recordCount := session.RecordCount()
		mc.Logger.Info(fmt.Sprintf("source(mc): record count: %d", recordCount))

		// read records
		i := 0
		step := 1000 // batch size for reading records
		for i < recordCount {
			reader, err := session.OpenRecordReader(i, step, 0, nil)
			if err != nil {
				mc.Logger.Error("source(mc): failed to open record reader")
				mc.SetError(errors.WithStack(err))
				return
			}
			defer reader.Close()

			count := 0
			for {
				record, err := reader.Read()
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					mc.Logger.Error("source(mc): failed to read record")
					mc.SetError(errors.WithStack(err))
					return
				}

				// process record
				mc.Logger.Debug(fmt.Sprintf("source(mc): record: %s", record))
				v, err := fromRecord(mc.Logger, record, session.Schema())
				if err != nil {
					mc.Logger.Error("source(mc): failed to process record")
					mc.SetError(errors.WithStack(err))
					return
				}
				raw, err := json.Marshal(v)
				if err != nil {
					mc.SetError(errors.WithStack(err))
					return
				}
				w.Write(raw)
				w.Write([]byte("\n"))
				count++
			}
			i += count
			mc.Logger.Info(fmt.Sprintf("source(mc): send %d records", count))
		}
	}()
	return r
}
