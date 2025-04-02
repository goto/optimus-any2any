package maxcompute

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"text/template"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/goccy/go-json"

	"maps"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// MaxcomputeSource is the source component for MaxCompute.
type MaxcomputeSource struct {
	*common.CommonSource

	client          *odps.Odps
	tunnel          *tunnel.Tunnel
	additionalHints map[string]string
	preQuery        string
	queryTemplate   *template.Template
}

var _ flow.Source = (*MaxcomputeSource)(nil)

// NewSource creates a new MaxcomputeSource.
func NewSource(l *slog.Logger, creds string, queryFilePath string, prequeryFilePath string, executionProject string, additionalHints map[string]string, opts ...common.Option) (*MaxcomputeSource, error) {
	// create commonSource source
	commonSource := common.NewCommonSource(l, "mc", opts...)

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
	queryTemplate, err := compiler.NewTemplate("source_mc_query", string(rawQuery))
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
		CommonSource:    commonSource,
		client:          client,
		tunnel:          t,
		additionalHints: hints,
		queryTemplate:   queryTemplate,
		preQuery:        string(rawPreQuery),
	}

	// add clean function
	commonSource.AddCleanFunc(func() error {
		mc.Logger().Debug(fmt.Sprintf("cleaning up"))
		return nil
	})

	commonSource.RegisterProcess(mc.process)

	return mc, nil
}

// process is the process function for MaxcomputeSource.
func (mc *MaxcomputeSource) process() error {
	preRecordReader, err := mc.getRecordReader(mc.preQuery)
	if err != nil {
		mc.Logger().Error(fmt.Sprintf("failed to get pre-record reader"))
		return errors.WithStack(err)
	}
	defer preRecordReader.Close()

	scPreQuery := bufio.NewScanner(preRecordReader)
	for scPreQuery.Scan() {
		rawPreRecord := scPreQuery.Bytes()
		linePreRecord := make([]byte, len(rawPreRecord)) // Important: make a copy of the line before processing
		copy(linePreRecord, rawPreRecord)

		var preRecord model.Record
		if err := json.Unmarshal(linePreRecord, &preRecord); err != nil {
			mc.Logger().Error(fmt.Sprintf("invalid data format"))
			return errors.WithStack(err)
		}
		// add prefix for every key
		preRecordWithPrefix := mc.RecordWithMetadata(&preRecord)
		mc.Logger().Debug(fmt.Sprintf("pre-record: %v", preRecordWithPrefix))

		// compile query
		query, err := compiler.Compile(mc.queryTemplate, model.ToMap(preRecordWithPrefix))
		if err != nil {
			mc.Logger().Error(fmt.Sprintf("failed to compile query"))
			return errors.WithStack(err)
		}

		recordReader, err := mc.getRecordReader(query)
		if err != nil {
			mc.Logger().Error(fmt.Sprintf("failed to get record reader"))
			return errors.WithStack(err)
		}
		defer recordReader.Close()

		scQuery := bufio.NewScanner(recordReader)
		for scQuery.Scan() {
			rawRecord := scQuery.Bytes()
			lineRecord := make([]byte, len(rawRecord)) // Important: make a copy of the line before processing
			copy(lineRecord, rawRecord)

			var record model.Record
			if err := json.Unmarshal(lineRecord, &record); err != nil {
				mc.Logger().Error(fmt.Sprintf("invalid data format"))
				return errors.WithStack(err)
			}
			// merge with pre-record
			for k := range preRecordWithPrefix.AllFromFront() {
				if _, ok := record.Get(k); !ok {
					record.Set(k, preRecordWithPrefix.GetOrDefault(k, nil))
				}
			}

			raw, err := json.Marshal(record)
			if err != nil {
				mc.Logger().Error(fmt.Sprintf("failed to marshal record"))
				return errors.WithStack(err)
			}
			mc.Send(raw)
		}
	}
	return nil
}

func (mc *MaxcomputeSource) getRecordReader(query string) (io.ReadCloser, error) {
	var e error
	buf := buffer.New(32 * 1024)
	r, w := nio.Pipe(buf)
	go func(w io.WriteCloser) {
		defer w.Close()
		if query == "" {
			w.Write([]byte("{}\n"))
			return
		}
		// run query
		additionalHints := map[string]string{}
		maps.Copy(additionalHints, mc.additionalHints)
		if strings.Contains(query, ";") {
			additionalHints["odps.sql.submit.mode"] = "script"
		}
		mc.Logger().Info(fmt.Sprintf("running query:\n%s", query))
		instance, err := mc.client.ExecSQl(query, additionalHints)
		if err != nil {
			mc.Logger().Error(fmt.Sprintf("failed to run query: %s", query))
			e = errors.WithStack(err)
			return
		}

		// wait for query to finish
		mc.Logger().Info(fmt.Sprintf("waiting for query to finish"))
		if err := instance.WaitForSuccess(); err != nil {
			mc.Logger().Error(fmt.Sprintf("query failed"))
			e = errors.WithStack(err)
			return
		}

		// create session for reading records
		mc.Logger().Info(fmt.Sprintf("creating session for reading records"))
		session, err := mc.tunnel.CreateInstanceResultDownloadSession(mc.client.DefaultProjectName(), instance.Id())
		if err != nil {
			mc.Logger().Error(fmt.Sprintf("failed to create session for reading records"))
			e = errors.WithStack(err)
			return
		}

		recordCount := session.RecordCount()
		mc.Logger().Info(fmt.Sprintf("record count: %d", recordCount))

		e = mc.sendRecordToWriter(session, recordCount, w)
	}(w)
	return r, e
}

func (mc *MaxcomputeSource) sendRecordToWriter(session *tunnel.InstanceResultDownloadSession, recordCount int, w io.WriteCloser) error {
	// read records
	i := 0
	step := 1000 // batch size for reading records
	for i < recordCount {
		reader, err := session.OpenRecordReader(i, step, 0, nil)
		if err != nil {
			mc.Logger().Error(fmt.Sprintf("failed to open record reader"))
			return errors.WithStack(err)
		}
		defer reader.Close()

		count := 0
		for {
			record, err := reader.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				mc.Logger().Error(fmt.Sprintf("failed to read record"))
				return errors.WithStack(err)
			}

			// process record
			mc.Logger().Debug(fmt.Sprintf("record: %s", record))
			v, err := fromRecord(mc.Logger(), record, session.Schema())
			if err != nil {
				mc.Logger().Error(fmt.Sprintf("failed to process record"))
				return errors.WithStack(err)
			}
			raw, err := json.Marshal(v)
			if err != nil {
				return errors.WithStack(err)
			}
			w.Write(append(raw, byte('\n')))
			count++
		}
		i += count
		mc.Logger().Info(fmt.Sprintf("send %d records to writer", count))
	}
	return nil
}
