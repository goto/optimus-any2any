package maxcompute

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"maps"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
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
	query           string
}

var _ flow.Source = (*MaxcomputeSource)(nil)

// NewSource creates a new MaxcomputeSource.
func NewSource(l *slog.Logger, creds string, queryFilePath string, executionProject string, additionalHints map[string]string, opts ...common.Option) (*MaxcomputeSource, error) {
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

	// read query from file
	raw, err := os.ReadFile(queryFilePath)
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
	if strings.Contains(string(raw), ";") {
		hints["odps.sql.submit.mode"] = "script"
	}

	mc := &MaxcomputeSource{
		Source:          commonSource,
		client:          client,
		tunnel:          t,
		additionalHints: hints,
		query:           string(raw),
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
	// run query
	mc.Logger.Info(fmt.Sprintf("source(mc): running query: %s", mc.query))
	instance, err := mc.client.ExecSQl(mc.query, mc.additionalHints)
	if err != nil {
		mc.Logger.Error(fmt.Sprintf("source(mc): failed to run query: %s", mc.query))
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
			mc.Send(raw)
			count++
		}
		i += count
		mc.Logger.Info(fmt.Sprintf("source(mc): send %d records", count))
	}
}
