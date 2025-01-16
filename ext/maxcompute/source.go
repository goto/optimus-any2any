package maxcompute

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/component/source"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"golang.org/x/exp/rand"
)

const (
	transitionTableColumnName = "json_value"
)

// MaxcomputeSource is the source component for MaxCompute.
type MaxcomputeSource struct {
	*source.CommonSource

	client          *odps.Odps
	session         *tunnel.DownloadSession
	query           string
	tableTransition *odps.Table
}

var _ flow.Source = (*MaxcomputeSource)(nil)

// NewSource creates a new MaxcomputeSource.
func NewSource(l *slog.Logger, svcAcc string, queryFilePath string, executionProject string, opts ...option.Option) (*MaxcomputeSource, error) {
	// create commonSource source
	commonSource := source.NewCommonSource(l, opts...)

	// create client for maxcompute
	client, err := NewClient(svcAcc)
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
	query := string(raw)

	// create temporary table
	tempTableID := fmt.Sprintf("optimus_mc_%s_temp_%d", generateRandomHex(), time.Now().Unix())
	if err := createTableFromSchema(client, tempTableID, tableschema.NewSchemaBuilder().
		Column(tableschema.Column{Name: transitionTableColumnName, Type: datatype.StringType}).
		Build(),
	); err != nil {
		return nil, errors.WithStack(err)
	}
	commonSource.Logger.Info(fmt.Sprintf("source(mc): temporary table created: %s", tempTableID))
	tempTable, err := getTable(client, tempTableID)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// create download session
	t, err := tunnel.NewTunnelFromProject(client.DefaultProject())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	session, err := t.CreateDownloadSession(tempTable.ProjectName(), tempTable.Name(), tunnel.SessionCfg.WithSchemaName(tempTable.SchemaName()))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	mc := &MaxcomputeSource{
		CommonSource:    commonSource,
		client:          client,
		session:         session,
		query:           query,
		tableTransition: tempTable,
	}

	// add clean function
	commonSource.AddCleanFunc(func() {
		commonSource.Logger.Debug("source(mc): cleaning up")
		commonSource.Logger.Info(fmt.Sprintf("source(mc): load method is replace, deleting temporary table: %s", mc.tableTransition.Name()))
		if err := dropTable(client, mc.tableTransition.Name()); err != nil {
			commonSource.Logger.Error(fmt.Sprintf("source(mc): delete temporary table error: %s", err.Error()))
			mc.SetError(errors.WithStack(err))
		}
	})

	commonSource.RegisterProcess(mc.process)

	return mc, nil
}

// process is the process function for MaxcomputeSource.
func (mc *MaxcomputeSource) process() {
	recordCount := mc.session.RecordCount()
	mc.Logger.Info(fmt.Sprintf("source(mc): record count: %d", recordCount))

	// read records
	i := 0
	step := 1000 // batch size for reading records
	for i < recordCount {
		reader, err := mc.session.OpenRecordReader(i, step, nil)
		if err != nil {
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
				mc.SetError(errors.WithStack(err))
				return
			}

			// process record
			mc.Logger.Debug(fmt.Sprintf("source(mc): record: %s", record))
			v, err := fromRecord(record, mc.tableTransition.Schema())
			if err != nil {
				mc.SetError(errors.WithStack(err))
				return
			}
			val, ok := v[transitionTableColumnName].(string)
			if !ok {
				err := fmt.Errorf("expected string, got %T", v[transitionTableColumnName])
				mc.SetError(errors.WithStack(err))
				return
			}
			raw, err := json.Marshal(val)
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

func generateRandomHex() string {
	const charset = "0123456789abcdef"
	const length = 4

	rand.Seed(uint64(time.Now().Unix()))
	var sb strings.Builder

	for i := 0; i < length; i++ {
		randomIndex := rand.Intn(len(charset))
		sb.WriteByte(charset[randomIndex])
	}

	return sb.String()
}
