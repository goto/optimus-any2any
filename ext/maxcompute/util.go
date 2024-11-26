package maxcompute

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/pkg/errors"
)

func getTable(client *odps.Odps, tableID string) (*odps.Table, error) {
	parts := strings.Split(tableID, ".")
	if len(parts) != 3 {
		return nil, errors.WithStack(fmt.Errorf("tableID must be in the format of project_name.schema_name.table_name, found %s", tableID))
	}
	return odps.NewTable(client, parts[0], parts[1], parts[2]), nil
}

func createRecord(b []byte, schema tableschema.TableSchema) (data.Record, error) {
	raw := map[string]interface{}{}
	err := json.Unmarshal(b, &raw)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if len(raw) != len(schema.Columns) {
		return nil, errors.WithStack(fmt.Errorf("record length mismatch: %d != %d", len(raw), len(schema.Columns)))
	}

	result := []data.Data{}
	for _, column := range schema.Columns {
		var d data.Data
		// TODO: handle other types
		switch column.Type.ID() {
		case datatype.STRING:
			curr, ok := raw[column.Name].(string)
			if !ok {
				return nil, errors.WithStack(fmt.Errorf("column %s is not a string, found %+v", column.Name, raw[column.Name]))
			}
			d = data.String(curr)
		}
		result = append(result, d)
	}

	return result, nil
}
