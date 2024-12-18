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
		d, err := createData(raw[column.Name], column.Type)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		result = append(result, d)
	}

	return result, nil
}

func createData(value interface{}, dt datatype.DataType) (data.Data, error) {
	switch dt.ID() {
	case datatype.TINYINT:
		curr, ok := value.(int8)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a tinyint, found %+v, type %T", value, value))
		}
		return data.TinyInt(curr), nil
	case datatype.SMALLINT:
		curr, ok := value.(int16)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a smallint, found %+v, type %T", value, value))
		}
		return data.SmallInt(curr), nil
	case datatype.INT:
		curr, ok := value.(int32)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not an int, found %+v, type %T", value, value))
		}
		return data.Int(curr), nil
	case datatype.BIGINT:
		curr, ok := value.(int64)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a bigint, found %+v, type %T", value, value))
		}
		return data.BigInt(curr), nil
	case datatype.DECIMAL, datatype.FLOAT:
		curr, ok := value.(float32)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a float64, found %+v, type %T", value, value))
		}
		return data.Float(curr), nil
	case datatype.DOUBLE:
		curr, ok := value.(float64)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a float64, found %+v, type %T", value, value))
		}
		return data.Double(curr), nil
	case datatype.BINARY:
		curr, ok := value.([]byte)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a binary, found %+v, type %T", value, value))
		}
		return data.Binary(curr), nil
	case datatype.BOOLEAN:
		curr, ok := value.(bool)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a boolean, found %+v, type %T", value, value))
		}
		return data.Bool(curr), nil
	case datatype.DATE, datatype.DATETIME, datatype.TIMESTAMP, datatype.TIMESTAMP_NTZ, datatype.STRING, datatype.CHAR, datatype.VARCHAR:
		curr, ok := value.(string)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a string, found %+v, type %T", value, value))
		}
		return data.String(curr), nil
	case datatype.ARRAY:
		arrayType, ok := dt.(datatype.ArrayType)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("dt is not an array"))
		}
		curr, ok := value.([]interface{})
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not an array, found %+v, type %T", value, value))
		}
		array := data.NewArrayWithType(arrayType)
		for _, v := range curr {
			d, err := createData(v, arrayType.ElementType)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			array.Append(d)
		}
		return array, nil
	case datatype.STRUCT:
		structType, ok := dt.(datatype.StructType)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("dt is not a struct"))
		}
		curr, ok := value.(map[string]interface{})
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a struct, found %+v, type %T", value, value))
		}
		record := data.NewStructWithTyp(structType)
		for _, field := range structType.Fields {
			if _, ok := curr[field.Name]; !ok {
				return nil, errors.WithStack(fmt.Errorf("field %s is missing", field.Name))
			}
			d, err := createData(curr[field.Name], field.Type)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			if err := record.SetField(field.Name, d); err != nil {
				return nil, errors.WithStack(err)
			}
		}
		return record, nil
	}
	return nil, errors.WithStack(fmt.Errorf("unsupported column type: %s", dt.ID()))
}
