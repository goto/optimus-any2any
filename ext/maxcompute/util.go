package maxcompute

import (
	"encoding/json"
	errs "errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/pkg/errors"
)

const (
	ISONonStandardDateTimeFormat = "2006-01-02T15:04:05.000-0700"
)

func insertOverwrite(client *odps.Odps, destinationTableID, sourceTableID string) error {
	table, err := getTable(client, destinationTableID)
	if err != nil {
		return errors.WithStack(err)
	}
	orderedColumns := []string{}
	for _, column := range table.Schema().Columns {
		orderedColumns = append(orderedColumns, column.Name)
	}

	instance, err := client.ExecSQl(fmt.Sprintf("INSERT OVERWRITE TABLE %s SELECT %s FROM %s;", destinationTableID, strings.Join(orderedColumns, ","), sourceTableID))
	if err != nil {
		return errors.WithStack(err)
	}
	if err := instance.WaitForSuccess(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func dropTable(client *odps.Odps, tableID string) error {
	// save current project and schema
	currProject := client.DefaultProjectName()
	currSchema := client.CurrentSchemaName()
	defer func() {
		// restore current project and schema
		client.SetDefaultProjectName(currProject)
		client.SetCurrentSchemaName(currSchema)
	}()

	splittedTableID := strings.Split(tableID, ".")
	if len(splittedTableID) != 3 {
		err := errors.Errorf("invalid tableID (tableID should be in format project.schema.table): %s", tableID)
		return errors.WithStack(err)
	}
	project, schema, name := splittedTableID[0], splittedTableID[1], splittedTableID[2]

	// set project and schema to the table
	client.SetDefaultProjectName(project)
	client.SetCurrentSchemaName(schema)

	return client.Tables().Delete(name, true)
}

func createTable(client *odps.Odps, tableID string, tableIDReference string) error {
	// save current project and schema
	currProject := client.DefaultProjectName()
	currSchema := client.CurrentSchemaName()
	defer func() {
		// restore current project and schema
		client.SetDefaultProjectName(currProject)
		client.SetCurrentSchemaName(currSchema)
	}()

	splittedTableID := strings.Split(tableID, ".")
	if len(splittedTableID) != 3 {
		err := errors.Errorf("invalid tableID (tableID should be in format project.schema.table): %s", tableID)
		return errors.WithStack(err)
	}
	project, schema, name := splittedTableID[0], splittedTableID[1], splittedTableID[2]

	// set project and schema to the table
	client.SetDefaultProjectName(project)
	client.SetCurrentSchemaName(schema)

	// get table reference
	tableReference, err := getTable(client, tableIDReference)
	if err != nil {
		return errors.WithStack(err)
	}

	tempSchema := tableschema.NewSchemaBuilder().
		Columns(tableReference.Schema().Columns...).
		Build()
	tempSchema.TableName = name

	// create table
	return client.Tables().Create(tempSchema, true, map[string]string{}, map[string]string{})
}

func getTable(client *odps.Odps, tableID string) (*odps.Table, error) {
	// save current project and schema
	currProject := client.DefaultProjectName()
	currSchema := client.CurrentSchemaName()
	defer func() {
		// restore current project and schema
		client.SetDefaultProjectName(currProject)
		client.SetCurrentSchemaName(currSchema)
	}()

	splittedTableID := strings.Split(tableID, ".")
	if len(splittedTableID) != 3 {
		return nil, errors.Errorf("invalid tableID (tableID should be in format project.schema.table): %s", tableID)
	}
	project, schema, name := splittedTableID[0], splittedTableID[1], splittedTableID[2]

	// set project and schema to the table
	client.SetDefaultProjectName(project)
	client.SetCurrentSchemaName(schema)

	// get table
	table := client.Tables().Get(name)
	if err := table.Load(); err != nil {
		return nil, errors.WithStack(err)
	}
	return table, nil
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
		if value == nil {
			return data.TinyInt(0), nil
		}
		curr, ok := value.(int8)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a tinyint, found %+v, type %T", value, value))
		}
		return data.TinyInt(curr), nil
	case datatype.SMALLINT:
		if value == nil {
			return data.SmallInt(0), nil
		}
		curr, ok := value.(int16)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a smallint, found %+v, type %T", value, value))
		}
		return data.SmallInt(curr), nil
	case datatype.INT:
		if value == nil {
			return data.Int(0), nil
		}
		curr, ok := value.(int32)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not an int, found %+v, type %T", value, value))
		}
		return data.Int(curr), nil
	case datatype.BIGINT:
		if value == nil {
			return data.BigInt(0), nil
		}
		curr, ok := value.(int64)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a bigint, found %+v, type %T", value, value))
		}
		return data.BigInt(curr), nil
	case datatype.DECIMAL:
		if value == nil {
			// get decimal precision and scale
			decimalType, ok := dt.(datatype.DecimalType)
			if !ok {
				return nil, errors.WithStack(fmt.Errorf("dt is not a decimal"))
			}
			return data.NewDecimal(int(decimalType.Precision), int(decimalType.Scale), "0"), nil
		}

		var curr float64
		switch v := value.(type) {
		case string:
			// Parse decimal string to float
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, errors.WithStack(fmt.Errorf("failed to parse decimal string %s: %w", v, err))
			}
			curr = float64(f)
		case float64:
			curr = v
		case float32:
			curr = float64(v)
		default:
			return nil, errors.WithStack(fmt.Errorf("unsupported decimal type %T with value %+v", value, value))
		}
		// get decimal precision and scale
		decimalType, ok := dt.(datatype.DecimalType)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("dt is not a decimal"))
		}
		return data.NewDecimal(int(decimalType.Precision), int(decimalType.Scale), fmt.Sprintf("%f", curr)), nil
	case datatype.FLOAT, datatype.DOUBLE:
		if value == nil {
			if dt.ID() == datatype.FLOAT {
				return data.Float(0), nil
			}
			return data.Double(0), nil
		}

		curr, ok := value.(float64)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a float64, found %+v, type %T", value, value))
		}

		if dt.ID() == datatype.FLOAT {
			return data.Float(curr), nil
		}
		return data.Double(curr), nil
	case datatype.BINARY:
		if value == nil {
			return data.Binary(nil), nil
		}
		curr, ok := value.([]byte)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a binary, found %+v, type %T", value, value))
		}
		return data.Binary(curr), nil
	case datatype.BOOLEAN:
		if value == nil {
			return data.Bool(false), nil
		}
		curr, ok := value.(bool)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a boolean, found %+v, type %T", value, value))
		}
		return data.Bool(curr), nil
	case datatype.DATE, datatype.DATETIME, datatype.TIMESTAMP, datatype.TIMESTAMP_NTZ, datatype.STRING, datatype.CHAR, datatype.VARCHAR:
		if value == nil {
			switch dt.ID() {
			case datatype.DATE:
				return data.NewDate("0001-01-01")
			case datatype.DATETIME:
				return data.NewDateTime("0001-01-01 00:00:00")
			case datatype.TIMESTAMP:
				return data.NewTimestamp("0001-01-01 00:00:00.000")
			case datatype.TIMESTAMP_NTZ:
				return data.NewTimestampNtz("0001-01-01 00:00:00.000")
			case datatype.CHAR:
				return data.NewChar(dt.(datatype.CharType).Length, "")
			case datatype.VARCHAR:
				return data.NewVarChar(dt.(datatype.VarcharType).Length, "")
			}
			return data.String(""), nil
		}
		curr, ok := value.(string)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a string, found %+v, type %T", value, value))
		}
		switch dt.ID() {
		case datatype.DATE:
			return data.NewDate(curr)
		case datatype.DATETIME:
			result, err := data.NewDateTime(curr)
			if err != nil {
				// try to parse with RFC3339 and ISONonStandardDateTimeFormat
				t, err1 := parseTime(curr)
				if err1 != nil {
					err = errs.Join(err, err1)
					err = errs.Join(err, errors.Errorf("failed to parse datetime: %s", curr))
					return nil, errors.WithStack(err)
				}
				return data.DateTime(t), nil
			}
			return result, nil
		case datatype.TIMESTAMP:
			result, err := data.NewTimestamp(curr)
			if err != nil {
				// try to parse with RFC3339 and ISONonStandardDateTimeFormat
				t, err1 := parseTime(curr)
				if err1 != nil {
					err = errs.Join(err, err1)
					err = errs.Join(err, errors.Errorf("failed to parse datetime: %s", curr))
					return nil, errors.WithStack(err)
				}
				return data.Timestamp(t), nil
			}
			return result, nil
		case datatype.TIMESTAMP_NTZ:
			result, err := data.NewTimestampNtz(curr)
			if err != nil {
				// try to parse with RFC3339 and ISONonStandardDateTimeFormat
				t, err1 := parseTime(curr)
				if err1 != nil {
					err = errs.Join(err, err1)
					err = errs.Join(err, errors.Errorf("failed to parse datetime: %s", curr))
					return nil, errors.WithStack(err)
				}
				return data.TimestampNtz(t), nil
			}
			return result, nil
		case datatype.CHAR:
			return data.NewChar(dt.(datatype.CharType).Length, curr)
		case datatype.VARCHAR:
			return data.NewVarChar(dt.(datatype.VarcharType).Length, curr)
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

func parseTime(curr string) (time.Time, error) {
	var e error
	// try to parse with ISO non-standard format
	t, err := time.Parse(ISONonStandardDateTimeFormat, curr)
	if err != nil {
		e = errs.Join(e, err)
	} else {
		return t, nil
	}
	// try to parse with RFC3339
	t, err = time.Parse(time.RFC3339, curr)
	if err != nil {
		e = errs.Join(e, err)
	} else {
		return t, nil
	}
	// try to parse with TimeStampFormat
	t, err = time.Parse(data.TimeStampFormat, curr)
	if err != nil {
		e = errs.Join(e, err)
	} else {
		return t, nil
	}
	// try to parse with DateTimeFormat
	t, err = time.Parse(data.DateTimeFormat, curr)
	if err != nil {
		e = errs.Join(e, err)
	} else {
		return t, nil
	}
	// try to parse with DateFormat
	t, err = time.Parse(data.DateFormat, curr)
	if err != nil {
		e = errs.Join(e, err)
		return time.Time{}, errors.WithStack(e)
	}
	return t, nil
}
