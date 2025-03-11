package maxcompute

import (
	"encoding/json"
	errs "errors"
	"fmt"
	"log/slog"
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

func insertOverwrite(l *slog.Logger, client *odps.Odps, destinationTableID, sourceTableID string) error {
	table, err := getTable(l, client, destinationTableID)
	if err != nil {
		return errors.WithStack(err)
	}
	orderedColumns := []string{}
	for _, column := range table.Schema().Columns {
		orderedColumns = append(orderedColumns, column.Name)
	}

	queryToExecute := fmt.Sprintf("INSERT OVERWRITE TABLE %s SELECT %s FROM %s;", destinationTableID, strings.Join(orderedColumns, ","), sourceTableID)
	l.Info(fmt.Sprintf("sink(mc): executing query: %s", queryToExecute))
	instance, err := client.ExecSQl(queryToExecute)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := instance.WaitForSuccess(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func dropTable(l *slog.Logger, client *odps.Odps, tableID string) error {
	// save current schema
	currSchema := client.CurrentSchemaName()
	defer func() {
		// restore current schema
		client.SetCurrentSchemaName(currSchema)
	}()

	splittedTableID := strings.Split(tableID, ".")
	if len(splittedTableID) != 3 {
		err := errors.Errorf("invalid tableID (tableID should be in format project.schema.table): %s", tableID)
		return errors.WithStack(err)
	}
	project, schema, name := splittedTableID[0], splittedTableID[1], splittedTableID[2]

	// set schema to the table
	client.SetCurrentSchemaName(schema)

	l.Debug(fmt.Sprintf("sink(mc): dropping table: %s", tableID))
	return odpsDropTable(l, client, project, schema, name)
}

// substitution from (ts *Tables) Delete() function
func odpsDropTable(l *slog.Logger, client *odps.Odps, project, schema, name string) error {
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString("drop table")
	sqlBuilder.WriteString(" if exists")
	sqlBuilder.WriteRune(' ')
	sqlBuilder.WriteString(project)
	sqlBuilder.WriteRune('.')
	sqlBuilder.WriteString("`" + schema + "`")
	sqlBuilder.WriteRune('.')
	sqlBuilder.WriteString("`" + name + "`")
	sqlBuilder.WriteString(";")

	sqlTask := odps.NewSqlTask("SQLDropTableTask", sqlBuilder.String(), map[string]string{
		"odps.namespace.schema": "true",
	})

	l.Debug(fmt.Sprintf("sink(mc): dropping table: %s.%s.%s", project, schema, name))
	instances := odps.NewInstances(client, client.DefaultProjectName())
	i, err := instances.CreateTask(client.DefaultProjectName(), &sqlTask)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(i.WaitForSuccess())
}

func createTempTable(l *slog.Logger, client *odps.Odps, tableID string, tableIDReference string, lifecycleInDays int) error {
	// save current schema
	currSchema := client.CurrentSchemaName()
	defer func() {
		// restore current schema
		client.SetCurrentSchemaName(currSchema)
	}()

	splittedTableID := strings.Split(tableID, ".")
	if len(splittedTableID) != 3 {
		err := errors.Errorf("invalid tableID (tableID should be in format project.schema.table): %s", tableID)
		return errors.WithStack(err)
	}
	project, schema, name := splittedTableID[0], splittedTableID[1], splittedTableID[2]

	// set schema to the table
	client.SetCurrentSchemaName(schema)

	// get table reference
	tableReference, err := getTable(l, client, tableIDReference)
	if err != nil {
		return errors.WithStack(err)
	}

	tempSchema := tableschema.NewSchemaBuilder().
		Columns(tableReference.Schema().Columns...).
		Build()
	tempSchema.TableName = name
	tempSchema.Lifecycle = lifecycleInDays

	// create table
	l.Debug(fmt.Sprintf("sink(mc): creating temporary table: %s", tableID))

	return odpsCreateTable(l, client, project, schema, tempSchema)
}

// substitution from (ts *Tables) Create() function
func odpsCreateTable(l *slog.Logger, client *odps.Odps, project, schema string, tableSchema tableschema.TableSchema) error {
	sql, err := tableSchema.ToSQLString(project, schema, true)
	if err != nil {
		return errors.WithStack(err)
	}

	task := odps.NewSqlTask("SQLCreateTableTask", sql, map[string]string{
		"odps.namespace.schema": "true",
	})

	l.Debug(fmt.Sprintf("sink(mc): creating table: %s.%s.%s", project, schema, tableSchema.TableName))
	instances := odps.NewInstances(client, client.DefaultProjectName())
	ins, err := instances.CreateTask(client.DefaultProjectName(), &task)
	if err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(ins.WaitForSuccess())
}

func getTable(l *slog.Logger, client *odps.Odps, tableID string) (*odps.Table, error) {
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
	l.Debug(fmt.Sprintf("sink(mc): getting table: %s", tableID))
	table := client.Tables().Get(name)
	if err := table.Load(); err != nil {
		return nil, errors.WithStack(err)
	}
	return table, nil
}

func fromRecord(l *slog.Logger, record data.Record, schema tableschema.TableSchema) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	if record.Len() != len(schema.Columns) {
		return nil, errors.WithStack(fmt.Errorf("record length does not match schema column length"))
	}
	for i, d := range record {
		val, err := fromData(l, d)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		m[schema.Columns[i].Name] = val
	}
	return m, nil
}

func fromData(l *slog.Logger, d data.Data) (interface{}, error) {
	if d == nil {
		return nil, nil
	}
	switch d.Type().ID() {
	case datatype.TINYINT:
		val, ok := d.(data.TinyInt)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected tinyint, got %T", d))
		}
		return int8(val), nil
	case datatype.SMALLINT:
		val, ok := d.(data.SmallInt)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected smallint, got %T", d))
		}
		return int16(val), nil
	case datatype.INT:
		val, ok := d.(data.Int)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected int, got %T", d))
		}
		return int32(val), nil
	case datatype.BIGINT:
		val, ok := d.(data.BigInt)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected bigint, got %T", d))
		}
		return int64(val), nil
	case datatype.FLOAT:
		val, ok := d.(data.Float)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected float, got %T", d))
		}
		return float32(val), nil
	case datatype.DOUBLE:
		val, ok := d.(data.Double)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected double, got %T", d))
		}
		return float64(val), nil
	case datatype.DECIMAL:
		val, ok := d.(data.Decimal)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected decimal, got %T", d))
		}
		return val.String(), nil
	case datatype.CHAR:
		val, ok := d.(data.Char)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected char, got %T", d))
		}
		return val.String(), nil
	case datatype.VARCHAR:
		val, ok := d.(data.VarChar)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected varchar, got %T", d))
		}
		return val.String(), nil
	case datatype.STRING:
		val, ok := d.(data.String)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected string, got %T", d))
		}
		return val.String(), nil
	case datatype.BINARY:
		val, ok := d.(data.Binary)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected binary, got %T", d))
		}
		return []byte(val), nil
	case datatype.BOOLEAN:
		val, ok := d.(data.Bool)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected boolean, got %T", d))
		}
		return bool(val), nil
	case datatype.DATE:
		val, ok := d.(data.Date)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected date, got %T", d))
		}
		return val.Time().Format(data.DateFormat), nil
	case datatype.DATETIME:
		val, ok := d.(data.DateTime)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected datetime, got %T", d))
		}
		return val.Time().Format(data.DateTimeFormat), nil
	case datatype.TIMESTAMP:
		val, ok := d.(data.Timestamp)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected timestamp, got %T", d))
		}
		return val.Time().Format(data.TimeStampFormat), nil
	case datatype.TIMESTAMP_NTZ:
		val, ok := d.(data.TimestampNtz)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected timestamp_ntz, got %T", d))
		}
		return val.Time().Format(data.TimeStampFormat), nil
	case datatype.ARRAY:
		val, ok := d.(data.Array)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected array, got %T", d))
		}
		arr := []interface{}{}
		for _, currData := range val.ToSlice() {
			curr, err := fromData(l, currData)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			arr = append(arr, curr)
		}
		return arr, nil
	case datatype.STRUCT:
		val, ok := d.(data.Struct)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected struct, got %T", d))
		}
		m := map[string]interface{}{}
		for _, field := range val.Fields() {
			curr, err := fromData(l, field.Value)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			m[field.Name] = curr
		}
		return m, nil
	default:
		return nil, errors.WithStack(fmt.Errorf("unsupported data type: %s", d.Type().ID()))
	}
}

func lowerCaseMapKeys(m map[string]interface{}) map[string]interface{} {
	result := map[string]interface{}{}
	for k, v := range m {
		result[strings.ToLower(k)] = v
	}
	return result
}

func createRecord(l *slog.Logger, b []byte, schema tableschema.TableSchema) (data.Record, error) {
	raw := map[string]interface{}{}
	err := json.Unmarshal(b, &raw)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	raw = lowerCaseMapKeys(raw)

	result := []data.Data{}
	for _, column := range schema.Columns {
		d, err := createData(l, raw[strings.ToLower(column.Name)], column.Type)
		if err != nil {
			err = errors.Wrapf(err, "failed to create data for column %s on record: %+v", column.Name, raw)
			return nil, errors.WithStack(err)
		}
		result = append(result, d)
	}

	return result, nil
}

func createData(l *slog.Logger, value interface{}, dt datatype.DataType) (data.Data, error) {
	if value == nil {
		return data.Null, nil
	}
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
	case datatype.DECIMAL:
		var curr float64
		switch v := value.(type) {
		case string:
			return data.DecimalFromStr(v)
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
		curr, ok := value.(float64)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a float64, found %+v, type %T", value, value))
		}

		if dt.ID() == datatype.FLOAT {
			return data.Float(curr), nil
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

		switch dt.ID() {
		case datatype.DATE, datatype.DATETIME, datatype.TIMESTAMP, datatype.TIMESTAMP_NTZ:
			t, err := parseTime(l, curr)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			switch dt.ID() {
			case datatype.DATE:
				return data.Date(t), nil
			case datatype.DATETIME:
				return data.DateTime(t), nil
			case datatype.TIMESTAMP:
				return data.Timestamp(t), nil
			case datatype.TIMESTAMP_NTZ:
				return data.TimestampNtz(t), nil
			}
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
			d, err := createData(l, v, arrayType.ElementType)
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
		curr = lowerCaseMapKeys(curr)

		record := data.NewStructWithTyp(structType)
		for _, field := range structType.Fields {
			var d interface{} = nil

			if currValue, ok := curr[strings.ToLower(field.Name)]; ok && currValue != nil {
				v, err := createData(l, currValue, field.Type)
				if err != nil {
					return nil, errors.WithStack(err)
				}
				d = v
			}

			if err := record.SetField(field.Name, d); err != nil {
				return nil, errors.WithStack(err)
			}
		}
		return record, nil
	}
	return nil, errors.WithStack(fmt.Errorf("unsupported column type: %s", dt.ID()))
}

func parseTime(l *slog.Logger, curr string) (time.Time, error) {
	var e error
	// try to parse with ISO non-standard format
	l.Debug(fmt.Sprintf("sink(mc): trying to parse time with ISO non-standard format: %s", curr))
	t, err := time.Parse(ISONonStandardDateTimeFormat, curr)
	if err != nil {
		e = errs.Join(e, err)
	} else {
		return t, nil
	}
	// try to parse with RFC3339
	l.Debug(fmt.Sprintf("sink(mc): trying to parse time with RFC3339 format: %s", curr))
	t, err = time.Parse(time.RFC3339, curr)
	if err != nil {
		e = errs.Join(e, err)
	} else {
		return t, nil
	}
	// try to parse with TimeStampFormat
	l.Debug(fmt.Sprintf("sink(mc): trying to parse time with TimeStampFormat: %s", curr))
	t, err = time.Parse(data.TimeStampFormat, curr)
	if err != nil {
		e = errs.Join(e, err)
	} else {
		return t, nil
	}
	// try to parse with DateTimeFormat
	l.Debug(fmt.Sprintf("sink(mc): trying to parse time with DateTimeFormat: %s", curr))
	t, err = time.Parse(data.DateTimeFormat, curr)
	if err != nil {
		e = errs.Join(e, err)
	} else {
		return t, nil
	}
	// try to parse with DateFormat
	l.Debug(fmt.Sprintf("sink(mc): trying to parse time with DateFormat: %s", curr))
	t, err = time.Parse(data.DateFormat, curr)
	if err != nil {
		e = errs.Join(e, err)
		return time.Time{}, errors.WithStack(e)
	}
	return t, nil
}
