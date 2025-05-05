package maxcompute

import (
	errs "errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/goccy/go-json"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/pkg/errors"
)

var (
	ISONonStandardDateTimeFormats = []string{"2006-01-02T15:04:05.000-0700", "2006-01-02 15:04:05 MST"}
)

var (
	// reserved keywords https://www.alibabacloud.com/help/en/maxcompute/user-guide/reserved-words-and-keywords
	reservedKeywordsList = []string{
		"add", "after", "all", "alter", "analyze", "and", "archive", "array", "as", "asc",
		"before", "between", "bigint", "binary", "blob", "boolean", "both", "decimal",
		"bucket", "buckets", "by", "cascade", "case", "cast", "cfile", "change", "cluster",
		"clustered", "clusterstatus", "collection", "column", "columns", "comment", "compute",
		"concatenate", "continue", "create", "cross", "current", "cursor", "data", "database",
		"databases", "date", "datetime", "dbproperties", "deferred", "delete", "delimited",
		"desc", "describe", "directory", "disable", "distinct", "distribute", "double", "drop",
		"else", "enable", "end", "except", "escaped", "exclusive", "exists", "explain", "export",
		"extended", "external", "false", "fetch", "fields", "fileformat", "first", "float",
		"following", "format", "formatted", "from", "full", "function", "functions", "grant",
		"group", "having", "hold_ddltime", "idxproperties", "if", "import", "in", "index",
		"indexes", "inpath", "inputdriver", "inputformat", "insert", "int", "intersect", "into",
		"is", "items", "join", "keys", "lateral", "left", "lifecycle", "like", "limit", "lines",
		"load", "local", "location", "lock", "locks", "long", "map", "mapjoin", "materialized",
		"minus", "msck", "not", "no_drop", "null", "of", "offline", "offset", "on", "option",
		"or", "order", "out", "outer", "outputdriver", "outputformat", "over", "overwrite",
		"partition", "partitioned", "partitionproperties", "partitions", "percent", "plus",
		"preceding", "preserve", "procedure", "purge", "range", "rcfile", "read", "readonly",
		"reads", "rebuild", "recordreader", "recordwriter", "reduce", "regexp", "rename",
		"repair", "replace", "restrict", "revoke", "right", "rlike", "row", "rows", "schema",
		"schemas", "select", "semi", "sequencefile", "serde", "serdeproperties", "set", "shared",
		"show", "show_database", "smallint", "sort", "sorted", "ssl", "statistics", "status",
		"stored", "streamtable", "string", "struct", "table", "tables", "tablesample",
		"tblproperties", "temporary", "terminated", "textfile", "then", "timestamp", "tinyint",
		"to", "touch", "transform", "trigger", "true", "type", "unarchive", "unbounded", "undo",
		"union", "uniontype", "uniquejoin", "unlock", "unsigned", "update", "use", "using",
		"utc", "utc_timestamp", "view", "when", "where", "while", "div",
	}
	reservedKeywords map[string]bool
)

func init() {
	reservedKeywords = make(map[string]bool)
	for _, keyword := range reservedKeywordsList {
		reservedKeywords[keyword] = true
	}
}

// generateLogView generates a log view for the given task instance
func generateLogView(l *slog.Logger, c *odps.Odps, taskIns *odps.Instance, logViewRetentionInDays int) (string, error) {
	u, err := c.LogView().GenerateLogView(taskIns, logViewRetentionInDays*24)
	if err != nil {
		return "", errors.WithStack(err)
	}
	l.Debug(fmt.Sprintf("origin log view url: %s", u))

	// change query parameter h to http://service.id-all.maxcompute.aliyun-inc.com
	parsedURL, err := url.Parse(u)
	if err != nil {
		return "", errors.WithStack(err)
	}
	q := parsedURL.Query()
	q.Set("h", "http://service.id-all.maxcompute.aliyun-inc.com/api")

	// reconstruct the URL with the new query parameter
	parsedURL.RawQuery = q.Encode()
	u = parsedURL.String()

	return u, nil
}

func insertOverwrite(l *slog.Logger, client *odps.Odps, destinationTableID, sourceTableID string) error {
	table, err := getTable(l, client, destinationTableID)
	if err != nil {
		return errors.WithStack(err)
	}
	orderedColumns := []string{}
	for _, column := range table.Schema().Columns {
		orderedColumns = append(orderedColumns, sanitizeName(column.Name))
	}

	queryToExecute := fmt.Sprintf("INSERT OVERWRITE TABLE %s SELECT %s FROM %s;", sanitizeTableID(destinationTableID), strings.Join(orderedColumns, ","), sanitizeTableID(sourceTableID))
	l.Info(fmt.Sprintf("executing query: %s", queryToExecute))
	instance, err := client.ExecSQl(queryToExecute)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := instance.WaitForSuccess(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func sanitizeName(columnName string) string {
	if reservedKeywords[columnName] {
		return fmt.Sprintf("`%s`", columnName)
	}
	return columnName
}

func sanitizeTableID(tableID string) string {
	splittedTableID := strings.Split(tableID, ".")
	for i, part := range splittedTableID {
		splittedTableID[i] = sanitizeName(part)
	}
	return strings.Join(splittedTableID, ".")
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

	l.Debug(fmt.Sprintf("dropping table: %s", tableID))
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

	l.Debug(fmt.Sprintf("dropping table: %s.%s.%s", project, schema, name))
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
	l.Debug(fmt.Sprintf("creating temporary table: %s", tableID))

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

	l.Debug(fmt.Sprintf("creating table: %s.%s.%s", project, schema, tableSchema.TableName))
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
	l.Debug(fmt.Sprintf("getting table: %s", tableID))
	table := client.Tables().Get(name)
	if err := table.Load(); err != nil {
		return nil, errors.WithStack(err)
	}
	return table, nil
}

func fromRecord(l *slog.Logger, record data.Record, schema tableschema.TableSchema) (*model.Record, error) {
	m := model.NewRecord()
	if record.Len() != len(schema.Columns) {
		return m, errors.WithStack(fmt.Errorf("record length does not match schema column length"))
	}
	for i, d := range record {
		val, err := fromData(l, d)
		if err != nil {
			return m, errors.WithStack(err)
		}
		m.Set(schema.Columns[i].Name, val)
	}
	return m, nil
}

func fromData(l *slog.Logger, d data.Data) (interface{}, error) {
	if d == nil {
		return nil, nil
	}
	switch d.Type().ID() {
	case datatype.TINYINT:
		val := data.TinyInt(0)
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return int8(val), nil
	case datatype.SMALLINT:
		val, ok := d.(data.SmallInt)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("expected smallint, got %T", d))
		}
		return int16(val), nil
	case datatype.INT:
		val := data.Int(0)
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return int32(val), nil
	case datatype.BIGINT:
		val := data.BigInt(0)
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return int64(val), nil
	case datatype.FLOAT:
		val := data.Float(0)
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return float32(val), nil
	case datatype.DOUBLE:
		val := data.Double(0)
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return float64(val), nil
	case datatype.DECIMAL:
		val := data.Decimal{}
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return val.String(), nil
	case datatype.CHAR:
		val := data.Char{}
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return val.String(), nil
	case datatype.VARCHAR:
		val := data.VarChar{}
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return val.String(), nil
	case datatype.STRING:
		val := data.String("")
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return val.String(), nil
	case datatype.BINARY:
		val := data.Binary{}
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return []byte(val), nil
	case datatype.BOOLEAN:
		val := data.Bool(false)
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return bool(val), nil
	case datatype.DATE:
		val := data.Date{}
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return val.Time().Format(data.DateFormat), nil
	case datatype.DATETIME:
		val := data.DateTime{}
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return val.Time().Format(data.DateTimeFormat), nil
	case datatype.TIMESTAMP:
		val := data.Timestamp{}
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return val.Time().Format(data.TimeStampFormat), nil
	case datatype.TIMESTAMP_NTZ:
		val := data.TimestampNtz{}
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}
		return val.Time().Format(data.TimeStampFormat), nil
	case datatype.ARRAY:
		val := data.Array{}
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}

		// convert array to slice
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
		val := data.Struct{}
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}

		// convert struct to map
		m := map[string]interface{}{}
		for _, field := range val.Fields() {
			curr, err := fromData(l, field.Value)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			m[field.Name] = curr
		}
		return m, nil
	case datatype.JSON:
		val := data.Json{}
		if err := val.Scan(d); err != nil {
			return nil, errors.WithStack(err)
		}

		// unquote JSON
		unquotedJSON, err := strconv.Unquote(val.GetData())
		if err != nil {
			return nil, errors.WithStack(err)
		}
		raw := []byte(unquotedJSON)

		// convert JSON to map
		m := map[string]interface{}{}
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, errors.WithStack(err)
		}
		return m, nil
	default:
		return nil, errors.WithStack(fmt.Errorf("unsupported data type: %s", d.Type().ID()))
	}
}

func lowerCaseMapKeys(m *model.Record) *model.Record {
	result := model.NewRecord()
	for k, v := range m.AllFromFront() {
		result.Set(strings.ToLower(k), v)
	}
	return result
}

func createRecord(l *slog.Logger, record *model.Record, schema tableschema.TableSchema) (data.Record, error) {
	raw := lowerCaseMapKeys(record)
	result := []data.Data{}
	var errResult error = nil
	for _, column := range schema.Columns {
		d, err := createData(l, raw.GetOrDefault(strings.ToLower(column.Name), nil), column.Type)
		if err != nil {
			err = errors.Wrapf(err, "failed to create data for column %s", column.Name)
			errResult = errs.Join(errResult, err)
		}
		result = append(result, d)
	}

	if errResult != nil {
		errResult = errors.WithMessagef(errResult, "failed to create record %+v", raw)
		return nil, errors.WithStack(errResult)
	}

	return result, nil
}

func createData(l *slog.Logger, value interface{}, dt datatype.DataType) (data.Data, error) {
	if value == nil {
		return data.Null, nil
	}
	switch dt.ID() {
	case datatype.TINYINT:
		var curr int8
		switch v := value.(type) {
		case int64:
			curr = int8(v)
		case int32:
			curr = int8(v)
		case int:
			curr = int8(v)
		case int16:
			curr = int8(v)
		case int8:
			curr = v
		case float32:
			curr = int8(v)
		case float64:
			curr = int8(v)
			return nil, errors.WithStack(fmt.Errorf("unsupported tinyint type %T with value %+v", value, value))
		}
		return data.TinyInt(curr), nil
	case datatype.SMALLINT:
		var curr int16
		switch v := value.(type) {
		case int64:
			curr = int16(v)
		case int32:
			curr = int16(v)
		case int:
			curr = int16(v)
		case int16:
			curr = v
		case int8:
			curr = int16(v)
		case float32:
			curr = int16(v)
		case float64:
			curr = int16(v)
		default:
			return nil, errors.WithStack(fmt.Errorf("unsupported smallint type %T with value %+v", value, value))
		}
		return data.SmallInt(curr), nil
	case datatype.INT:
		var curr int32
		switch v := value.(type) {
		case int64:
			curr = int32(v)
		case int32:
			curr = v
		case int:
			curr = int32(v)
		case int16:
			curr = int32(v)
		case int8:
			curr = int32(v)
		case float32:
			curr = int32(v)
		case float64:
			curr = int32(v)
		default:
			return nil, errors.WithStack(fmt.Errorf("unsupported int type %T with value %+v", value, value))
		}
		return data.Int(curr), nil
	case datatype.BIGINT:
		var curr int64
		switch v := value.(type) {
		case int64:
			curr = v
		case int32:
			curr = int64(v)
		case int:
			curr = int64(v)
		case int16:
			curr = int64(v)
		case int8:
			curr = int64(v)
		case float32:
			curr = int64(v)
		case float64:
			curr = int64(v)
		default:
			return nil, errors.WithStack(fmt.Errorf("unsupported bigint type %T with value %+v", value, value))
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
		if curr == "" {
			return createData(l, nil, dt)
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
		currRecord, ok := value.(*model.Record)
		if !ok {
			return nil, errors.WithStack(fmt.Errorf("value is not a struct, found %+v, type %T", value, value))
		}
		currRecord = lowerCaseMapKeys(currRecord)

		record := data.NewStructWithTyp(structType)
		for _, field := range structType.Fields {
			var d interface{} = nil

			if currValue := currRecord.GetOrDefault(strings.ToLower(field.Name), nil); currValue != nil {
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
	// try to parse with RFC3339
	l.Debug(fmt.Sprintf("trying to parse time with RFC3339 format: %s", curr))
	t, err := time.Parse(time.RFC3339, curr)
	if err != nil {
		e = errs.Join(e, err)
	} else {
		return t, nil
	}
	// try to parse with TimeStampFormat
	l.Debug(fmt.Sprintf("trying to parse time with TimeStampFormat: %s", curr))
	t, err = time.Parse(data.TimeStampFormat, curr)
	if err != nil {
		e = errs.Join(e, err)
	} else {
		return t, nil
	}
	// try to parse with DateTimeFormat
	l.Debug(fmt.Sprintf("trying to parse time with DateTimeFormat: %s", curr))
	t, err = time.Parse(data.DateTimeFormat, curr)
	if err != nil {
		e = errs.Join(e, err)
	} else {
		return t, nil
	}
	// try to parse with DateFormat
	l.Debug(fmt.Sprintf("trying to parse time with DateFormat: %s", curr))
	t, err = time.Parse(data.DateFormat, curr)
	if err != nil {
		e = errs.Join(e, err)
	} else {
		return t, nil
	}
	// try to parse with ISO non-standard format
	l.Debug(fmt.Sprintf("trying to parse time with ISO non-standard format: %s", curr))
	for _, format := range ISONonStandardDateTimeFormats {
		t, err := time.Parse(format, curr)
		if err != nil {
			e = errs.Join(e, err)
		} else {
			return t, nil
		}
	}

	return time.Time{}, errors.WithStack(e)
}
