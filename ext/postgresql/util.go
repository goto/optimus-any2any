package postgresql

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

func checkSchemaValidity(l *slog.Logger, tableColumns, headers []string) error {
	if len(tableColumns) != len(headers) {
		l.Warn(fmt.Sprintf("table columns and csv headers do not match: %d != %d, %d columns will have null value", len(tableColumns), len(headers), len(tableColumns)-len(headers)))
		l.Debug(fmt.Sprintf("table columns: %v", tableColumns))
		l.Debug(fmt.Sprintf("record headers: %v", headers))
	}

	tableColumnMap := make(map[string]bool)
	for _, column := range tableColumns {
		tableColumnMap[strings.ToLower(column)] = true
	}

	for _, field := range headers {
		if _, exists := tableColumnMap[strings.ToLower(field)]; !exists {
			l.Error(fmt.Sprintf("field '%s' in CSV does not match any column in the table", field))
			l.Error(fmt.Sprintf("table columns: %v", tableColumns))
			l.Error(fmt.Sprintf("record headers: %v", headers))
			return errors.New(fmt.Sprintf("field '%s' does not match any column in the table", field))
		}
	}
	return nil
}

// getTableColumns retrieves the column names of a specified table in PostgreSQL.
func getTableColumns(ctx context.Context, l *slog.Logger, conn *pgx.Conn, tableName string) ([]string, error) {
	query := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position", tableName)
	if len(strings.Split(tableName, ".")) > 1 {
		schemaName := strings.Split(tableName, ".")[0]
		tableName = strings.Split(tableName, ".")[1]
		query = fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' ORDER BY ordinal_position", schemaName, tableName)
	}

	rows, err := conn.Query(ctx, query)
	if err != nil {
		l.Error(fmt.Sprintf("error querying table columns: %s", err.Error()))
		return nil, errors.WithStack(err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			l.Error(fmt.Sprintf("error scanning column: %s", err.Error()))
			return nil, errors.WithStack(err)
		}
		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		l.Error(fmt.Sprintf("error scanning rows: %s", err.Error()))
		return nil, errors.WithStack(err)
	}

	return columns, nil
}

// getCSVHeaders reads the first line of a CSV file to extract the headers.
func getCSVHeaders(l *slog.Logger, r io.ReadSeeker) ([]string, error) {
	// Reset the reader to the beginning
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}

	// Read the first line to get headers
	csvReader := csv.NewReader(r)
	headers, err := csvReader.Read()
	if err != nil {
		l.Error(fmt.Sprintf("failed to read csv headers: %v", err))
		return nil, errors.WithStack(err)
	}

	// Reset the reader again to the beginning for further processing
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}

	return headers, nil
}
