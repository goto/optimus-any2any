package postgresql

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

func checkSchema(ctx context.Context, l *slog.Logger, conn *pgx.Conn, tableName string, csvReader io.ReadSeeker) error {
	// check the schema csv header and destination table
	tableColumns, err := getTableColumns(ctx, l, conn, tableName)
	if err != nil {
		l.Error(fmt.Sprintf("failed to get table columns"))
		return errors.WithStack(err)
	}
	headers, err := getCSVHeaders(l, csvReader)
	if err != nil {
		l.Error(fmt.Sprintf("failed to get csv headers"))
		return errors.WithStack(err)
	}

	if len(tableColumns) != len(headers) {
		l.Error(fmt.Sprintf("table columns and csv headers do not match: %d != %d", len(tableColumns), len(headers)))
		return errors.New("table columns and csv headers do not match")
	}
	for i, column := range tableColumns {
		if strings.ToLower(column) != strings.ToLower(headers[i]) {
			l.Error(fmt.Sprintf("table column %s does not match csv header %s", column, headers[i]))
			return errors.New("table columns and csv headers do not match")
		}
	}
	return nil
}

// getTableColumns retrieves the column names of a specified table in PostgreSQL.
func getTableColumns(ctx context.Context, l *slog.Logger, conn *pgx.Conn, tableName string) ([]string, error) {
	query := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position", tableName)
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

func getCSVHeaders(l *slog.Logger, r io.ReadSeeker) ([]string, error) {
	// Reset the reader to the beginning
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}

	// Read the first line to get headers
	var headerLine string
	if _, err := fmt.Fscanf(r, "%s\n", &headerLine); err != nil {
		l.Error(fmt.Sprintf("failed to read header line from CSV: %s", err.Error()))
		return nil, errors.WithStack(err)
	}

	headers := []string{}
	for _, header := range strings.Split(headerLine, ",") {
		headers = append(headers, header)
	}

	// Reset the reader again to the beginning for further processing
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, errors.WithStack(err)
	}

	return headers, nil
}
