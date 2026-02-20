package postgresql

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pkg/errors"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	xnet "github.com/goto/optimus-any2any/internal/net"
	"github.com/goto/optimus-any2any/pkg/flow"
)

// PGSource is a source that reads data from PostgreSQL.
type PGSource struct {
	common.Source

	pool  *pgxpool.Pool
	query string
}

var _ flow.Source = (*PGSource)(nil)

// NewSource creates a new PostgreSQL source.
func NewSource(commonSource common.Source, dsn, queryFilePath string, queryTemplateValues map[string]string, maxOpenConnection, minOpenConnection int32) (*PGSource, error) {
	// build pool config from DSN
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if maxOpenConnection > 0 {
		poolConfig.MaxConns = maxOpenConnection
	}
	if minOpenConnection > 0 {
		poolConfig.MinConns = minOpenConnection
	}

	// create connection pool
	pool, err := pgxpool.NewWithConfig(commonSource.Context(), poolConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// read query from file
	rawQuery, err := os.ReadFile(queryFilePath)
	if err != nil {
		pool.Close()
		return nil, errors.WithStack(err)
	}
	// compile query as golang template using provided values
	queryTemplate, err := compiler.NewTemplate("source_pg_query", string(rawQuery))
	if err != nil {
		pool.Close()
		return nil, errors.WithStack(err)
	}
	query, err := compiler.Compile(queryTemplate, queryTemplateValues)
	if err != nil {
		pool.Close()
		return nil, errors.WithStack(err)
	}
	query = strings.TrimSpace(query)
	if ok := IsSelectQuery(query); !ok {
		pool.Close()
		return nil, fmt.Errorf("non select statements not supported")
	}

	s := &PGSource{
		Source: commonSource,
		pool:   pool,
		query:  query,
	}

	// add clean func
	commonSource.AddCleanFunc(func() error {
		s.Logger().Info("close pg source pool")
		s.pool.Close()
		return nil
	})

	// register source process
	commonSource.RegisterProcess(s.process)

	return s, nil
}

func (s *PGSource) process() error {
	var rows pgx.Rows

	err := s.DryRunable(func() error {
		r, err := s.pool.Query(s.Context(), s.query)
		if err != nil {
			return errors.WithStack(err)
		}
		rows = r
		return nil
	}, func() error {
		address := fmt.Sprintf("%s:%d", s.pool.Config().ConnConfig.Host, s.pool.Config().ConnConfig.Port)
		s.Logger().Info(fmt.Sprintf("dry run will not run the query, generated query:\n%s", s.query))
		return errors.WithStack(xnet.ConnCheck(address))
	})
	if err != nil {
		return errors.WithStack(err)
	}
	if rows == nil {
		return nil
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return errors.WithStack(err)
		}

		record := model.NewRecord()
		for i, field := range fields {
			var value interface{}
			if i < len(values) {
				value = normalizePGValue(field.DataTypeOID, values[i])
			}
			record.Set(field.Name, value)
		}

		if err := s.SendRecord(record); err != nil {
			return errors.WithStack(err)
		}
	}

	if err := rows.Err(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func normalizePGValue(oid uint32, v any) any {
	if v == nil {
		return nil
	}
	switch oid {
	case pgtype.UUIDOID:
		if b, ok := v.([16]byte); ok {
			return uuid.UUID(b).String()
		}
		return fmt.Sprintf("%v", v)
	case pgtype.ByteaOID:
		if b, ok := v.([]byte); ok {
			return fmt.Sprintf("\\x%x", b)
		}
		return v
	case pgtype.TimeOID:
		if t, ok := v.(pgtype.Time); ok {
			d := time.Duration(t.Microseconds) * time.Microsecond
			base := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)
			return base.Add(d).Format(time.TimeOnly)
		}
		return v
	case pgtype.IntervalOID:
		if iv, ok := v.(pgtype.Interval); ok {
			d := time.Duration(iv.Microseconds)*time.Microsecond + time.Duration(iv.Days)*24*time.Hour + time.Duration(iv.Months)*30*24*time.Hour
			return d.String()
		}
		return v
	case pgtype.XMLOID:
		if b, ok := v.([]byte); ok {
			return string(b)
		}
		return v
	case pgtype.MacaddrOID:
		if b, ok := v.(net.HardwareAddr); ok {
			return fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x", b[0], b[1], b[2], b[3], b[4], b[5])
		}
		return v
	default:
		return v
	}
}
