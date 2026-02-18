package postgresql

import (
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	xnet "github.com/goto/optimus-any2any/internal/net"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

// PGSource is a source that reads data from PostgreSQL.
type PGSource struct {
	common.Source

	pool  *pgxpool.Pool
	query string
}

var _ flow.Source = (*PGSource)(nil)

// NewSource creates a new PostgreSQL source.
func NewSource(commonSource common.Source, dsn, queryFilePath string, maxOpenConnection, minOpenConnection int32) (*PGSource, error) {
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

	s := &PGSource{
		Source: commonSource,
		pool:   pool,
		query:  string(rawQuery),
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
				value = normalizePGValue(values[i])
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

func normalizePGValue(v any) any {
	if v == nil {
		return nil
	}
	switch vv := v.(type) {
	case []byte:
		return string(vv)
	case [16]uint8:
		return uuid.UUID(vv).String()
	default:
		return vv
	}
}
