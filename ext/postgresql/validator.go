package postgresql

import (
	"regexp"
	"strings"

	"github.com/goto/optimus-any2any/internal/helper"
	"github.com/pkg/errors"
)

var mutatingKeywordPattern = regexp.MustCompile(`\b(INSERT|UPDATE|DELETE|MERGE|UPSERT|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|COMMENT|VACUUM|REINDEX|CLUSTER|REFRESH|CALL|DO|COPY)\b`)

func validateReadOnlyQuery(query string) error {
	if query == "" {
		return errors.New("postgresql source query must not be empty")
	}

	// Protect string literals before stripping comments so validation
	// doesn't misinterpret tokens inside quoted values.
	_, protectedQuery := helper.ProtectedStringLiteral(query)
	clean := strings.TrimSpace(helper.RemoveComments(protectedQuery))
	if clean == "" {
		return errors.New("postgresql source query must not be empty")
	}

	// Disallow multiple statements entirely. This prevents stacked queries
	// where one statement is read-only and another mutates data.
	statements := splitStatements(clean)
	if len(statements) != 1 {
		return errors.Errorf("postgresql source supports exactly one read-only statement, got %d statements", len(statements))
	}

	upper := strings.ToUpper(statements[0])
	allowedPrefixes := []string{
		"SELECT",
		"WITH",
		"SHOW",
		"EXPLAIN",
		"VALUES",
	}
	for _, prefix := range allowedPrefixes {
		if strings.HasPrefix(upper, prefix) {
			if mutatingKeywordPattern.MatchString(upper) {
				return errors.Errorf(
					"postgresql source query contains mutating keyword: %s",
					firstMatchingToken(upper),
				)
			}
			return nil
		}
	}

	return errors.Errorf(
		"postgresql source only supports read-only queries (SELECT, WITH, SHOW, EXPLAIN, VALUES), got: %s",
		firstSQLToken(upper),
	)
}

func firstSQLToken(sql string) string {
	fields := strings.Fields(sql)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func splitStatements(sql string) []string {
	parts := strings.Split(sql, ";")
	stmts := make([]string, 0, len(parts))
	for _, part := range parts {
		s := strings.TrimSpace(part)
		if s == "" {
			continue
		}
		stmts = append(stmts, s)
	}
	return stmts
}

func firstMatchingToken(sql string) string {
	m := mutatingKeywordPattern.FindString(sql)
	return strings.TrimSpace(m)
}
