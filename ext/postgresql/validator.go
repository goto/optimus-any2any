package postgresql

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func IsSelectQuery(query string) bool {
	tree, err := pg_query.Parse(query)
	if err != nil {
		return false
	}

	if len(tree.Stmts) == 0 {
		return false
	}

	stmt := tree.Stmts[0].Stmt
	_, ok := stmt.Node.(*pg_query.Node_SelectStmt)
	return ok
}
