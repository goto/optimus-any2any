package postgresql

import (
	"reflect"

	pgquery "github.com/wasilibs/go-pgquery"
)

var forbiddenNodeWrappers = map[string]struct{}{
	"*pg_query.Node_InsertStmt":             {},
	"*pg_query.Node_UpdateStmt":             {},
	"*pg_query.Node_DeleteStmt":             {},
	"*pg_query.Node_MergeStmt":              {},
	"*pg_query.Node_CreateTableAsStmt":      {},
	"*pg_query.Node_CreateStmt":             {},
	"*pg_query.Node_AlterTableStmt":         {},
	"*pg_query.Node_AlterTableCmd":          {},
	"*pg_query.Node_DropStmt":               {},
	"*pg_query.Node_TruncateStmt":           {},
	"*pg_query.Node_IndexStmt":              {},
	"*pg_query.Node_CallStmt":               {},
	"*pg_query.Node_DoStmt":                 {},
	"*pg_query.Node_CopyStmt":               {},
	"*pg_query.Node_VariableSetStmt":        {},
	"*pg_query.Node_VariableShowStmt":       {},
	"*pg_query.Node_TransactionStmt":        {},
	"*pg_query.Node_LockStmt":               {},
	"*pg_query.Node_PrepareStmt":            {},
	"*pg_query.Node_ExecuteStmt":            {},
	"*pg_query.Node_DeallocateStmt":         {},
	"*pg_query.Node_RefreshMatViewStmt":     {},
	"*pg_query.Node_VacuumStmt":             {},
	"*pg_query.Node_ExplainStmt":            {},
	"*pg_query.Node_GrantStmt":              {},
	"*pg_query.Node_GrantRoleStmt":          {},
	"*pg_query.Node_RevokeStmt":             {},
	"*pg_query.Node_CommentStmt":            {},
	"*pg_query.Node_CheckPointStmt":         {},
	"*pg_query.Node_DiscardStmt":            {},
	"*pg_query.Node_NotifyStmt":             {},
	"*pg_query.Node_ListenStmt":             {},
	"*pg_query.Node_UnlistenStmt":           {},
	"*pg_query.Node_CreateFunctionStmt":     {},
	"*pg_query.Node_AlterFunctionStmt":      {},
	"*pg_query.Node_CreatePublicationStmt":  {},
	"*pg_query.Node_AlterPublicationStmt":   {},
	"*pg_query.Node_CreateSubscriptionStmt": {},
	"*pg_query.Node_AlterSubscriptionStmt":  {},
	"*pg_query.Node_DropSubscriptionStmt":   {},
	"*pg_query.Node_CreateRoleStmt":         {},
	"*pg_query.Node_AlterRoleStmt":          {},
	"*pg_query.Node_AlterRoleSetStmt":       {},
	"*pg_query.Node_DropRoleStmt":           {},
}

func IsSelectQuery(query string) bool {
	tree, err := pgquery.Parse(query)
	if err != nil || tree == nil {
		return false
	}

	if len(tree.Stmts) != 1 || tree.Stmts[0] == nil || tree.Stmts[0].Stmt == nil {
		return false
	}

	rootNode := tree.Stmts[0].Stmt.GetNode()
	if reflect.TypeOf(rootNode).String() != "*pg_query.Node_SelectStmt" {
		return false
	}

	return !containsForbiddenNode(tree.Stmts[0].Stmt)
}

func containsForbiddenNode(v any) bool {
	return containsForbiddenValue(reflect.ValueOf(v))
}

func containsForbiddenValue(v reflect.Value) bool {
	if !v.IsValid() {
		return false
	}

	if v.CanInterface() {
		if _, forbidden := forbiddenNodeWrappers[reflect.TypeOf(v.Interface()).String()]; forbidden {
			return true
		}
	}

	switch v.Kind() {
	case reflect.Interface, reflect.Ptr:
		if v.IsNil() {
			return false
		}
		return containsForbiddenValue(v.Elem())
	case reflect.Struct:
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			// Skip unexported fields (protobuf internals).
			if t.Field(i).PkgPath != "" {
				continue
			}
			if containsForbiddenValue(v.Field(i)) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if containsForbiddenValue(v.Index(i)) {
				return true
			}
		}
	case reflect.Map:
		for _, k := range v.MapKeys() {
			if containsForbiddenValue(v.MapIndex(k)) {
				return true
			}
		}
	}

	return false
}
