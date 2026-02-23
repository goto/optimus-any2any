package postgresql

import "testing"

func TestIsSelectQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "simple select",
			query: "SELECT 1",
			want:  true,
		},
		{
			name:  "select with trailing semicolon",
			query: "SELECT 1;",
			want:  true,
		},
		{
			name: "select with comments and string semicolon",
			query: `
				-- leading comment
				SELECT 'a; b' AS v /* inline ; block comment */;
			`,
			want: true,
		},
		{
			name: "with multiple ctes",
			query: `
				WITH a AS (SELECT 1 AS id),
				     b AS (SELECT id FROM a)
				SELECT * FROM b
			`,
			want: true,
		},
		{
			name: "with recursive cte",
			query: `
				WITH RECURSIVE t(n) AS (
					SELECT 1
					UNION ALL
					SELECT n + 1 FROM t WHERE n < 5
				)
				SELECT * FROM t
			`,
			want: true,
		},
		{
			name: "select with read-only cte and values",
			query: `
				WITH cte AS (
					SELECT * FROM (VALUES (1), (2), (3)) AS v(id)
				)
				SELECT id FROM cte ORDER BY id
			`,
			want: true,
		},
		{
			name: "reject multi statement",
			query: `
				SELECT 1;
				SELECT 2;
			`,
			want: false,
		},
		{
			name:  "reject write query",
			query: "UPDATE users SET name = 'x'",
			want:  false,
		},
		{
			name:  "reject insert query",
			query: "INSERT INTO users(id) VALUES (1)",
			want:  false,
		},
		{
			name:  "reject delete query",
			query: "DELETE FROM users WHERE id = 1",
			want:  false,
		},
		{
			name: "reject write cte",
			query: `
				WITH updated AS (
					UPDATE users SET name = 'x' RETURNING id
				)
				SELECT * FROM updated
			`,
			want: false,
		},
		{
			name: "reject explain select",
			query: `
				EXPLAIN SELECT * FROM users;
			`,
			want: false,
		},
		{
			name: "reject show statement",
			query: `
				SHOW search_path;
			`,
			want: false,
		},
		{
			name: "reject set statement",
			query: `
				SET search_path TO public;
			`,
			want: false,
		},
		{
			name: "reject lock statement",
			query: `
				LOCK TABLE users IN ACCESS EXCLUSIVE MODE;
			`,
			want: false,
		},
		{
			name: "reject create table as select",
			query: `
				CREATE TABLE users_copy AS SELECT * FROM users;
			`,
			want: false,
		},
		{
			name: "reject malformed query",
			query: `
				SELECT FROM
			`,
			want: false,
		},
		{
			name: "reject do block",
			query: `
				DO $$
				BEGIN
					RAISE NOTICE 'hello';
				END
				$$;
			`,
			want: false,
		},
		{
			name:  "reject empty query",
			query: "   \n\t ",
			want:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := IsSelectQuery(tt.query); got != tt.want {
				t.Fatalf("%s \nIsSelectQuery() = %v, want %v\n", tt.name, got, tt.want)
			}
		})
	}
}
