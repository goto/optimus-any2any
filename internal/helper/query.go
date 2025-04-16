// Copied from https://github.com/goto/transformers/blob/484e03703ab3591d22261ef64ba3b1798b6b9222/mc2mc/internal/query/helper.go
package helper

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	BREAK_MARKER = "--*--optimus-break-marker--*--"
)

var (
	semicolonPattern    = regexp.MustCompile(`;\s*(\n+|$)`)      // regex to match semicolons
	commentPattern      = regexp.MustCompile(`--[^\n]*`)         // regex to match comments
	multiCommentPattern = regexp.MustCompile(`(?s)/\*.*?\*/`)    // regex to match multi-line comments
	headerPattern       = regexp.MustCompile(`(?i)^set`)         // regex to match header statements
	variablePattern     = regexp.MustCompile(`(?i)^@`)           // regex to match variable statements
	dropPattern         = regexp.MustCompile(`(?i)^DROP\s+`)     // regex to match DROP statements
	udfPattern          = regexp.MustCompile(`(?i)^function\s+`) // regex to match UDF statements
	ddlPattern          = regexp.MustCompile(`(?i)^CREATE\s+`)   // regex to match DDL statements
	stringPattern       = regexp.MustCompile(`'[^']*'`)          // regex to match SQL strings (anything inside single quotes)
)

func SplitQueryComponents(query string) (headers []string, varsUDFs []string, queries []string) {
	query = strings.TrimSpace(query)

	// extract all header, variable and query lines
	stmts := semicolonPattern.Split(query, -1)
	queryIndex := 0
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		stmtWithoutComment := RemoveComments(stmt)
		if headerPattern.MatchString(strings.TrimSpace(stmtWithoutComment)) {
			for len(headers) <= queryIndex {
				headers = append(headers, "")
			}
			headers[queryIndex] += strings.TrimSpace(stmt) + "\n;\n"
		} else if variablePattern.MatchString(strings.TrimSpace(stmtWithoutComment)) ||
			udfPattern.MatchString(strings.TrimSpace(stmtWithoutComment)) {
			for len(varsUDFs) <= queryIndex {
				varsUDFs = append(varsUDFs, "")
			}
			varsUDFs[queryIndex] += strings.TrimSpace(stmt) + "\n;\n"
		} else if strings.TrimSpace(stmtWithoutComment) == "" {
			// if the statement is empty, it's a comment, then omit it
			// since it doesn't make sense to execute this statement
		} else {
			queries = append(queries, stmt)
			queryIndex++
		}
	}

	// fill in empty headers and varsUDFs + clear whitespace
	for i := range queries {
		if len(headers) == i {
			headers = append(headers, "")
		}
		if len(varsUDFs) == i {
			varsUDFs = append(varsUDFs, "")
		}
		headers[i] = strings.TrimSpace(headers[i])
		varsUDFs[i] = strings.TrimSpace(varsUDFs[i])
		queries[i] = strings.TrimSpace(queries[i])
	}

	return headers, varsUDFs, queries
}

// JoinSliceString joins a slice of strings with a delimiter
// and skips empty strings
func JoinSliceString(slice []string, delimiter string) string {
	builder := strings.Builder{}
	for i, s := range slice {
		if s == "" {
			continue
		}
		if i > 0 {
			builder.WriteString(delimiter)
		}
		builder.WriteString(s)
	}
	return strings.TrimSpace(builder.String())
}

func SeparateHeadersAndQuery(query string) (string, string) {
	headers := []string{}
	query = strings.TrimSpace(query)
	remainingQueries := []string{}

	// extract all header lines (set statements)
	stmts := semicolonPattern.Split(query, -1)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		stmtWithoutComment := RemoveComments(stmt)
		if headerPattern.MatchString(strings.TrimSpace(stmtWithoutComment)) {
			headers = append(headers, stmt)
		} else if strings.TrimSpace(stmtWithoutComment) == "" {
			// if the statement is empty, it's a comment, then omit it
			// since it doesn't make sense to execute this statement
			continue
		} else {
			remainingQueries = append(remainingQueries, stmt)
		}
	}

	headerStr := ""
	if len(headers) > 0 {
		for i, header := range headers {
			headers[i] = strings.TrimSpace(header)
		}
		headerStr = strings.Join(headers, "\n;\n")
		headerStr += "\n;"
	}

	// join the remaining queries back together
	queryStr := strings.Join(remainingQueries, "\n;\n")

	return headerStr, queryStr
}

func SeparateVariablesUDFsAndQuery(query string) (string, string) {
	variablesAndUDFs := []string{}
	query = strings.TrimSpace(query)
	remainingQueries := []string{}

	// extract all variable lines (@ statements and comments)
	stmts := semicolonPattern.Split(query, -1)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		stmtWithoutComment := RemoveComments(stmt)
		if variablePattern.MatchString(strings.TrimSpace(stmtWithoutComment)) ||
			udfPattern.MatchString(strings.TrimSpace(stmtWithoutComment)) {
			variablesAndUDFs = append(variablesAndUDFs, stmt)
		} else if strings.TrimSpace(stmtWithoutComment) == "" {
			// if the statement is empty, it's a comment, then omit it
			// since it doesn't make sense to execute this statement
		} else {
			remainingQueries = append(remainingQueries, stmt)
		}
	}

	variableUDFStr := ""
	if len(variablesAndUDFs) > 0 {
		for i, variable := range variablesAndUDFs {
			variablesAndUDFs[i] = strings.TrimSpace(variable)
		}
		variableUDFStr = strings.Join(variablesAndUDFs, "\n;\n")
		variableUDFStr += "\n;"
	}

	// join the remaining queries back together
	queryStr := strings.Join(remainingQueries, "\n;\n")

	return variableUDFStr, queryStr
}

func SeparateDropsAndQuery(query string) (string, string) {
	drops := []string{}
	query = strings.TrimSpace(query)
	remainingQueries := []string{}

	// extract all drop lines
	stmts := semicolonPattern.Split(query, -1)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		stmtWithoutComment := RemoveComments(stmt)
		if dropPattern.MatchString(strings.TrimSpace(stmtWithoutComment)) {
			drops = append(drops, stmt)
		} else if strings.TrimSpace(stmtWithoutComment) == "" {
			// if the statement is empty, it's a comment, then omit it
			// since it doesn't make sense to execute this statement
		} else {
			remainingQueries = append(remainingQueries, stmt)
		}
	}

	dropStr := ""
	if len(drops) > 0 {
		for i, drop := range drops {
			drops[i] = strings.TrimSpace(drop)
		}
		dropStr = strings.Join(drops, "\n;\n")
		dropStr += "\n;"
	}

	// join the remaining queries back together
	queryStr := strings.Join(remainingQueries, "\n;\n")

	return dropStr, queryStr
}

func RemoveComments(query string) string {
	query = commentPattern.ReplaceAllString(query, "")
	query = multiCommentPattern.ReplaceAllString(query, "")
	return query
}

func ProtectedStringLiteral(query string) (map[string]string, string) {
	// Replace all strings with a placeholder to protect them
	placeholders := make(map[string]string)
	protectedQuery := stringPattern.ReplaceAllStringFunc(query, func(match string) string {
		placeholder := fmt.Sprintf("__STRING_PLACEHOLDER_%d__", len(placeholders))
		placeholders[placeholder] = match
		return placeholder
	})
	return placeholders, protectedQuery
}

func RestoreStringLiteral(query string, placeholders map[string]string) string {
	// Restore all strings from the placeholders
	for placeholder, original := range placeholders {
		query = strings.ReplaceAll(query, placeholder, original)
	}
	return query
}

func IsDDL(query string) bool {
	return ddlPattern.MatchString(query)
}
