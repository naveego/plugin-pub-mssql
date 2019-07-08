package meta

import (
	"regexp"
	"strings"
)

/*
Matches these strings into [_, schema, table] slices:
dbo.name
schema.table
[schema].table
schema.[table]
[schema].[table]
table
[table]
 */
var schemaIDWithBracketsRegex = regexp.MustCompile(`^(?:(\[[^]]+]|[^.]+)\.)?(\[?[^]\n]+]?)$`)
type SchemaID string

// SQLSchema returns the SQL schema from the schema ID (or "dbo" if there is no schema)
func (s SchemaID) SQLSchema() string {
	segs := schemaIDWithBracketsRegex.FindStringSubmatch(string(s))
	if len(segs) == 0 {
		return "dbo"
	}
	if segs[1] == "" {
		return "dbo"
	}

	return strings.Trim(segs[1], "[]")
}

// SQLSchemaEsc returns the SQL schema from the schema ID (or "dbo" if there is no schema),
// surrounded by escaping `[]`.
func (s SchemaID) SQLSchemaEsc() string {
	schema := s.SQLSchema()
	if strings.HasPrefix(schema, "["){
		return schema
	}
	return "[" + schema + "]"
}

// SQLSchema returns the SQL table name (without escaping brackets) from the schema ID,
// without surrounding '[]'
func (s SchemaID) SQLTable() string {
	segs := schemaIDWithBracketsRegex.FindStringSubmatch(string(s))
	if len(segs) == 0 {
		return ""
	}
	return strings.Trim(segs[2], "[]")
}


// SQLTableEsc returns the SQL table from the schema ID (or "dbo" if there is no schema),
// surrounded by escaping `[]`.
func (s SchemaID) SQLTableEsc() string {
	table := s.SQLTable()
	if strings.HasPrefix(table, "["){
		return table
	}
	return "[" + table + "]"
}