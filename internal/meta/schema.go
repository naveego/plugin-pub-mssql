package meta

import (
	"encoding/json"
	"fmt"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"regexp"
	"sort"
	"strings"
)

type Schema struct {
	ID               string
	IsTable          bool
	IsChangeTracking bool
	columns          Columns
	// sorted list of key names
	keyNames   []string
	keyColumns []*Column
	// The query for this schema, if it's not a table or view.
	Query string
}

// Keys returns the names of the key columns, in alphabetical order.
func (s *Schema) Keys() []string {
	if s.keyNames == nil {
		for _, col := range s.columns {
			if col.IsKey {
				s.keyNames = append(s.keyNames, col.ID)
			}
		}
		sort.Strings(s.keyNames)
	}
	return s.keyNames
}

// KeyColumns returns the key columns, in alphabetical order.
func (s *Schema) KeyColumns() []*Column {
	if s.keyColumns == nil {
		for _, key := range s.Keys() {
			col, _ := s.LookupColumn(key)
			s.keyColumns = append(s.keyColumns, col)
		}
	}
	return s.keyColumns
}

func (s *Schema) GetColumn(id string) *Column {
	c, _ := s.LookupColumn(id)
	return c
}

func (s *Schema) LookupColumn(id string) (*Column, bool) {
	for _, col := range s.columns {
		if col.ID == id {
			return col, true
		}
	}
	return nil, false
}

func (s *Schema) GetColumnByOpaqueName(id string) (*Column, bool) {
	for _, col := range s.columns {
		if col.OpaqueName() == id {
			return col, true
		}
	}
	return nil, false
}

func (s *Schema) WithColumns(cols Columns) *Schema {
	for _, c := range cols {
		s.AddColumn(c)
	}
	return s
}

func (s *Schema) AddColumn(c *Column) *Column {
	c.SchemaID = s.ID
	s.columns = append(s.columns, c)
	sort.Stable(s.columns)
	s.keyNames = nil
	s.keyColumns = nil
	return c
}

func (s *Schema) Columns() Columns {
	return s.columns
}

func (s *Schema) NonKeyColumns() Columns {
	var out Columns
	for _, c := range s.columns {
		if !c.IsKey {
			out = append(out, c)
		}
	}
	return out
}

// ColumnsKeysFirst returns the columns with keys sorted to the beginning.
func (s *Schema) ColumnsKeysFirst() Columns {
	var out Columns
	for _, c := range s.columns {
		if c.IsKey {
			out = append(out, c)
		}
	}

	for _, c := range s.columns {
		if !c.IsKey {
			out = append(out, c)
		}
	}

	return out
}

type Columns []*Column

func (c Columns) Len() int           { return len(c) }
func (c Columns) Less(i, j int) bool { return strings.Compare(c[i].ID, c[j].ID) < 0 }
func (c Columns) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

// MakeSQLValuesFromMap produces a string like `'a', 7, null` from a map by extracting
// the values in the order the columns are in.
func (c Columns) MakeSQLValuesFromMap(m map[string]interface{}) string {

	var values []string

	for _, column := range c {
		if raw, ok := m[column.ID]; ok {
			values = append(values, column.RenderSQLValue(raw))
		} else {
			values = append(values, "null")
		}
	}

	return  strings.Join(values, ", ")

}

// MakeSQLValuesFromMap produces a string like `ID, Value, Other` from a map by extracting
// the column names in the order the columns are in.
func (c Columns) MakeSQLColumnNameList() string {

	var values []string

	for _, column := range c {

			values = append(values, column.ID)
	}

	return strings.Join(values, ", ")

}



func (c Columns) OmitKeys() Columns {
	var out Columns
	for _, column := range c {
		if !column.IsKey {
			out = append(out, column)
		}
	}
	return out
}

func (c Columns) OmitIDs(omit ...string) Columns {
	var out Columns
	for _, column := range c {
		skip := false
		for _, o := range omit {
			if column.ID == o || column.ID ==`[`+o+`]` {
				skip = true
				break
			}
		}
		if !skip{
			out = append(out, column)
		}
	}
	return out
}

type Column struct {
	SchemaID string `json:"s"`
	ID       string `json:"id"`
	IsKey    bool   `json:"-"`

	// If this is true, the additional properties below this line are valid
	IsDiscovered bool `json:"-"`
	//
	PropertyType pub.PropertyType `json:"-"`
	SQLType      string           `json:"-"`
	MaxLength	 int64			  `json:"-"`
	IsNullable   bool			  `json:"-"`
	opaqueName   string           `json:"-"`
}

var simplifierRE = regexp.MustCompile(`\W`)

// OpaqueName is an encoded safe name which can be used
// to alias to this column in a script.
func (c Column) OpaqueName() string {
	if c.opaqueName == "" {
		s := simplifierRE.ReplaceAllString(c.SchemaID, "")
		i := simplifierRE.ReplaceAllString(c.ID, "")
		c.opaqueName = fmt.Sprintf("__%s_%s__", s, i)
	}
	return c.opaqueName
	// j, _ := json.Marshal(c)
	// b := base64.RawURLEncoding.EncodeToString(j)
	// return b
}

func (c Column) String() string {
	return fmt.Sprintf("%s.%s", c.SchemaID, c.ID)
}

// RenderSQLValue emits a string which will be a valid representation of
// value v in a SQL query.
func (c Column) RenderSQLValue(v interface{}) string {

	if v == nil {
		return "null"
	}

	switch c.PropertyType {
	case pub.PropertyType_STRING,
		pub.PropertyType_TEXT,
		pub.PropertyType_DATE,
		pub.PropertyType_DATETIME,
		pub.PropertyType_XML,
		pub.PropertyType_BLOB:
		s := strings.Replace(fmt.Sprint(v), "'", "''", -1)
		return fmt.Sprintf("'%s'", s)
	case pub.PropertyType_JSON:

		j, _ := json.Marshal(v)
		s := strings.Replace(string(j), "'", "''", -1)
		return fmt.Sprintf("'%s'", s)
	case pub.PropertyType_BOOL:
		if b, ok := v.(bool); ok && b {
			return "1"
		}
		return "0"

	default:
		return fmt.Sprintf("%v", v)
	}
}

// Cast returns expression (which must be a SQL expression
// representing a value from this column) wrapped in a cast which will
// make the value an appropriate type for a SELECT statement.
// If the type of this column is already appropriate, expression
// will be returned.
func (c Column) CastForSelect(expression string) string {
	baseType := strings.ToLower(strings.Split(c.SQLType, "(")[0])
	switch (baseType) {
	case "datetimeoffset":
		return expression
	case "datetime2":
		return expression
	case "datetime":
		return expression
	case "smalldatetime":
		return expression
	case "date":
		return expression
	case "time":
		return expression
	case "float":
		return expression
	case "real":
		return expression
	case "decimal":
		return expression
	case "money":
		return expression
	case "smallmoney":
		return expression
	case "bigint":
		return expression
	case "int":
		return expression
	case "smallint":
		return expression
	case "tinyint":
		return expression
	case "bit":
		return expression
	case "timestamp":
		return expression
	case "uniqueidentifier":
		return expression
	case "nvarchar":
		return expression
	case "nchar":
		return expression
	case "varchar":
		return expression
	case "char":
		return expression
	case "varbinary":
		return expression
	case "binary":
		return expression
		// these types cannot be used in a DISTINCT, so must be cast to string
	case "sql_variant",
		"xml",
		"text",
		"image",
		"ntext":
		return fmt.Sprintf("CAST (%s as VARCHAR(MAX))", expression)
	// all user-defined types must be cast to strings
	default:
		return fmt.Sprintf("CAST (%s as VARCHAR(MAX))", expression)
	}
}
