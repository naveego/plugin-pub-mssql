package meta

import (
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

type Column struct {
	SchemaID string `json:"s"`
	ID       string `json:"id"`
	IsKey    bool   `json:"-"`

	// If this is true, the additional properties below this line are valid
	IsDiscovered bool `json:"-"`
	//
	PropertyType pub.PropertyType `json:"-"`
	SQLType      string           `json:"-"`
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

	switch c.PropertyType {
	case pub.PropertyType_STRING:
		return fmt.Sprintf("'%s'", v)
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
