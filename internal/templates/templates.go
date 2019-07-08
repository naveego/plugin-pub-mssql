package templates

import (
	"crypto/md5"
	"fmt"
	"github.com/Masterminds/sprig"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/pkg/errors"
	"regexp"
	"strings"
	"text/template"
)

type BatchQueryArgs struct {
	Columns       []*pub.Property
	Keys          []string
	SchemaQuery   string
	TrackedTables []BatchTrackedTable
}

type BatchTrackedTable struct {
	ID    string
	SelectQuery string
	ProjectQuery string
	Keys []string
	NonKeys []string
}

var simplifierRE = regexp.MustCompile(`\W+`)

func UniquifySQLName(x string) string {
	x = strings.Trim(x, "[]")
	x =  simplifierRE.ReplaceAllString(x, "_")
	h := md5.New()
	h.Write([]byte(x))
	o := h.Sum(nil)
	suffix := fmt.Sprintf("%x", o)
	return fmt.Sprintf("[%s_%s]", x, suffix[:4])
}

// PrefixColumn makes a safe column name containing the prefix, a ., and the current ID, inside []"
// PrefixColumn("Table", "[Key]") => "[Table.Key]"
func PrefixColumn(prefix, id string) string {
	id = strings.Trim(id, "[]")
	return fmt.Sprintf("[%s.%s]", prefix, id)
}


func compileTemplate(name, input string) *template.Template {
	t, err := template.New(name).
		Funcs(sprig.TxtFuncMap()).
		Funcs(template.FuncMap{
			"uniquify":     UniquifySQLName,
			"PrefixColumn": PrefixColumn,
			"ConvertPluginTypeToSQLType": meta.ConvertPluginTypeToSQLType,
		}).
		Parse(input)
	if err != nil {
		panic(errors.Errorf("error compiling template: %s\ntemplate was:\n%s", err, input))
	}
	return t
}

func renderTemplate(t *template.Template, args interface{}) (string, error) {
	w := new(strings.Builder)
	err := t.Execute(w, args)

	if err != nil {
		return "", errors.Wrap(err, "error rendering template")
	}

	return w.String(),nil
}



var selfBridgeQueryTemplate = compileTemplate("selfBridgeQueryTemplate",
	// language=GoTemplate
	`SELECT 
    {{ first .SchemaInfo.Keys }} as {{ first .SchemaInfo.Keys | PrefixColumn "Schema" }}         
    , {{ first .SchemaInfo.Keys }} as {{ PrefixColumn "Dependency" (first .SchemaInfo.Keys) }}
  	{{- range rest .SchemaInfo.Keys }}
  	, {{ . }} as {{ PrefixColumn "Schema" . }},  {{ . }} as {{ PrefixColumn "Dependency" . }}
  	{{- end }}
FROM {{ .SchemaInfo.ID }}` )

type SelfBridgeQueryArgs struct {
	SchemaInfo *meta.Schema
}

func RenderSelfBridgeQuery(args SelfBridgeQueryArgs) (string, error) {
	return renderTemplate(selfBridgeQueryTemplate, args)
}

type ChangeDetectionQueryArgs struct {
	SchemaArgs     *meta.Schema
	DependencyArgs *meta.Schema
	BridgeQuery    string
	// Key which will be prefixed to the names of columns from
	// the change tracking results.
	ChangeKeyPrefix string
	// The column name to populate with SYS_CHANGE_OPERATION
	ChangeOperationColumnName string
}

func RenderChangeDetectionQuery(args ChangeDetectionQueryArgs) (string, error) {
	return renderTemplate(changeDetectionQueryTemplate, args)
}

var changeDetectionQueryTemplate *template.Template = compileTemplate("changeDetectionQuery",
	// language=GoTemplate
	`
SELECT 
{{- range .DependencyArgs.KeyColumns }}
   {{ uniquify "CT" }}.{{ .ID }} AS "{{ $.ChangeKeyPrefix }}{{ .OpaqueName }}" /*{{ . }}*/, 
{{- end }}         
{{- range .SchemaArgs.KeyColumns }}
    {{ uniquify "Bridge" }}.{{ PrefixColumn "Schema" .ID }} AS {{ .OpaqueName }} /*{{ . }}*/, 
{{- end }}
    {{ uniquify "CT" }}.SYS_CHANGE_OPERATION as {{ .ChangeOperationColumnName }}
	-- The change tracking data is the driver for this query         
    FROM 
    (
	 	SELECT *
        FROM CHANGETABLE(CHANGES {{ .DependencyArgs.ID }}, @minVersion) AS {{ uniquify "CT" }}        	    
	    WHERE SYS_CHANGE_VERSION <= @maxVersion 
	) as {{ uniquify "CT" }}
	-- We join in the bridge query to get us the rows in the schema which have been affected by the change. 
	-- It's a left outer join so that if no rows are affected we still get the change keys, so 
	-- that we can check rows which were previously associated with the changed row to see if they've changed now.
	LEFT OUTER JOIN 
    (
{{ .BridgeQuery | indent 8 }}
	) as {{ uniquify "Bridge" }}
	ON  {{ uniquify "Bridge" }}.{{ first .DependencyArgs.Keys | PrefixColumn "Dependency" }} = {{ uniquify "CT" }}.{{ first .DependencyArgs.Keys}}
{{- range (rest .DependencyArgs.Keys ) }}
	AND  {{ uniquify "Bridge" }}.{{ PrefixColumn "Dependency" . }} = {{ uniquify "CT" }}.{{ . }}
{{- end }}	
        	` )


type SchemaDataQueryArgs struct {
	SchemaArgs *meta.Schema
	DependencyTables []*meta.Schema
	// Keys to filter the schema records by.
	RowKeys []meta.RowKeys
	// Name which will be used for the column which contains 1 if the row has been deleted from the schema.
	DeletedFlagColumnName string
}

// RenderSchemaDataQuery renders the query used to actually retrieve data from a schema.
func RenderSchemaDataQuery(args SchemaDataQueryArgs) (string, error) {
	result, err :=  renderTemplate(schemaDataQuery, args)
	return result, err
}

var schemaDataQuery = compileTemplate("schema data query",
	// language=GoTemplate
	`
{{- if .RowKeys }}
/* declare and populate the table used to filter change sets */
DECLARE @ChangeKeys table(
{{ with first .SchemaArgs.KeyColumns }}{{ .ID }} {{ .SQLType }}{{ end }}
{{- range rest .SchemaArgs.KeyColumns }}
, {{ .ID }} {{ .SQLType }}
{{- end }}
)

INSERT INTO @ChangeKeys ({{ join ", " .SchemaArgs.Keys}})
VALUES
{{- range $index, $rowKeys := .RowKeys }}
	{{ if $index }},{{ end }}({{ with first $rowKeys }}{{ ($.SchemaArgs.GetColumn .ColumnID).RenderSQLValue .Value }}{{ end }}{{ range rest $rowKeys }}, {{ ($.SchemaArgs.GetColumn .ColumnID).RenderSQLValue .Value }}{{ end }})
{{- end }}

{{- end }}

SELECT DISTINCT
{{- range .SchemaArgs.Columns }}
{{- if and $.RowKeys .IsKey }}
		COALESCE({{ uniquify "SchemaQuery" }}.{{ .ID }}, {{ uniquify "ChangeKeys" }}.{{ .ID }}) as {{ .ID }},
{{- else }}
		{{ printf "%s.%s" (uniquify "SchemaQuery") .ID | .CastForSelect }} as {{ .ID }},
{{- end }}
{{- end }}
{{- range .DependencyTables }}
{{- range .Columns}}
{{- if .IsKey }}
		{{ uniquify .SchemaID }}.{{ PrefixColumn "Dependency" .ID }} AS {{ .OpaqueName }} /*{{ . }}*/,
{{- end }}
{{- end }}
{{- end }}
{{- if .RowKeys }}
	CASE 
		WHEN {{ with first .SchemaArgs.Keys }}{{ uniquify "SchemaQuery" }}.{{ . }} IS NULL {{end}} THEN CONVERT(bit, 1)
		ELSE CONVERT(bit, 0)
	END
{{- else }}
	CONVERT(bit, 0)
{{- end }} AS {{.DeletedFlagColumnName}} 
/*
Here we are running the query provided by the user, or just selecting everything from the table/view.
This is the part that gets us the data we will actually be turning into published records.
*/
FROM (
{{ .SchemaArgs.Query | indent 6 -}}
     ) AS {{ uniquify "SchemaQuery" }}
{{- if .DependencyTables }}
/*
Now we outer join the changes from all the tables which drive the thing we're publishing from.
*/
    {{- range .DependencyTables }}
{{ $table := .}}
LEFT OUTER JOIN 
	(
{{ .Query | indent 6 }}
	) as {{ uniquify $table.ID }}
	ON {{ uniquify $table.ID }}.{{ PrefixColumn "Schema" (first $.SchemaArgs.Keys) }} = {{ uniquify "SchemaQuery" }}.{{ first $.SchemaArgs.Keys }}
        {{- range (rest $.SchemaArgs.Keys ) }}
	AND {{ uniquify $table.ID }}.{{ PrefixColumn "Schema" . }} = {{ uniquify "SchemaQuery" }}.{{ . }}
        {{- end }}	
    {{- end }}
{{- end }}
{{- if .RowKeys }}
/* Now we filter the results down to only those rows that we care about based on the change keys derived from a detected change. */
RIGHT OUTER JOIN @ChangeKeys as {{ uniquify "ChangeKeys" }}
	ON {{ with first .SchemaArgs.Keys }}{{ uniquify "SchemaQuery" }}.{{ . }} = {{ uniquify "ChangeKeys" }}.{{ . }}{{end}}
{{- range rest .SchemaArgs.Keys }}
	AND {{ uniquify "SchemaQuery" }}.{{ . }} = {{ uniquify "ChangeKeys" }}.{{ . }}
{{- end }}
{{- end }}
`)
