package internal

import (
	"crypto/md5"
	"fmt"
	"github.com/Masterminds/sprig"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/pkg/errors"
	"regexp"
	"strings"
	"text/template"
)

type batchQueryArgs struct {
	Columns       []*pub.Property
	Keys          []string
	SchemaQuery   string
	TrackedTables []batchTrackedTable
}

type batchTrackedTable struct {
	ID    string
	SelectQuery string
	ProjectQuery string
	Keys []string
	NonKeys []string
}

var simplifierRE = regexp.MustCompile("[^A-z09_]")

func uniquify(x string) string {
	x = simplifierRE.ReplaceAllString(x, "")
	h := md5.New()
	h.Write([]byte(x))
	o := h.Sum(nil)
	suffix := fmt.Sprintf("%x", o)
	return fmt.Sprintf("%s_%s", x, suffix[:4])
}

func prefixCol(prefix, id string) string {
	id = strings.Trim(id, "[]")
	return fmt.Sprintf("[%s.%s]", prefix, id)
}


func compileTemplate(name, input string) *template.Template {
	t, err := template.New(name).
		Funcs(sprig.TxtFuncMap()).
		Funcs(template.FuncMap{
			"uniquify": uniquify,
			"prefixCol": prefixCol,
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

func renderBatchQuery(args batchQueryArgs) (string, error) {
	return renderTemplate(batchQueryTemplate, args)
}

var batchQueryTemplate = compileTemplate("batch-query",
	// language=GoTemplate
	`{{- /*gotype: github.com/naveego/plugin-pub-mssql/internal.batchQueryArgs*/ -}}
/* 
We explicitly select each column from the shape, but we select the 
key columns from the intermediate ("Changes") table rather than from the 
source view/table/query so that we can at least get the PKs of deleted rows. 
*/ 
SELECT 
{{- range .Columns }}
       {{ if .IsKey }}{{ uniquify "Changes" }}{{ else }}{{ uniquify "SchemaQuery" }}{{ end }}.{{ .Id }}, 
{{- end }}
	   {{ uniquify "Changes" }}.SYS_CHANGE_OPERATION AS __NAVEEGO__ACTION -- we alias the operation with a safe name here
/*
Here we are running the query provided by the user, or just selecting everything from the table/view.
This is the part that gets us the data we will actually be turning into published records.
*/
FROM (
{{ .SchemaQuery | indent 6 -}}
     ) AS {{ uniquify "SchemaQuery" }}
/*
Now we outer join the changes from all the tables which drive the thing we're publishing from.
*/
RIGHT OUTER JOIN 
	(
	SELECT DISTINCT *
	FROM (
{{- range $i, $TT := .TrackedTables }}
{{- if gt $i 0 }}
	      UNION ALL
{{- end }}
          {{ $TT.SelectQuery }}, SYS_CHANGE_OPERATION
          FROM (
                SELECT {{range $TT.Keys}}Changes.{{ . }}, {{end}}
        		       {{ range $TT.NonKeys }}Source.{{ . }}, {{ end}}
         		       SYS_CHANGE_OPERATION
                FROM CHANGETABLE(CHANGES {{ $TT.ID }}, @minVersion) AS Changes
        	    LEFT OUTER JOIN {{ $TT.ID }} AS Source ON Source.{{ first $TT.Keys }} = Changes.{{ first $TT.Keys}}
{{- range (rest $TT.Keys ) }}
	   	                                               AND Source.{{ . }} = Changes.{{ . }}
{{- end }}	
	            WHERE SYS_CHANGE_VERSION <= @maxVersion
               ) AS Source
	      {{ $TT.ProjectQuery -}}
{{- end }}
	     ) AS AllChangeSources
    ) AS {{ uniquify "Changes" }}
	ON {{ with first .Keys }} {{ uniquify "SchemaQuery" }}.{{ . }} = {{ uniquify "Changes" }}.{{ . }} {{ end }}
{{ range (rest .Keys) -}}
	   AND {{ uniquify "SchemaQuery" }}.{{ . }} = {{ uniquify "Changes" }}.{{ . }}
{{ end -}}`)



var selfDependencyQueryTemplate = compileTemplate("dependencyQuery",
	// language=GoTemplate
	`SELECT 
        {{ first .SchemaInfo.Keys }} as {{ first .SchemaInfo.Keys | prefixCol "Source" }}         
        , {{ first .SchemaInfo.Keys }} as {{ prefixCol "Target" (first .SchemaInfo.Keys) }}
  		{{ range rest .SchemaInfo.Keys }}, {{ . }} as {{ prefixCol "Source" . }},  {{ . }} as {{ prefixCol "Target" . }}{{ end }}
 
        	FROM {{ .SchemaInfo.ID }}` )

type SelfDependencyQueryArgs struct {
	SchemaInfo *SchemaInfo
}

func RenderSelfDependencyQuery(args SelfDependencyQueryArgs) (string, error) {
	return renderTemplate(selfDependencyQueryTemplate, args)
}


var changeDetectionQueryTemplate *template.Template = compileTemplate("changeDetectionQuery",
	// language=GoTemplate
	`

SELECT 
        {{ range .DependencyKeys }}{{ uniquify "Dep" }}.{{prefixCol "Target" . }}, {{ end }}         
        {{ range .TableInfo.Keys }}{{ uniquify "CT" }}.{{ . }}, {{ end }}
        {{ uniquify "CT" }}.SYS_CHANGE_OPERATION as SYS_CHANGE_OPERATION         
        FROM (
		  	SELECT *
            FROM CHANGETABLE(CHANGES {{ .TableInfo.ID }}, @minVersion) AS {{ uniquify "CT" }}        	    
	        WHERE SYS_CHANGE_VERSION <= @maxVersion 
			) as {{ uniquify "CT" }} 
		LEFT OUTER JOIN 
		    (
{{ .DependencyQuery | indent 16 }}
			) as {{ uniquify "Dep" }}
			ON  {{ uniquify "Dep" }}.{{ first .TableInfo.Keys | prefixCol "Target" }} = {{ uniquify "CT" }}.{{ first .TableInfo.Keys}}
{{- range (rest .TableInfo.Keys ) }}
	   	   		AND  {{ uniquify "Dep" }}.{{ prefixCol "Target" . }} = {{ uniquify "CT" }}.{{ . }}
{{- end }}	
        	` )

type ChangeDetectionQueryArgs struct {
	TableInfo       *SchemaInfo
	DependencyKeys []string
	DependencyQuery string
}

func RenderChangeDetectionQuery(args ChangeDetectionQueryArgs) (string, error) {
	return renderTemplate(changeDetectionQueryTemplate, args)
}


type RealTimeSchemaQueryArgs struct {
	Shape *pub.Shape
	Tables RealTimeTableSettings
	SchemaInfo map[string]*SchemaInfo
}

type RealTimeSchemaQueryInfoForTable struct {

}

type RealTimeSchemaQueryInfoForTable struct {

}

func RenderRealTimeSchemaQuery(args ChangeDetectionQueryArgs) (string, error) {
	return renderTemplate(changeTrackedSchemaQueryTemplate, args)
}

var changeTrackedSchemaQueryTemplate = compileTemplate("change tracked query",
	// language=GoTemplate
	`
SELECT 
{{- range .Shape.Properties }}
		{{ .Id }}, 
{{- end }}
{{- range .Tables }}
{{ $info := index .SchemaInfo .SchemaID }}
{{- range $info.Keys}}
		{{ uniquify $info.ID }}.{{ . }} AS {{ prefixCol "" }}
{{- end }}

	   {{ uniquify "Changes" }}.SYS_CHANGE_OPERATION AS __NAVEEGO__ACTION -- we alias the operation with a safe name here
/*
Here we are running the query provided by the user, or just selecting everything from the table/view.
This is the part that gets us the data we will actually be turning into published records.
*/
FROM (
{{ .SchemaQuery | indent 6 -}}
     ) AS {{ uniquify "SchemaQuery" }}
/*
Now we outer join the changes from all the tables which drive the thing we're publishing from.
*/
RIGHT OUTER JOIN 
	(
	SELECT DISTINCT *
	FROM (
{{- range $i, $TT := .TrackedTables }}
{{- if gt $i 0 }}
	      UNION ALL
{{- end }}
          {{ $TT.SelectQuery }}, SYS_CHANGE_OPERATION
          FROM (
                SELECT {{range $TT.Keys}}Changes.{{ . }}, {{end}}
        		       {{ range $TT.NonKeys }}Source.{{ . }}, {{ end}}
         		       SYS_CHANGE_OPERATION
                FROM CHANGETABLE(CHANGES {{ $TT.ID }}, @minVersion) AS Changes
        	    LEFT OUTER JOIN {{ $TT.ID }} AS Source ON Source.{{ first $TT.Keys }} = Changes.{{ first $TT.Keys}}
{{- range (rest $TT.Keys ) }}
	   	                                               AND Source.{{ . }} = Changes.{{ . }}
{{- end }}	
	            WHERE SYS_CHANGE_VERSION <= @maxVersion
               ) AS Source
	      {{ $TT.ProjectQuery -}}
{{- end }}
	     ) AS AllChangeSources
    ) AS {{ uniquify "Changes" }}
	ON {{ with first .Keys }} {{ uniquify "SchemaQuery" }}.{{ . }} = {{ uniquify "Changes" }}.{{ . }} {{ end }}
{{ range (rest .Keys) -}}
	   AND {{ uniquify "SchemaQuery" }}.{{ . }} = {{ uniquify "Changes" }}.{{ . }}
{{ end -}}`)


