package internal

import (
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/Masterminds/sprig"
	"github.com/naveego/go-json-schema"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/pkg/errors"
	"regexp"
	"sort"
	"strings"
	"text/template"
	"time"
)

type RealTimeHelper struct {
	dbChangeTrackingEnabled    bool
	tableChangeTrackingEnabled map[string]bool
}

func (r *RealTimeHelper) ensureDBChangeTrackingEnabled(session *OpSession) error {

	if r.dbChangeTrackingEnabled {
		return nil
	}

	dbName := session.Settings.Database

	row := session.DB.QueryRowContext(session.Ctx,
		`SELECT count(1)
FROM sys.change_tracking_databases
WHERE database_id=DB_ID(@ID)`, sql.Named("ID", dbName))

	var count int
	err := row.Scan(&count)
	if err == nil && count == 0 {
		err = errors.New("Database does not have change tracking enabled. " +
			"See https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-tracking-sql-server for details on enabling change tracking.")
	}
	if err == nil {
		r.dbChangeTrackingEnabled = true
	}

	return err
}

func (r *RealTimeHelper) ensureTableChangeTrackingEnabled(session *OpSession, schemaID string) error {
	if r.tableChangeTrackingEnabled == nil {
		r.tableChangeTrackingEnabled = map[string]bool{}
	}

	if !r.tableChangeTrackingEnabled[schemaID] {
		row := session.DB.QueryRow(
			`SELECT count(1)
FROM sys.change_tracking_tables
WHERE object_id=OBJECT_ID(@ID)`, sql.Named("ID", schemaID))
		var count int
		err := row.Scan(&count)
		if err == nil && count == 0 {
			err = errors.New("Table does not have change tracking enabled. See https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-tracking-sql-server for details on enabling change tracking.")
		}
		if err != nil {
			return err
		}
	}

	r.tableChangeTrackingEnabled[schemaID] = true
	return nil
}

func (r *RealTimeHelper) isSchemaTable(session *OpSession, schemaID string) (bool, error) {

	for id, table := range session.SchemaInfo {
		if id == schemaID {
			return table.IsTable, nil
		}
	}

	return false, nil
}

func (r *RealTimeHelper) ConfigureRealTime(session *OpSession, req *pub.ConfigureRealTimeRequest) (*pub.ConfigureRealTimeResponse, error) {
	var err error

	err = r.ensureDBChangeTrackingEnabled(session)

	if err != nil {
		// If change tracking is disabled on the DB we can't do much.
		return (&pub.ConfigureRealTimeResponse{}).WithFormErrors(err.Error()), nil
	}

	isTable, err := r.isSchemaTable(session, req.Shape.Id)
	if err != nil {
		// This shouldn't happen, but if we can't tell whether it's a table or not we can't do anything.
		return nil, err
	}

	if isTable {
		// If the schema is a table and it has change tracking, we have nothing else to configure.
		// Otherwise, we'll show the user an error telling them to configure change tracking for the table.

		err = r.ensureTableChangeTrackingEnabled(session, req.Shape.Id)
		if err != nil {
			msg := fmt.Sprintf("The schema is a table, but real time configuration failed: %s", err)
			return (&pub.ConfigureRealTimeResponse{}).WithFormErrors(msg), nil
		} else {
			return (&pub.ConfigureRealTimeResponse{}).WithSchema(&jsonschema.JSONSchema{
				Property: jsonschema.Property{Description:
				fmt.Sprintf(`The table %s has change tracking enabled and is ready for real time publishing.`, req.Shape.Id)},
			}), nil
		}
	}

	schema, ui := GetRealTimeSchemas()
	err = updateProperty(&schema.Property, func(p *jsonschema.Property) {
		for _, info := range session.SchemaInfo {
			if info.IsChangeTracking {
				p.Enum = append(p.Enum, info.ID)
			}
		}
		sort.Strings(p.Enum)
	}, "tables", "tableName")
	if err != nil {
		panic("schema was malformed")
	}

	err = updateProperty(&schema.Property, func(property *jsonschema.Property) {
		property.Extensions = map[string]interface{}{"uniqueItems": true}
		var cols []string
		for _, p := range req.Shape.Properties {
			cols = append(cols, p.Id)
		}
		sort.Strings(cols)
		property.Items = &jsonschema.Property{Type:"string", Enum: cols }
	}, "keyColumns")
	if err != nil {
		panic("schema was malformed, could not set columns")
	}


	var settings RealTimeSettings
	if req.Form.DataJson != "" {
		err = json.Unmarshal([]byte(req.Form.DataJson), &settings)
		if err != nil {
			return nil, errors.Wrap(err, "form.dataJson was invalid")
		}
	}

	var tableErrors []map[string]interface{}

	for _, table := range settings.Tables {
		tableErrorMap := map[string]interface{}{}
		err = r.ensureTableChangeTrackingEnabled(session, table.TableName)
		if err != nil {
			tableErrorMap["tableName"] = err.Error()
		}
		tableErrors = append(tableErrors, tableErrorMap)
	}

	errMap := SchemaMap{
		"tables": tableErrors,
	}

	form := &pub.ConfigurationFormResponse{
		UiJson:         ui.String(),
		SchemaJson:     schema.String(),
		DataErrorsJson: errMap.String(),
		StateJson:      req.Form.StateJson,
		DataJson:       req.Form.DataJson,
	}

	resp := &pub.ConfigureRealTimeResponse{
		Form: form,
	}

	return resp, nil
}

func (r *RealTimeHelper) PublishStream(session *OpSession, req *pub.PublishRequest, out chan<- *pub.Record) error {
	log := session.Log.Named("RealTimeHelper.PublishStream")

	var realTimeSettings RealTimeSettings
	var realTimeState RealTimeState
	err := json.Unmarshal([]byte(req.RealTimeSettingsJson), &realTimeSettings)
	if err != nil {
		return errors.Wrap(err, "invalid real time settings")
	}
	if req.RealTimeStateJson != "" {
		err = json.Unmarshal([]byte(req.RealTimeStateJson), &realTimeState)
		if err != nil {
			return errors.Wrap(err, "invalid real time state")
		}
	}

	minVersion := realTimeState.Version
	maxVersion, err := getChangeTrackingVersion(session)
	if err != nil {
		return errors.Wrap(err, "get next change tracking version")
	}

	query, err := buildQuery(req)
	if err != nil {
		return errors.Wrap(err, "build query")
	}

	initializeNeeded := false
	if minVersion == 0 {
		log.Info("No change tracking history found, running initial load of entire table.")
		initializeNeeded = true
	} else {
		// We need to check whether the min version from the committed state
		// is still valid. It may have expired if this job was paused for a long time.
		// If the min version is no longer valid, we'll need to re-initialize the data.
		var trackedSchemaIDs []string
		for _, t := range realTimeSettings.Tables {
			trackedSchemaIDs = append(trackedSchemaIDs, t.TableName)
		}
		if len(trackedSchemaIDs) == 0 {
			// no tracked source tables, the schema itself is tracked:
			trackedSchemaIDs = []string{req.Shape.Id}
		}

		for _, schemaID := range trackedSchemaIDs {
			versionValid, err := validateChangeTrackingVersion(session, schemaID, minVersion)
			if err != nil {
				return errors.Wrap(err, "validate version")
			}
			if !versionValid {
				log.Info("Last committed version is no longer valid, running initial load of entire table.")
				initializeNeeded = true
			}
		}
	}

	if initializeNeeded {
		// Run a straight load from the table, using the default query.
		stmt, err := session.DB.Prepare(query)
		if err != nil {
			return errors.Wrap(err, "prepare failed")
		}
		err = readRecordsUsingQuery(session, req.Shape, out, stmt)
		if err != nil {
			return errors.Wrap(err, "initial load of table")
		}

		log.Info("Completed initial load, committing current version checkpoint.")
		// Commit the version we captured before the initial load.
		// Now we want subsequent publishes to begin at the
		// version we got before we did the full load,
		// because we know that we've already published
		// all the changes before that version.
		minVersion = commitVersion(session, out, maxVersion)
	}

	// check if the session has been canceled yet
	if session.Ctx.Err() != nil {
		return nil
	}

	batchQuery, queryText, err := prepareBatchQuery(session, query, realTimeSettings, req)
	if err != nil {
		return errors.Wrap(err, "build batch query")
	}

	interval, _ := time.ParseDuration(realTimeSettings.PollingInterval)
	if interval == 0 {
		interval = 5 * time.Second
	}

	for {
		if session.Ctx.Err() != nil {
			log.Warn("Session context has been cancelled")
			return nil
		}

		// Capture the version for this point in time. We'll publish
		// The publish will get everything that has changed between
		// the previous max version and the current version.
		maxVersion, err = getChangeTrackingVersion(session)
		if err != nil {
			return errors.Wrap(err, "get next change tracking version")
		}
		log.Info("Got max version for this batch of changes", "maxVersion", maxVersion)

		log.Debug("Beginning publish of changes since last commit", "minVersion", minVersion, "maxVersion", maxVersion)

		err = readRecordsUsingQuery(session, req.Shape, out, batchQuery, sql.Named("minVersion", minVersion), sql.Named("maxVersion", maxVersion))
		if err != nil {
			return errors.Errorf("publishing changes using query %s: %s", queryText, err)
		}

		log.Debug("Completed publish of recent changes, committing max version", "maxVersion", maxVersion)
		// commit the most recent version we captured so that if something goes wrong we'll start at that version next time
		minVersion = commitVersion(session, out, maxVersion)

		log.Debug("Waiting until interval elapses before checking for more changes", "interval", interval)
		// wait until interval elapses, then we'll loop again.
		select {
		case <-time.After(interval):
			log.Debug("Interval elapsed")
		case <-session.Ctx.Done():
			log.Info("Session canceled")
			return nil
		}
	}

	return err
}

// commitVersion commits the version by writing out a state commit to the out channel.
// It will return early if the session is cancelled. It returns the version for clarity.
func commitVersion(session *OpSession, out chan<- *pub.Record, version int) int {
	state := RealTimeState{
		Version: version,
	}
	r := &pub.Record{
		Action:            pub.Record_REAL_TIME_STATE_COMMIT,
		RealTimeStateJson: state.String(),
	}

	select {
	case out <- r:
	case <-session.Ctx.Done():
	}
	return version
}

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

func prepareBatchQuery(session *OpSession, query string, settings RealTimeSettings, req *pub.PublishRequest) (*sql.Stmt, string, error) {

	args := batchQueryArgs{
		SchemaQuery: query,
		Columns:req.Shape.Properties,
	}


	trackedTables := settings.Tables
	if len(trackedTables) == 0 {
		// The target is a table, so it doesn't have any source tables.
		// We just use the target itself as the source:
		trackedTables = []RealTimeTableSettings{
			{
				TableName:req.Shape.Id,
			},
		}
		// The keys come from the properties of the shape, which will have
		// key flags on them because they belong to a real table.
		for _, p := range req.Shape.Properties{
			if p.IsKey {
				args.Keys = append(args.Keys, p.Id)
			}
		}
	} else {
		// We're going to be tracking other tables and mapping
		// them to the query which defines the data, so we
		// need to find the things the user labelled as keys for us to
		// bind on.
		for _, p := range settings.KeyColumns {
			args.Keys = append(args.Keys, p)
		}

	}

	for _, tt := range trackedTables {
		btt := batchTrackedTable{
			ID:tt.TableName,
		}

		info, ok := session.SchemaInfo[btt.ID]
		if !ok {
			return nil, "", errors.Errorf("database does not contain metadata about tracked table %q", btt.ID)
		}
		for _, c := range info.Columns {
			if c.IsKey {
				btt.Keys = append(btt.Keys, c.ID)
			} else {
				btt.NonKeys = append(btt.NonKeys, c.ID)
			}
		}
		if tt.Query == "" {
			// There's no query defined, so we will generate an identity query
			btt.SelectQuery = fmt.Sprintf("SELECT %s", strings.Join(btt.Keys, ", "))
			btt.ProjectQuery = ""
		} else {
			segs := dependencyQuerySplitterRE.Split(tt.Query, 2)
			btt.SelectQuery = segs[0]
			if len(segs) > 1 {
				btt.ProjectQuery = segs[1]
			}
		}

		args.TrackedTables = append(args.TrackedTables, btt)
	}

	w := new(strings.Builder)
	err := batchQueryTemplate.Execute(w, args)
	if err != nil {
		return nil, "", errors.Wrap(err, "error formatting query")
	}

	batchQuery := w.String()

	fmt.Println(batchQuery)

	session.Log.Debug("Rendered batch query.", "query", batchQuery)

	stmt, err := session.DB.Prepare(batchQuery)

	return stmt, batchQuery, err
}

// returns true if the provided version is valid; false otherwise.
func validateChangeTrackingVersion(session *OpSession, schemaID string, version int) (bool, error) {
	row := session.DB.QueryRow(`SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@schema))`, sql.Named("schema", schemaID))
	var minValidVersion int
	err := row.Scan(&minValidVersion)
	if err != nil {
		return false, err
	}
	if version < minValidVersion {
		session.Log.Warn("Current version is less than min valid version in database; all data will be re-loaded from source tables.",
			"currentVersion", version,
			"minValidVersion", minValidVersion)
		return false, nil
	}
	return true, nil
}

func getChangeTrackingVersion(session *OpSession) (int, error) {
	row := session.DB.QueryRow(`SELECT CHANGE_TRACKING_CURRENT_VERSION()`)
	var version int
	err := row.Scan(&version)
	session.Log.Debug("Got current version.", "CHANGE_TRACKING_CURRENT_VERSION", version)
	return version, err
}

var dependencyQuerySplitterRE = regexp.MustCompile(`(?i:FROM\s+.*\s+AS Source)`)

var depencyQueryTemplate = compileTemplate("dependencyQuery",
	// language=GoTemplate
	`SELECT /* {{ first .Target.Keys }} */ AS {{ first .Target.Keys }},  {{ range rest .Target.Keys }}, /* {{ . }} */ AS {{ . }}{{ end }}
FROM {{ .Source.ID }} AS Source` )

const naveegoPrefix = "__NAVEEGO__"
const naveegoActionHint = naveegoPrefix + "ACTION"
const naveegoChangeTableToken = "__NAVEEGO_CHANGES__"

func compileTemplate(name, input string) *template.Template {
	t, err := template.New(name).
		Funcs(sprig.TxtFuncMap()).
		Funcs(template.FuncMap{
			"uniquify": uniquify,
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