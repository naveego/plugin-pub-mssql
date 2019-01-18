package internal

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/Masterminds/sprig"
	"github.com/naveego/go-json-schema"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/pkg/errors"
	"html/template"
	"sort"
	"strings"
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
			return !table.IsView, nil
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

	isTable, err := r.isSchemaTable(session, req.SchemaID)
	if err != nil {
		// This shouldn't happen, but if we can't tell whether it's a table or not we can't do anything.
		return nil, err
	}

	if isTable {
		// If the schema is a table and it has change tracking, we have nothing else to configure.
		// Otherwise, we'll show the user an error telling them to configure change tracking for the table.

		err = r.ensureTableChangeTrackingEnabled(session, req.SchemaID)
		if err != nil {
			msg := fmt.Sprintf("The schema is a table, but real time configuration failed: %s", err)
			return (&pub.ConfigureRealTimeResponse{}).WithFormErrors(msg), nil
		} else {
			return (&pub.ConfigureRealTimeResponse{}).WithSchema(&jsonschema.JSONSchema{
				Property: jsonschema.Property{Description:
				fmt.Sprintf(`The table %s has change tracking enabled and is ready for real time publishing.`, req.SchemaID)},
			}), nil
		}
	}

	schema, ui := GetRealTimeSchemas()
	err = updateProperty(&schema.Property, func(p *jsonschema.Property) {
		for id := range session.SchemaInfo {
			p.Enum = append(p.Enum, id)
		}
		sort.Strings(p.Enum)
	}, "tables", "tableName")
	if err != nil {
		panic("schema was malformed")
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

	committedVersion := realTimeState.Version
	mostRecentVersion, err := getChangeTrackingVersion(session)
	if err != nil {
		return errors.Wrap(err, "get next change tracking version")
	}

	query, err := buildQuery(req)
	if err != nil {
		return errors.Wrap(err, "build query")
	}

	if committedVersion == 0 {
		// no prior version committed, we'll start from scratch
		log.Info("No change tracking history found, running initial load of entire table.")
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
		// at least all the changes before that version.
		committedVersion = commitVersion(session, out, mostRecentVersion)
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
			log.Warn("Session context has been cancelled.")
			return nil
		}

		// capture the next version before we do a publish
		mostRecentVersion, err = getChangeTrackingVersion(session)
		if err != nil {
			return errors.Wrap(err, "get next change tracking version")
		}

		log.Debug("Beginning publish of changes since last commit.")

		err = readRecordsUsingQuery(session, req.Shape, out, batchQuery, sql.Named("version", committedVersion))
		if err != nil {
			return errors.Errorf("publishing changes using query %s: %s", queryText, err)
		}

		log.Debug("Completed publish of recent changes, committing new version.")
		// commit the most recent version we captured so that if something goes wrong we'll start at that version next time
		committedVersion = commitVersion(session, out, mostRecentVersion)

		log.Debug("Waiting until interval elapses before checking for more changes.", "interval", interval)
		// wait until interval elapses, then we'll loop again.
		select {
		case <-time.After(interval):
			log.Debug("Interval elapsed.")
		case <-session.Ctx.Done():
			log.Info("Session canceled.")
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

// language=gotemplate
const batchQueryTemplateStr = `
{{ $keys := .Keys }}
SELECT Q.{{ first .Columns }}{{ range (rest .Columns) }}, Q.{{ . }}{{ end }}
FROM (
{{ .SchemaQuery | indent 6 -}}
     ) AS Q
{{- range $TT := .TrackedTables }}
RIGHT OUTER JOIN 
	(
	 SELECT *
     FROM CHANGETABLE(CHANGES {{ $TT.ID }}, @version) AS CT
    ) AS CT
	ON Q.{{ first $keys }} = CT.{{ first $keys }}
{{ range (rest $keys) -}}
	   AND Q.{{ . }} = CT.{{ . }}
{{ end -}}
{{ end }}
`

type batchQueryArgs struct {
	Columns       []string
	Keys          []string
	SchemaQuery   string
	TrackedTables []batchTrackedTable
}

type batchTrackedTable struct {
	ID    string
	Query string
}

var batchQueryTemplate = template.Must(template.New("batch-query").Funcs(sprig.FuncMap()).Parse(batchQueryTemplateStr))

func prepareBatchQuery(session *OpSession, query string, settings RealTimeSettings, req *pub.PublishRequest) (*sql.Stmt, string, error) {

	args := batchQueryArgs{
		SchemaQuery: query,
	}

	// Add the columns and keys to the template args
	for _, p := range req.Shape.Properties {
		args.Columns = append(args.Columns, p.Id)
		if p.IsKey {
			args.Keys = append(args.Keys, p.Id)
		}
	}

	schemaID := req.Shape.Id
	// We have different behaviors for tables and views.
	schemaInfo, ok := session.SchemaInfo[schemaID]
	if !ok {
		return nil, "", errors.Errorf("unknown schema ID %q", schemaID)
	}
	if schemaInfo.IsView {
		// Views use the tracking info from their constituent tables
		for _, table := range settings.Tables {
			args.TrackedTables = append(args.TrackedTables, batchTrackedTable{
				ID: table.TableName,
			})
		}
	} else {
		// Tables provide their tracking directly:
		args.TrackedTables = []batchTrackedTable{
			{ID: schemaID},
		}
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

func getChangeTrackingVersion(session *OpSession) (int, error) {
	row := session.DB.QueryRow(`SELECT CHANGE_TRACKING_CURRENT_VERSION()`)
	var version int
	err := row.Scan(&version)
	session.Log.Debug("Got current version.", "CHANGE_TRACKING_CURRENT_VERSION", version)
	return version, err
}
