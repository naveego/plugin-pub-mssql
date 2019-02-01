package internal

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/naveego/go-json-schema"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/naveego/plugin-pub-mssql/internal/templates"
	"github.com/pkg/errors"
	"sort"
	"strings"
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

type ConfigurationFormPreResponse struct {
	// The JSONSchema which should be used to build the form.
	Schema *jsonschema.JSONSchema
	// The UI hints which should be provided to the form.
	Ui SchemaMap
	// The state object which should be passed in any future Configure*Request as part of this configuration session.
	StateJson string
	// Current values from the form.
	DataJson string
	// Errors which should be displayed attached to fields in the form,
	// in the form of a JSON object with the same shape as the data object,
	// but the values are arrays of strings containing the error messages.
	DataErrors ErrorMap
	// Generic errors which should be displayed at the bottom of the form,
	// not associated with any specific fields.
	Errors []string
}

func (c ConfigurationFormPreResponse) ToResponse() *pub.ConfigureRealTimeResponse {
	return &pub.ConfigureRealTimeResponse{
		Form: &pub.ConfigurationFormResponse{
			UiJson:         c.Ui.String(),
			SchemaJson:     c.Schema.String(),
			DataJson:       c.DataJson,
			StateJson:      c.StateJson,
			Errors:         c.Errors,
			DataErrorsJson: c.DataErrors.String(),
		},
	}
}

func (r *RealTimeHelper) ConfigureRealTime(session *OpSession, req *pub.ConfigureRealTimeRequest) (*pub.ConfigureRealTimeResponse, error) {
	var err error

	err = r.ensureDBChangeTrackingEnabled(session)

	if err != nil {
		// If change tracking is disabled on the DB we can't do much.
		return (&pub.ConfigureRealTimeResponse{}).WithFormErrors(err.Error()), nil
	}

	jsonSchema, uiSchema := GetRealTimeSchemas()

	resp := ConfigurationFormPreResponse{
		DataJson:   req.Form.DataJson,
		StateJson:  req.Form.StateJson,
		Schema:     jsonSchema,
		Ui:         uiSchema,
		DataErrors: ErrorMap{},
	}

	schemaInfo := session.SchemaInfo[req.Shape.Id]
	if schemaInfo == nil || !schemaInfo.IsTable {
		schemaInfo = MetaSchemaFromShape(req.Shape)
	} else {
		// If the schema is a table and it has change tracking, we have nothing else to configure.
		// Otherwise, we'll show the user an error telling them to configure change tracking for the table.

		err = r.ensureTableChangeTrackingEnabled(session, req.Shape.Id)
		if err != nil {
			resp.Errors = []string{fmt.Sprintf("The schema is a table, but real time configuration failed: %s", err)}
			return resp.ToResponse(), nil
		} else {

			delete(jsonSchema.Properties, "tables")
			jsonSchema.Property.Description = fmt.Sprintf(`The table %s has change tracking enabled and is ready for real time publishing.`, req.Shape.Id)

			return resp.ToResponse(), nil
		}
	}

	err = updateProperty(&jsonSchema.Property, func(p *jsonschema.Property) {
		for _, info := range session.SchemaInfo {
			if info.IsChangeTracking {
				p.Enum = append(p.Enum, info.ID)
			}
		}
		sort.Strings(p.Enum)
	}, "tables", "schemaID")
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

	if len(settings.Tables) == 0 {
		resp.DataErrors.GetOrAddChild("tables").AddError("At least one table is required.")
	} else {

		allTableErrors := resp.DataErrors.GetOrAddChild("tables")

		for i, table := range settings.Tables {
			tableErrors := allTableErrors.GetOrAddChild(i)
			err = r.ensureTableChangeTrackingEnabled(session, table.SchemaID)
			if err != nil {
				tableErrors.GetOrAddChild("schemaID").AddError(err.Error())
			}

			expectedQueryKeys := map[string]bool{}

			depSchema := session.SchemaInfo[table.SchemaID]
			if depSchema == nil {
				tableErrors.GetOrAddChild("schemaID").AddError("Invalid table %q.", table.SchemaID)
				continue
			}
			for _, k := range depSchema.Keys() {
				expectedQueryKeys[templates.PrefixColumn("Dependency", k)] = false
			}

			for _, k := range schemaInfo.Keys() {
				expectedQueryKeys[templates.PrefixColumn("Schema", k)] = false
			}

			if table.Query == "" {
				tableErrors.GetOrAddChild("query").AddError("Query is required.")
			} else {
				metadata, err := describeResultSet(session, table.Query)
				if err != nil {
					tableErrors.GetOrAddChild("query").AddError("Query failed: %s", err)
				} else {
					for _, col := range metadata {
						safeName := fmt.Sprintf("[%s]", strings.Trim(col.Name, "[]"))
						expectedQueryKeys[safeName] = true
					}
					var missing []string
					for name, detected := range expectedQueryKeys {
						if !detected {
							missing = append(missing, name)
						}
					}
					sort.Strings(missing)
					if len(missing) > 0 {
						tableErrors.GetOrAddChild("query").AddError(`One or more required columns not found in query. Missing column(s): %s`, strings.Join(missing, ", "))
					}
				}
			}

		}
	}

	return resp.ToResponse(), nil
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
	session.Log.Debug("Got current version", "version", version)
	return version, err
}
