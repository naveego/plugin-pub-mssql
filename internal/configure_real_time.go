package internal

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	jsonschema "github.com/naveego/go-json-schema"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/naveego/plugin-pub-mssql/internal/templates"
	"github.com/pkg/errors"
)

const CustomTargetOption string = "Custom Target"

type RealTimeHelper struct {
	dbChangeTrackingEnabled    map[string]bool
	tableChangeTrackingEnabled map[string]bool
}

func (r *RealTimeHelper) ensureDBChangeTrackingEnabled(session *OpSession, dbName string) error {
	if r.dbChangeTrackingEnabled == nil {
		r.dbChangeTrackingEnabled = map[string]bool{}
	}

	enabled, ok := r.dbChangeTrackingEnabled[dbName]
	if ok && enabled {
		return nil
	}

	row := session.DB.QueryRowContext(session.Ctx,
		fmt.Sprintf(`SELECT count(1)
FROM [%s].sys.change_tracking_databases
WHERE database_id=DB_ID(@ID)`, dbName), sql.Named("ID", dbName))

	var count int
	err := row.Scan(&count)
	if err == nil && count == 0 {
		err = errors.New("Database does not have change tracking enabled. " +
			"See https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-tracking-sql-server for details on enabling change tracking.")
	}
	if err != nil {
		session.Log.Error("ensure db change tracking error: %s", "error", err.Error())
		return err
	}

	r.dbChangeTrackingEnabled[dbName] = true
	return nil
}

func (r *RealTimeHelper) ensureTableChangeTrackingEnabled(session *OpSession, dbName string, schemaID string) error {
	session.Log.Debug("ensure change tracking", "dbName", dbName, "schemaID", schemaID)

	err := r.ensureDBChangeTrackingEnabled(session, dbName)
	if err != nil {
		// If change tracking is disabled on the DB we can't do much.
		return err
	}

	session.Log.Debug("Database has change tracking enabled", "database", dbName)

	if r.tableChangeTrackingEnabled == nil {
		r.tableChangeTrackingEnabled = map[string]bool{}
	}

	enabled, ok := r.tableChangeTrackingEnabled[schemaID]
	if ok && enabled {
		return nil
	}

	row := session.DB.QueryRow(
		fmt.Sprintf(`SELECT count(1)
FROM [%s].sys.change_tracking_tables
WHERE object_id=OBJECT_ID(@ID)`, dbName), sql.Named("ID", schemaID))
	var count int
	err = row.Scan(&count)
	if err == nil && count == 0 {
		session.Log.Debug("change tracking not enabled", "dbName", dbName, "schemaID", schemaID)
		err = errors.New("Table does not have change tracking enabled. See https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-tracking-sql-server for details on enabling change tracking.")
	}
	if err != nil {
		session.Log.Error("ensure table change tracking error: %s", "error", err.Error())
		return err
	}

	r.tableChangeTrackingEnabled[schemaID] = true
	session.Log.Debug("Table has change tracking enabled", "table", schemaID)
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

	// get form ui json
	jsonSchema, uiSchema := GetRealTimeSchemas()

	resp := ConfigurationFormPreResponse{
		DataJson:   req.Form.DataJson,
		StateJson:  req.Form.StateJson,
		Schema:     jsonSchema,
		Ui:         uiSchema,
		DataErrors: ErrorMap{},
	}

	// attempt to auto validate target schema if schema is a table
	schemaInfo := session.SchemaInfo[req.Schema.Id]
	if schemaInfo == nil || !schemaInfo.IsTable {
		schemaInfo = MetaSchemaFromPubSchema(req.Schema)
	} else {
		// If the schema is a table and it has change tracking, we have nothing else to configure.
		// Otherwise, we'll show the user an error telling them to configure change tracking for the table.
		err = r.ensureTableChangeTrackingEnabled(session, session.Settings.Database, req.Schema.Id)
		if err == nil {
			// target schema table is already configured
			delete(jsonSchema.Properties, "tables")
			jsonSchema.Property.Description = fmt.Sprintf("The table `%s` has change tracking enabled and is ready for real time publishing.", req.Schema.Id)

			return resp.ToResponse(), nil
		}
	}

	// attempt to auto populate the schema id list with tables that have change tracking enabled
	err = updateProperty(&jsonSchema.Property, func(p *jsonschema.Property) {
		for _, info := range session.SchemaInfo {
			if info.IsChangeTracking {
				p.Enum = append(p.Enum, info.ID)
			}
		}
		sort.Strings(p.Enum)
		p.Enum = append(p.Enum, CustomTargetOption)
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

			if table.SchemaID == CustomTargetOption {
				dbName, _, _ := DecomposeSafeName(table.CustomTarget)
				err = r.ensureTableChangeTrackingEnabled(session, dbName, table.CustomTarget)
				if err != nil {
					tableErrors.GetOrAddChild("schemaID").AddError(err.Error())
					continue
				}
			} else {
				err = r.ensureTableChangeTrackingEnabled(session, session.Settings.Database, table.SchemaID)
				if err != nil {
					tableErrors.GetOrAddChild("schemaID").AddError(err.Error())
					continue
				}
			}

			expectedQueryKeys := map[string]bool{}

			depSchema := session.SchemaInfo[table.SchemaID]
			if depSchema == nil {
				if table.SchemaID == CustomTargetOption {
					depSchema, err = getMetaSchemaForSchemaId(session, table.CustomTarget)
					if err != nil {
						tableErrors.GetOrAddChild("schemaID").AddError(err.Error())
						continue
					}
				} else {
					tableErrors.GetOrAddChild("schemaID").AddError("Invalid table `%s`.", table.SchemaID)
					continue
				}
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
					continue
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
						tableErrors.GetOrAddChild("query").AddError("One or more required columns not found in query. Missing column(s): `%s`", strings.Join(missing, ", "))
					}
				}
			}
		}
	}

	return resp.ToResponse(), nil
}

func getTableSchemaId(table RealTimeTableSettings) string {
	if table.SchemaID == CustomTargetOption {
		return table.CustomTarget
	}

	return table.SchemaID
}

func getMetaSchemaForSchemaId(session *OpSession, schemaId string) (*meta.Schema, error) {
	schema := new(pub.Schema)
	schema.Query = fmt.Sprintf("SELECT * FROM %s", schemaId)
	schema.Id = schemaId

	_, err := DiscoverSchemasSync(session, session.SchemaDiscoverer, &pub.DiscoverSchemasRequest{
		Mode:       pub.DiscoverSchemasRequest_REFRESH,
		SampleSize: 0,
		ToRefresh: []*pub.Schema{
			schema,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Unable to retrieve schema for target table `%s`.", schemaId))
	}

	metaSchemaInfo := MetaSchemaFromPubSchema(schema)
	session.SchemaInfo[schemaId] = metaSchemaInfo

	return metaSchemaInfo, nil
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
	dbName, _, _ := DecomposeSafeName(schemaID)

	db, err := getDbHandleForDatabase(session, dbName)
	if err  != nil {
		return false, err
	}

	row := db.QueryRow(`SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@schema))`, sql.Named("schema", schemaID))
	var minValidVersion int
	err = row.Scan(&minValidVersion)
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

func getChangeTrackingVersion(session *OpSession, versionedTableIDs []string) (VersionSet, error) {
	out := VersionSet{}

	for _, schemaID := range versionedTableIDs {
		dbName, _, _ := DecomposeSafeName(schemaID)

		db, err := getDbHandleForDatabase(session, dbName)
		if err  != nil {
			return nil, err
		}

		row := db.QueryRow(`SELECT CHANGE_TRACKING_CURRENT_VERSION()`)
		var version int
		err = row.Scan(&version)
		if err != nil {
			return nil, err
		}
		out[schemaID] = version
		session.Log.Debug("Got current version", "database", dbName, "version", version)
	}

	return out, nil
}

func getDbHandleForDatabase(session *OpSession, dbName string) (*sql.DB, error) {
	db := session.DB

	if session.Settings.Database != dbName && dbName != "" {
		if session.DbHandles == nil {
			session.DbHandles = map[string]*sql.DB{}
		}

		var ok bool
		db, ok = session.DbHandles[dbName]
		if !ok {
			settingsJson, err := json.Marshal(session.Settings)
			if err != nil {
				return nil, errors.Errorf("could serialize settings: %s", err)
			}

			var localSettings *Settings
			err = json.Unmarshal(settingsJson, &localSettings)
			if err != nil {
				return nil, errors.Errorf("could deserialize settings: %s", err)
			}
			localSettings.Database = dbName

			connectionString, err := localSettings.GetConnectionString()
			if err != nil {
				return nil, errors.Errorf("could not get connection string: %s", err)
			}
			db, err = sql.Open("sqlserver", connectionString)
			if err != nil {
				return nil, errors.Errorf("could not open connection: %s", err)
			}
			err = session.DB.Ping()
			if err != nil {
				return nil, err
			}

			session.DbHandles[dbName] = db
		}
	}

	return db, nil
}
