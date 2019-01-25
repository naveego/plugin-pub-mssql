package internal

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/naveego/go-json-schema"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/pkg/errors"
	"sort"
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
	}, "tables", "schemaID")
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
		property.Items = &jsonschema.Property{Type: "string", Enum: cols}
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
		err = r.ensureTableChangeTrackingEnabled(session, table.SchemaID)
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