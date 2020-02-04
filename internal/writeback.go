package internal

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/naveego/plugin-pub-mssql/internal/constants"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/naveego/plugin-pub-mssql/internal/templates"
	"github.com/naveego/plugin-pub-mssql/pkg/canonical"
	"github.com/naveego/plugin-pub-mssql/pkg/sqlstructs"
	"github.com/pkg/errors"
	"strings"
	"time"
)

const (
	ReplicationAPIVersion = 1
)

type Writer interface {
	Write(session *OpSession, record *pub.UnmarshalledRecord) error
}

func PrepareWriteHandler(session *OpSession, req *pub.PrepareWriteRequest) (Writer, error) {

	if req.Replication == nil {
		return NewDefaultWriteHandler(session, req)
	}

	return NewReplicationWriteHandler(session, req)

}

type DefaultWriteHandler struct {
	WriteSettings *WriteSettings
}

func NewDefaultWriteHandler(session *OpSession, req *pub.PrepareWriteRequest) (Writer, error) {

	d := &DefaultWriteHandler{}

	d.WriteSettings = &WriteSettings{
		Schema:    req.Schema,
		CommitSLA: req.CommitSlaSeconds,
	}

	schemaJSON, _ := json.MarshalIndent(req.Schema, "", "  ")

	session.Log.Debug("Prepared to write.", "commitSLA", req.CommitSlaSeconds, "schema", string(schemaJSON))

	return d, nil
}

func (d *DefaultWriteHandler) Write(session *OpSession, record *pub.UnmarshalledRecord) error {

	var err error

	schema := d.WriteSettings.Schema

	// build params for stored procedure
	var args []interface{}
	for _, prop := range schema.Properties {

		rawValue := record.Data[prop.Id]
		var value interface{}
		switch prop.Type {
		case pub.PropertyType_DATE, pub.PropertyType_DATETIME:
			stringValue, ok := rawValue.(string)
			if !ok {
				value = nil
			} else {
				value, err = time.Parse(time.RFC3339, stringValue)
			}

			if err != nil {
				return errors.Errorf("invalid date time %q: %s", stringValue, err)
			}

		default:
			value = rawValue
		}

		args = append(args, sql.Named(prop.Id, value))
	}

	// call stored procedure and capture any error
	_, err = session.DB.Exec(schema.Query, args...)
	if err != nil {
		return errors.Wrapf(err, "could not write: query: %s; args: %v", schema.Query, args)
	}
	return nil
}

type ReplicationWriter struct {
	req          *pub.PrepareWriteRequest
	GoldenIDMap  map[string]string
	VersionIDMap map[string]string
	GoldenMetaSchema *meta.Schema
	VersionMetaSchema *meta.Schema
	changes      []string
}

func NewReplicationWriteHandler(session *OpSession, req *pub.PrepareWriteRequest) (Writer, error) {

	w := &ReplicationWriter{
		req: req,
	}

	var settings ReplicationSettings
	if err := json.Unmarshal([]byte(req.Replication.SettingsJson), &settings); err != nil {
		return nil, errors.Wrapf(err, "invalid replication settings %s", req.Replication.SettingsJson)
	}

	sqlSchema := settings.SQLSchema

	// Ensure that we have the correct tables in place:
	_, err := templates.ExecuteCommand(session.DB, templates.ReplicationMetadataDDLArgs{
		SQLSchema: sqlSchema,
	})
	if err != nil {
		return nil, errors.Wrap(err, "ensure replication supporting tables exist")
	}

	// get the most recent versioning record
	query := fmt.Sprintf(`select top (1) * 
from [%s].[%s]
where ReplicatedShapeID = '%s'
order by id desc`, sqlSchema, constants.ReplicationVersioningTable, req.Schema.Id)

	rows, err := session.DB.Query(query)
	if err != nil {
		return nil, errors.Wrap(err, "get replication metadata")
	}

	var previousMetadata NaveegoReplicationVersioning
	var previousMetadataSettings NaveegoReplicationVersioningSettings
	naveegoReplicationMetadataRows := make([]NaveegoReplicationVersioning, 0, 0)
	err = sqlstructs.UnmarshalRows(rows, &naveegoReplicationMetadataRows)
	if err != nil {
		return nil, err
	}
	if len(naveegoReplicationMetadataRows) > 0 {
		previousMetadata = naveegoReplicationMetadataRows[0]
		previousMetadataSettings = previousMetadata.GetSettings()

		// check if golden record and version table names have changed
		if previousMetadataSettings.Settings.GetNamespacedVersionRecordTable() != settings.GetNamespacedVersionRecordTable() {
			// version table name has changed
			session.Log.Debug("version table name changed")
			if err := w.dropTable(session, previousMetadataSettings.Settings.GetNamespacedVersionRecordTable()); err != nil {
				return nil, errors.Wrap(err, "dropping version table after name change")
			}
		}
		if previousMetadataSettings.Settings.GetNamespacedGoldenRecordTable() != settings.GetNamespacedGoldenRecordTable() {
			// golden table name has changed
			session.Log.Debug("golden record table name changed")
			if err := w.dropTable(session, previousMetadataSettings.Settings.GetNamespacedGoldenRecordTable()); err != nil {
				return nil, errors.Wrap(err, "dropping golden table after name change")
			}
		}

		if previousMetadataSettings.Request.DataVersions != nil {
			// check if job data version has changed
			if req.DataVersions.JobDataVersion > previousMetadataSettings.Request.DataVersions.JobDataVersion {
				session.Log.Debug("job data version changed", "previous", previousMetadataSettings.Request.DataVersions.JobDataVersion, "current", req.DataVersions.JobDataVersion)
				if err := w.dropTable(session, previousMetadataSettings.Settings.GetNamespacedVersionRecordTable()); err != nil {
					return nil, errors.Wrap(err, "dropping version table after job data version change")
				}
				if err := w.dropTable(session, previousMetadataSettings.Settings.GetNamespacedGoldenRecordTable()); err != nil {
					return nil, errors.Wrap(err, "dropping golden table after job data version change")
				}
			}

			// check if shape data version has changed
			if req.DataVersions.ShapeDataVersion > previousMetadataSettings.Request.DataVersions.ShapeDataVersion {
				session.Log.Debug("shape data version changed", "previous", previousMetadataSettings.Request.DataVersions.ShapeDataVersion, "current", req.DataVersions.ShapeDataVersion)
				if err := w.dropTable(session, previousMetadataSettings.Settings.GetNamespacedVersionRecordTable()); err != nil {
					return nil, errors.Wrap(err, "dropping version table after shape data version change")
				}
				if err := w.dropTable(session, previousMetadataSettings.Settings.GetNamespacedGoldenRecordTable()); err != nil {
					return nil, errors.Wrap(err, "dropping golden table after shape data version change")
				}
			}
		}
	}

	var goldenSchema *pub.Schema
	var versionSchema *pub.Schema
	schemaJson, err := json.Marshal(req.Schema)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid schema")
	}

	_ = json.Unmarshal(schemaJson, &goldenSchema)
	_ = json.Unmarshal(schemaJson, &versionSchema)

	session.Log.Debug("version schema before", "schema", fmt.Sprintf("%v", versionSchema))
	session.Log.Debug("golden schema before", "schema", fmt.Sprintf("%v", goldenSchema))
	session.Log.With("config", fmt.Sprintf("%v", settings.PropertyConfiguration)).Debug("property config")

	w.augmentGoldenProperties(goldenSchema)
	goldenSchema.Id = GetSchemaID(settings.SQLSchema, settings.GoldenRecordTable)
	w.GoldenIDMap = w.canonicalizeProperties(goldenSchema)

	w.augmentVersionProperties(versionSchema)
	versionSchema.Id = GetSchemaID(settings.SQLSchema, settings.VersionRecordTable)
	w.VersionIDMap = w.canonicalizeProperties(versionSchema)

	var toRefreshNew []*pub.Schema
	toRefresh := []*pub.Schema{
		goldenSchema,
		versionSchema,
	}
	toRefreshJson, _ := json.Marshal(toRefresh)
	_ = json.Unmarshal(toRefreshJson, &toRefreshNew)

	discoveredSchemas, err := DiscoverSchemasSync(session, session.SchemaDiscoverer, &pub.DiscoverSchemasRequest{
		Mode:       pub.DiscoverSchemasRequest_REFRESH,
		SampleSize: 0,
		ToRefresh: toRefreshNew,
	})
	if err != nil {
		return nil, errors.Wrap(err, "checking for owned schemas")
	}

	var existingGoldenSchema *pub.Schema
	var existingVersionSchema *pub.Schema
	for _, schema := range discoveredSchemas {
		if len(schema.Errors) > 0 {
			continue
		}
		if schema.Id == settings.GetNamespacedGoldenRecordTable() {
			existingGoldenSchema = schema
			session.Log.With("existing golden schema", fmt.Sprintf("%v", existingGoldenSchema)).Debug("found existing golden record schema")
		}
		if schema.Id == settings.GetNamespacedVersionRecordTable() {
			existingVersionSchema = schema
			session.Log.With("existing version schema", fmt.Sprintf("%v", existingVersionSchema)).Debug("found existing version schema")
		}
	}

	var customGoldenSchema *pub.Schema
	goldenSchemaJson, _ := json.Marshal(goldenSchema)
	_ = json.Unmarshal(goldenSchemaJson, &customGoldenSchema)

	var customVersionSchema *pub.Schema
	customVersionSchemaJson, _ := json.Marshal(versionSchema)
	_ = json.Unmarshal(customVersionSchemaJson, &customVersionSchema)

	applyCustomSQLTypes(customGoldenSchema, settings.PropertyConfiguration)
	applyCustomSQLTypes(customVersionSchema, settings.PropertyConfiguration)

	session.Log.Debug("version schema after", "schema", fmt.Sprintf("%v", customVersionSchema))
	session.Log.Debug("golden schema after", "schema", fmt.Sprintf("%v", customGoldenSchema))

	if err := w.reconcileSchemas(session, existingVersionSchema, customVersionSchema); err != nil {
		return nil, errors.Wrap(err, "reconciling version schema")
	}
	if err := w.reconcileSchemas(session, existingGoldenSchema, customGoldenSchema); err != nil {
		return nil, errors.Wrap(err, "reconciling golden schema")
	}

	if len(req.Replication.Versions) > 0 {

		metadataMergeArgs := templates.ReplicationMetadataMerge{
			SQLSchema:sqlSchema,
		}
		var entries []templates.ReplicationMetadataEntry
		for _, version := range req.Replication.Versions {
			entries = append(entries,
				templates.ReplicationMetadataEntry{Kind: "Job", ID: version.JobId, Name: version.JobName},
				templates.ReplicationMetadataEntry{Kind: "Connection", ID: version.ConnectionId, Name: version.ConnectionName},
				templates.ReplicationMetadataEntry{Kind: "Schema", ID: version.SchemaId, Name: version.SchemaName},
				)
		}

		// the same resource may be in the list more than once
		// so we need to de-duplicate them or the merge won't work
		dups := make(map[string]bool)
		for _, entry := range entries {
			if !dups[entry.ID] {
				dups[entry.ID] = true
				metadataMergeArgs.Entries = append(metadataMergeArgs.Entries, entry)
			}
		}

		_, err = templates.ExecuteCommand(session.DB, metadataMergeArgs)
		if err != nil {
			return nil, errors.Wrap(err, "merge metadata about versions")
		}
	}


	// If we made any changes to the database, insert a new versioning record
	if len(w.changes) > 0 {

		session.Log.Info("Made changes to database during prep, will save new metadata.")

		metadataSettings := NaveegoReplicationVersioningSettings{
			Settings:      settings,
			Request:       req,
			Changes:       w.changes,
			GoldenSchema:  customGoldenSchema,
			VersionSchema: customVersionSchema,
		}
		versioning := NaveegoReplicationVersioning{
			Settings:            metadataSettings.JSON(),
			Timestamp:           time.Now().UTC(),
			APIVersion:          ReplicationAPIVersion,
			ReplicatedShapeID:   req.Schema.Id,
			ReplicatedShapeName: req.Schema.Name,
		}

		err = sqlstructs.Insert(session.DB, fmt.Sprintf("[%s].[%s]",sqlSchema, "NaveegoReplicationVersioning"), versioning)

		if err != nil {
			return nil, errors.Wrapf(err, "saving metadata %s", versioning.Settings)
		}
	}


	// Capture schemas for use during write.
	w.GoldenMetaSchema = MetaSchemaFromPubSchema(customGoldenSchema)
	w.VersionMetaSchema = MetaSchemaFromPubSchema(customVersionSchema)

	return w, nil
}


func (r *ReplicationWriter) Write(session *OpSession, record *pub.UnmarshalledRecord) error {

	// Canonicalize all the fields in the record data and the version data
	record.Data[constants.GroupID] = record.RecordId
	record.Data = r.getCanonicalizedMap(record.Data, r.GoldenIDMap)
	for _, version := range record.UnmarshalledVersions {
		version.Data[constants.GroupID] = record.RecordId
		version.Data[constants.RecordID] = version.RecordId
		version.Data[constants.JobID] = version.JobId
		version.Data[constants.ConnectionID] = version.ConnectionId
		version.Data[constants.SchemaID] = version.SchemaId
		version.Data = r.getCanonicalizedMap(version.Data, r.VersionIDMap)
	}

	// Merge group data
	_, err := templates.ExecuteCommand(session.DB, templates.ReplicationGoldenMerge{
		Schema: r.GoldenMetaSchema,
		Record:record,
	})

	if err != nil {
		return errors.Wrapf(err, "group merge query")
	}

	// Merge version data
	_, err = templates.ExecuteCommand(session.DB, templates.ReplicationVersionMerge{
		Schema:r.VersionMetaSchema,
		Record:record,
	})

	if err != nil {
		return errors.Wrapf(err, "version merge query")
	}

	return nil
}

func (r *ReplicationWriter) recordChange(f string, args ...interface{}) {
	r.changes = append(r.changes, fmt.Sprintf(f, args...))
}

func (r *ReplicationWriter) reconcileSchemas(session *OpSession, current *pub.Schema, desired *pub.Schema) error {
	session.Log.Debug("reconcile schemas", "current", fmt.Sprintf("%v", current), "desired", fmt.Sprintf("%v", desired))
	needsDelete := false
	needsCreate := false
	if current == nil {
		needsCreate = true
	} else {
		//needsDelete = r.arePropsSame(current, desired) || r.arePropsSame(desired, current)
		needsDelete = !r.arePropsSame(current, desired)
		needsCreate = needsDelete
	}

	session.Log.Debug("reconcile schemas", "needsDelete", needsDelete, "needsCreate", needsCreate)

	if needsDelete && current != nil {
		session.Log.Debug("deleting table", "schema id", desired.Id)
		if err := r.dropTable(session, current.Id); err != nil {
			session.Log.Error("Could not drop table.", "table", current.Id, "err", err)
		}
	}

	if needsCreate {
		session.Log.Debug("creating table", "schema id", desired.Id)
		if err := r.createTable(session, desired); err != nil {
			session.Log.Error("Could not create table.", "table", desired, "err", err)
			return errors.Wrapf(err, "create table")
		}
	}

	return nil
}

func (r *ReplicationWriter) dropTable(session *OpSession, table string) error {
	_, err := session.DB.Exec(fmt.Sprintf(`IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s`, table, table))
	r.recordChange("Dropped table %q (if it existed) because of changes.", table)
	return err
}

func (r *ReplicationWriter) arePropsSame(left, right *pub.Schema) (same bool) {
	if len(left.Properties) != len(right.Properties) {
		return false
	}
	rightProps := map[string]*pub.Property{}
	for _, prop := range right.Properties {
		rightProps[prop.Id] = prop
	}

	for _, leftProp := range left.Properties {
		rightProp, ok := rightProps[leftProp.Id]
		if !ok {
			return false
		}
		if rightProp.Type != leftProp.Type {
			return false
		}
		if rightProp.TypeAtSource != leftProp.TypeAtSource {
			return false
		}
	}

	return true
}

func (r *ReplicationWriter) canonicalizeProperties(schema *pub.Schema) map[string]string {
	c := canonical.Strings(canonical.ToAlphanumeric, canonical.ToPascalCase)

	m := map[string]string{}

	for _, property := range schema.Properties {
		canonicalID := fmt.Sprintf("[%s]",c.Canonicalize(property.Name))
		m[property.Id] = canonicalID
		property.Id = canonicalID
	}
	return m
}

func (r *ReplicationWriter) createTable(session *OpSession, schema *pub.Schema) error {

	args := templates.ReplicationTableCreationDDL{
		Schema: MetaSchemaFromPubSchema(schema),
	}
	command, err := templates.RenderReplicationTableCreationDDLArgs(args)
	if err != nil {
		return errors.Wrapf(err, "rendering command for creating table using args %s", args)
	}

	_, err = session.DB.Exec(command)
	if err != nil {
		return errors.Wrapf(err, "executing table creation command\n %s", command)
	}

	r.recordChange("Created table %q.", schema.Id)

	return nil
}

func (r *ReplicationWriter) getCanonicalizedMap(data map[string]interface{}, idMap map[string]string) map[string]interface{} {
	out := make(map[string]interface{},len(data))
	for k, v := range data {
		if newKey, ok := idMap[k]; ok {
			out[newKey] = v
		} else {
			out[k] = v
		}
	}
	return out
}

func (r *ReplicationWriter) augmentVersionProperties(schema *pub.Schema) {
	versionPropertyNames := []string{
		constants.ConnectionID,
		constants.SchemaID,
	}

	schema.Properties = append(schema.Properties,
		&pub.Property{
			Id:           constants.RecordID,
			Name:         constants.RecordID,
			Type:         pub.PropertyType_STRING,
			TypeAtSource: "VARCHAR(44)", // fits a base64 encoded SHA256
			IsKey:        true,
		},
		&pub.Property{
			Id:           constants.JobID,
			Name:         constants.JobID,
			Type:         pub.PropertyType_STRING,
			TypeAtSource: "VARCHAR(44)",
			IsKey:        true,
		},
		&pub.Property{
			Id:           constants.GroupID,
			Name:         constants.GroupID,
			Type:         pub.PropertyType_STRING,
			TypeAtSource: "VARCHAR(44)",
		},
		&pub.Property{
			Id:           constants.CreatedAt,
			Name:         constants.CreatedAt,
			Type:         pub.PropertyType_DATETIME,
			TypeAtSource: "DATETIME",
		},
		&pub.Property{
			Id:           constants.UpdatedAt,
			Name:         constants.UpdatedAt,
			Type:         pub.PropertyType_DATETIME,
			TypeAtSource: "DATETIME",
		},
		)

	for _, name := range versionPropertyNames {
		property := &pub.Property{
			Id:   name,
			Name: strings.Trim(name, "[]"),
			Type: pub.PropertyType_STRING,
		}
		schema.Properties = append(schema.Properties, property)
	}
}

func (r *ReplicationWriter) augmentGoldenProperties(goldenSchema *pub.Schema) {
	goldenSchema.Properties = append([]*pub.Property{
		{
			Id:           constants.GroupID,
			Name:         constants.GroupID,
			IsKey:        true,
			Type:         pub.PropertyType_STRING,
			TypeAtSource: "VARCHAR(44)", // fits an RID or a SHA256 if we change group ID convention
		},
		{
			Id:           constants.CreatedAt,
			Name:         constants.CreatedAt,
			Type:         pub.PropertyType_DATETIME,
			TypeAtSource: "DATETIME",
		},
		{
			Id:           constants.UpdatedAt,
			Name:         constants.UpdatedAt,
			Type:         pub.PropertyType_DATETIME,
			TypeAtSource: "DATETIME",
		},
	}, goldenSchema.Properties...)
}

func applyCustomSQLTypes(schema *pub.Schema, propertyConfig []PropertyConfig){
	for _, property := range schema.Properties {
		for _, config := range propertyConfig {
			if strings.Trim(config.Name, "[]") == strings.Trim(property.Name, "[]") {
				property.Type = meta.ConvertSQLTypeToPluginType(strings.ToLower(config.Type), -1)
				property.TypeAtSource = config.Type
			}
		}
	}
}


type NaveegoReplicationVersioning struct {
	ID                  int       `sql:"ID" sqlkey:"true"`
	Timestamp           time.Time `sql:"Timestamp"`
	APIVersion          int       `sql:"APIVersion"`
	ReplicatedShapeID   string    `sql:"ReplicatedShapeID"`
	ReplicatedShapeName string    `sql:"ReplicatedShapeName"`
	Settings            string    `sql:"Settings"`
}

func (n NaveegoReplicationVersioning) GetSettings() NaveegoReplicationVersioningSettings {
	var s NaveegoReplicationVersioningSettings
	_ = json.Unmarshal([]byte(n.Settings), &s)
	return s
}

type NaveegoReplicationVersioningSettings struct {
	Changes       []string
	Settings      ReplicationSettings
	GoldenSchema  *pub.Schema
	VersionSchema *pub.Schema
	Request       *pub.PrepareWriteRequest
}

func (s NaveegoReplicationVersioningSettings) JSON() string {
	j, _ := json.MarshalIndent(s, "", "  ")
	return string(j)
}
