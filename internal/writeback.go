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
		return errors.Wrap(err, "could not write")
	}
	return nil
}

type ReplicationWriter struct {
	req               *pub.PrepareWriteRequest
	GoldenIDMap       map[string]string
	VersionIDMap      map[string]string
	GoldenMetaSchema  *meta.Schema
	VersionMetaSchema *meta.Schema
	changes           []string
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
	requestedVersionTableName := settings.GetNamespacedVersionRecordTable()
	requestedGoldenTableName := settings.GetNamespacedGoldenRecordTable()
	var previousVersionTableSchema *pub.Schema
	var previousGoldenTableSchema *pub.Schema

	if len(naveegoReplicationMetadataRows) > 0 {
		previousMetadata = naveegoReplicationMetadataRows[0]
		previousMetadataSettings = previousMetadata.GetSettings()

		previousGoldenTableSchema = previousMetadataSettings.GoldenSchema
		previousVersionTableSchema = previousMetadataSettings.VersionSchema

		previousVersionTableName := previousMetadataSettings.Settings.GetNamespacedVersionRecordTable()
		if previousVersionTableName != requestedVersionTableName {
			// version table name has changed
			if err := w.dropTable(session, previousVersionTableName); err != nil {
				return nil, errors.Wrap(err, "dropping version table after name change")
			}
		}
		previousGoldenTableName := previousMetadataSettings.Settings.GetNamespacedGoldenRecordTable()
		if previousGoldenTableName != requestedGoldenTableName {
			// golden table name has changed
			if err := w.dropTable(session, previousGoldenTableName); err != nil {
				return nil, errors.Wrap(err, "dropping golden table after name change")
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

	w.augmentGoldenProperties(goldenSchema)
	goldenSchema.Id = GetSchemaID(settings.SQLSchema, settings.GoldenRecordTable)
	w.GoldenIDMap = w.canonicalizeProperties(goldenSchema)

	w.augmentVersionProperties(versionSchema)
	versionSchema.Id = GetSchemaID(settings.SQLSchema, settings.VersionRecordTable)
	w.VersionIDMap = w.canonicalizeProperties(versionSchema)

	if err := w.reconcileSchemas(session, previousVersionTableSchema, versionSchema); err != nil {
		return nil, errors.Wrap(err, "reconciling version schema")
	}
	if err := w.reconcileSchemas(session, previousGoldenTableSchema, goldenSchema); err != nil {
		return nil, errors.Wrap(err, "reconciling golden schema")
	}

	if len(req.Replication.Versions) > 0 {

		metadataMergeArgs := templates.ReplicationMetadataMerge{
			SQLSchema: sqlSchema,
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
			GoldenSchema:  goldenSchema,
			VersionSchema: versionSchema,
		}
		versioning := NaveegoReplicationVersioning{
			Settings:            metadataSettings.JSON(),
			Timestamp:           time.Now().UTC(),
			APIVersion:          ReplicationAPIVersion,
			ReplicatedShapeID:   req.Schema.Id,
			ReplicatedShapeName: req.Schema.Name,
		}

		err = sqlstructs.Insert(session.DB, fmt.Sprintf("[%s].[%s]", sqlSchema, "NaveegoReplicationVersioning"), versioning)

		if err != nil {
			return nil, errors.Wrapf(err, "saving metadata %s", versioning.Settings)
		}
	}

	// Capture schemas for use during write.
	w.GoldenMetaSchema = MetaSchemaFromPubSchema(goldenSchema)
	w.VersionMetaSchema = MetaSchemaFromPubSchema(versionSchema)

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
		Record: record,
	})

	if err != nil {
		return errors.Wrapf(err, "group merge query")
	}

	// Merge version data
	_, err = templates.ExecuteCommand(session.DB, templates.ReplicationVersionMerge{
		Schema: r.VersionMetaSchema,
		Record: record,
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

	var changes []string

	if current != nil {
		changes = append(changes, r.compareProps(current, desired, "the database", "the platform")...)
		changes = append(changes, r.compareProps(desired, current, "the platform", "the database")...)
	} else {
		changes = []string{"table has not been created"}
	}

	if len(changes) > 0 {

		if current != nil {
			// if there is an old version of the table, drop it
			if err := r.dropTable(session, current.Id, changes...); err != nil {
				return errors.Wrapf(err, "could not drop table %q (reasons to try to drop table: %s)", current.Id, strings.Join(changes, "; "))
			}
		} else {
			// even if table has not been previously configured by us, it may exist
			// because of a previous failed configuration or other nonsense
			if err := r.dropTable(session, desired.Id, "ensuring that table does not already exist"); err != nil {
				return errors.Wrapf(err, "could not drop table %q (reasons to try to drop table: ensuring that table does not already exist)", desired.Id)
			}
		}

		if err := r.createTable(session, desired, changes...); err != nil {
			return errors.Wrapf(err, "could not create table %q (reasons to create table: %s)", desired.Id, strings.Join(changes, "; "))
		}
	}

	return nil
}

func (r *ReplicationWriter) dropTable(session *OpSession, table string, changes ...string) error {
	_, err := session.DB.Exec(fmt.Sprintf(`IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s`, table, table))
	r.recordChange("Dropped table %q (if it existed). Reasons: %s.", table, strings.Join(changes, "; "))
	return err
}

func (r *ReplicationWriter) compareProps(left, right *pub.Schema, leftID, rightID string) (changes []string) {

	rightProps := map[string]*pub.Property{}
	for _, prop := range right.Properties {
		rightProps[prop.Id] = prop
	}

	for _, leftProp := range left.Properties {
		rightProp, ok := rightProps[leftProp.Id]
		if ok {
			// found property, now check type
			rightSQLType := rightProp.Type
			leftSQLType := leftProp.Type

			if rightSQLType != leftSQLType {
				changes = append(changes, fmt.Sprintf("property '%s' is based on type '%s' in %s but based on type '%s' in %s", leftProp.Id, leftSQLType, leftID, rightSQLType, rightID))
			}
		} else {

			changes = append(changes, fmt.Sprintf("property '%s' in %s is not present in %s", leftProp.Id, leftID, rightID))
		}
	}

	return
}

func (r *ReplicationWriter) canonicalizeProperties(schema *pub.Schema) map[string]string {
	c := canonical.Strings(canonical.ToAlphanumeric, canonical.ToPascalCase)

	m := map[string]string{}

	for _, property := range schema.Properties {
		canonicalID := fmt.Sprintf("[%s]", c.Canonicalize(property.Name))
		m[property.Id] = canonicalID
		property.Id = canonicalID
	}
	return m
}

func (r *ReplicationWriter) createTable(session *OpSession, schema *pub.Schema, changes ...string) error {

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

	r.recordChange("Created table %q. Reasons: %s.", schema.Id, strings.Join(changes, "; "))

	return nil
}

func (r *ReplicationWriter) getCanonicalizedMap(data map[string]interface{}, idMap map[string]string) map[string]interface{} {
	out := make(map[string]interface{}, len(data))
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
	}, goldenSchema.Properties...)
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
