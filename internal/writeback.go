package internal

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/naveego/plugin-pub-mssql/internal/templates"
	"github.com/naveego/plugin-pub-mssql/pkg/canonical"
	"github.com/naveego/plugin-pub-mssql/pkg/sqlstructs"
	"github.com/pkg/errors"
	"strings"
	"time"
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
				return errors.Errorf("cannot convert value %v to %s (was %T)", rawValue, prop.Type, rawValue)
			}
			value, err = time.Parse(time.RFC3339, stringValue)
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
	req          *pub.PrepareWriteRequest
	GoldenIDMap  map[string]string
	VersionIDMap map[string]string
	changes      []string
}

func (r *ReplicationWriter) Write(session *OpSession, record *pub.UnmarshalledRecord) error {

	return nil
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

	command, err := templates.RenderReplicationMetadataDDLArgs(templates.ReplicationMetadataDDLArgs{
		SQLSchema: sqlSchema,
	})
	if err != nil {
		return nil, errors.Wrap(err, "render metadata table ddl")
	}
	// ensure metadata table exists
	_, err = session.DB.Exec(command)
	if err != nil {
		return nil, errors.Wrapf(err, "create replication metadata table using command \n%s", command)
	}

	query := fmt.Sprintf("select top (1) * from %s.NaveegoReplicationMetadata order by id desc", sqlSchema)
	rows, err := session.DB.Query(query)
	if err != nil {
		return nil, errors.Wrap(err, "get replication metadata")
	}

	var previousMetadataSettings NaveegoReplicationMetadataSettings
	naveegoReplicationMetadataRows := make([]NaveegoReplicationMetadata, 0, 0)
	err = sqlstructs.UnmarshalRows(rows, &naveegoReplicationMetadataRows)
	if err != nil {
		return nil, err
	}
	if len(naveegoReplicationMetadataRows) > 0 {
		previousMetadataSettings = naveegoReplicationMetadataRows[0].GetSettings()
		if previousMetadataSettings.Settings.GetNamespacedVersionRecordTable() != settings.GetNamespacedVersionRecordTable() {
			// version table name has changed
			if err := w.dropTable(session, previousMetadataSettings.Settings.GetNamespacedVersionRecordTable()); err != nil {
				return nil, errors.Wrap(err, "dropping version table after name change")
			}
		}
		if previousMetadataSettings.Settings.GetNamespacedGoldenRecordTable() != settings.GetNamespacedVersionRecordTable() {
			// golden table name has changed
			if err := w.dropTable(session, previousMetadataSettings.Settings.GetNamespacedGoldenRecordTable()); err != nil {
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

	goldenSchema.Properties = append([]*pub.Property{
		{
			Id:           "[GroupID]",
			Name:         "GroupID",
			IsKey:        true,
			Type:         pub.PropertyType_STRING,
			TypeAtSource: "CHAR(44)", // fits an RID if we change group ID convention
		},
	}, goldenSchema.Properties...)
	goldenSchema.Id = GetSchemaID(settings.SQLSchema, settings.GoldenRecordTable)

	w.GoldenIDMap = w.canonicalizeProperties(goldenSchema)

	versionSchema.Properties = append(versionProperties, versionSchema.Properties...)
	versionSchema.Id = GetSchemaID(settings.SQLSchema, settings.VersionRecordTable)
	w.VersionIDMap = w.canonicalizeProperties(versionSchema)

	discoveredSchemas, err := DiscoverSchemasSync(session, session.SchemaDiscoverer, &pub.DiscoverSchemasRequest{
		Mode:       pub.DiscoverSchemasRequest_REFRESH,
		SampleSize: 0,
		ToRefresh: []*pub.Schema{
			goldenSchema,
			versionSchema,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "checking for owned schemas")
	}

	var existingGoldenSchema *pub.Schema
	var existingVersionSchema *pub.Schema
	for _, schema := range discoveredSchemas {
		if schema.Id == settings.GetNamespacedGoldenRecordTable() {
			existingGoldenSchema = schema
		}
		if schema.Id == settings.GetNamespacedVersionRecordTable() {
			existingVersionSchema = schema
		}
	}

	if err := w.reconcileSchemas(session, existingVersionSchema, versionSchema); err != nil {
		return nil, errors.Wrap(err, "reconciling version schema")
	}
	if err := w.reconcileSchemas(session, existingGoldenSchema, goldenSchema); err != nil {
		return nil, errors.Wrap(err, "reconciling golden schema")
	}

	if len(w.changes) > 0 {

		session.Log.Info("Made changes to database during prep, will save new metadata.")

		metadataSettings := NaveegoReplicationMetadataSettings{
			Settings:      settings,
			Request:       req,
			Changes:       w.changes,
			GoldenSchema:  goldenSchema,
			VersionSchema: versionSchema,
		}
		metadata := NaveegoReplicationMetadata{
			Settings:  metadataSettings.JSON(),
			Timestamp: time.Now().UTC(),
		}

		_, err := session.DB.Exec(fmt.Sprintf(`insert into %s.NaveegoReplicationMetadata (Timestamp, Settings) values (@timestamp, @settings)`, sqlSchema),
			sql.Named("timestamp", metadata.Timestamp),
			sql.Named("settings", metadata.Settings),
		)
		if err != nil {
			return nil, errors.Wrapf(err, "saving metadata %s", metadata.Settings)
		}
	}

	return w, nil
}

func (r *ReplicationWriter) recordChange(f string, args ...interface{}) {
	r.changes = append(r.changes, fmt.Sprintf(f, args...))
}

func (r *ReplicationWriter) reconcileSchemas(session *OpSession, current *pub.Schema, desired *pub.Schema) error {

	needsDelete := false
	needsCreate := false
	if current == nil {
		needsCreate = true
	} else {

		needsDelete = r.compareProps(current, desired) || r.compareProps(desired, current)
		needsCreate = needsDelete
	}

	if needsDelete && current != nil {
		if err := r.dropTable(session, current.Id); err != nil {
			session.Log.Warn("Could not drop table.", "table", current.Id, "err", err)
		}

	}

	if needsCreate {
		if err := r.createTable(session, current); err != nil {
			session.Log.Error("Could not create table.", "table", current, "err", err)
			return errors.Wrapf(err, "create table")
		}

	}

	return nil
}

func (r *ReplicationWriter) dropTable(session *OpSession, table string) error {
	_, err := session.DB.Exec(fmt.Sprintf(`DROP TABLE %s`, table))
	r.recordChange("Dropped table %q because of changes.", table)
	return err
}

func (r *ReplicationWriter) compareProps(left, right *pub.Schema) (same bool) {
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
	}

	return true
}

func (r *ReplicationWriter) canonicalizeProperties(schema *pub.Schema) map[string]string {
	c := canonical.Strings(canonical.ToAlphanumeric, canonical.ToPascalCase)

	m := map[string]string{}

	for _, property := range schema.Properties {
		canonicalID := c.Canonicalize(property.Name)
		m[property.Id] = canonicalID
		property.Id = fmt.Sprintf("[%s]", canonicalID)
	}
	return m
}

func (r *ReplicationWriter) createTable(session *OpSession, schema *pub.Schema) error {

	args := templates.ReplicationTableCreationDDLArgs{
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

var versionProperties []*pub.Property

func init() {
	versionPropertyNames := []string{

		"[ConnectionID]",
		"[ConnectionName]",
		"[GroupID]",
		"[JobName]",
		"[SchemaID]",
		"[SchemaName]",
	}

	versionProperties = []*pub.Property{
		{
			Id:           "[RecordID]",
			Name:         "RecordID",
			Type:         pub.PropertyType_STRING,
			TypeAtSource: "CHAR(44)", // fits a base64 encoded SHA256
			IsKey:        true,
		},
		{
			Id:           "[JobID]",
			Name:         "JobID",
			Type:         pub.PropertyType_STRING,
			TypeAtSource: "CHAR(44)",
			IsKey:        true,
		},
	}

	for _, name := range versionPropertyNames {
		property := &pub.Property{
			Id:   name,
			Name: strings.Trim(name, "[]"),
			Type: pub.PropertyType_STRING,
		}
		versionProperties = append(versionProperties, property)
	}

}

type NaveegoReplicationMetadata struct {
	ID        int       `sql:"ID"`
	Timestamp time.Time `sql:"Timestamp"`
	Settings  string    `sql:"Settings"`
}

func (n NaveegoReplicationMetadata) GetSettings() NaveegoReplicationMetadataSettings {
	var s NaveegoReplicationMetadataSettings
	_ = json.Unmarshal([]byte(n.Settings), &s)
	return s
}

type NaveegoReplicationMetadataSettings struct {
	Request       *pub.PrepareWriteRequest
	Settings      ReplicationSettings
	GoldenSchema  *pub.Schema
	VersionSchema *pub.Schema
	Changes       []string
}

func (s NaveegoReplicationMetadataSettings) JSON() string {
	j, _ := json.MarshalIndent(s, "", "  ")
	return string(j)
}
