package internal

import (
	"database/sql"
	"encoding/json"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
	"github.com/pkg/errors"
	"time"
)

type Writer interface {
	Write(session *OpSession, record *pub.Record) error
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

func (d *DefaultWriteHandler) Write(session *OpSession, record *pub.Record) error {

	var err error

	var recordData map[string]interface{}
	if err = json.Unmarshal([]byte(record.DataJson), &recordData); err != nil {
		return errors.WithStack(err)
	}

	schema := d.WriteSettings.Schema

	// build params for stored procedure
	var args []interface{}
	for _, prop := range schema.Properties {

		rawValue := recordData[prop.Id]
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
}

func (r *ReplicationWriter) Write(session *OpSession, record *pub.Record) error {
	panic("implement me")
}

func NewReplicationWriteHandler(session *OpSession, req *pub.PrepareWriteRequest) (Writer, error) {

	r := &ReplicationWriter{}
	return r, nil
}
