package templates

import (
	"encoding/json"
	"github.com/naveego/plugin-pub-mssql/internal/constants"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
)

var replicationMetadataTemplate = compileTemplate("replicationMetadata",
	// language=GoTemplate
	`
IF NOT exists(SELECT *
              FROM sys.schemas
              WHERE name = '{{.SQLSchema}}')
    BEGIN
        EXEC sp_executesql N'CREATE SCHEMA [{{.SQLSchema}}]'
    END;


IF OBJECT_ID('{{.SQLSchema}}.{{.ReplicationVersioningTable}}', 'U') IS NULL
	create table [{{.SQLSchema}}].{{.ReplicationVersioningTable}}
	(
		ID int IDENTITY PRIMARY KEY,
		Timestamp DATETIME2,
		APIVersion int,
		ReplicatedShapeID NVARCHAR(100),		
		ReplicatedShapeName NVARCHAR(100),		
		Settings NVARCHAR(max)
	)
	
IF OBJECT_ID('{{.SQLSchema}}.{{.ReplicationMetadataTable}}', 'U') IS NULL
	create table [{{.SQLSchema}}].{{.ReplicationMetadataTable}}
	(
		ID CHAR(20) PRIMARY KEY,
		Kind VARCHAR(20),
		Name NVARCHAR(200)
	)
` )

type ReplicationMetadataDDLArgs struct {
	SQLSchema string
	ReplicationVersioningTable string
	ReplicationMetadataTable string
}

func (r ReplicationMetadataDDLArgs) Render() (string, error) {
	r.ReplicationMetadataTable = constants.ReplicationMetadataTable
	r.ReplicationVersioningTable = constants.ReplicationVersioningTable
	return renderTemplate(replicationMetadataTemplate, r)
}



var replicationMetadataMergeTemplate = compileTemplate("replicationMetadataMerge",
	// language=GoTemplate
	`
MERGE INTO [{{.SQLSchema}}].{{.ReplicationMetadataTable}} AS Target
USING (VALUES
{{- range $i, $entry := .Entries -}}
	{{- if gt $i 0 -}}, {{else}}  {{ end }}('{{ $entry.ID }}', '{{ $entry.Kind }}', '{{ $entry.Name | replace "'" "''" }}')
{{- end -}}
        )
    AS Source (ID, Kind, Name)
ON Target.ID = Source.ID
	
WHEN MATCHED THEN
    UPDATE SET
		Kind = Source.Kind,
		Name = Source.Name
-- Insert all versions which have been newly added to this group
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ID, Kind, Name)
    VALUES (ID, Kind, Name);
` )

type ReplicationMetadataMerge struct {
	SQLSchema string
	Entries []ReplicationMetadataEntry
	ReplicationVersioningTable string
	ReplicationMetadataTable string
}
type ReplicationMetadataEntry struct {
	Kind string
	ID string
	Name string
}

func (r ReplicationMetadataMerge) String() string {
	x, _ := json.Marshal(r)
	return string(x)
}

func (r ReplicationMetadataMerge) Render() (string, error) {
	r.ReplicationMetadataTable = constants.ReplicationMetadataTable
	r.ReplicationVersioningTable = constants.ReplicationVersioningTable
	return renderTemplate(replicationMetadataMergeTemplate, r)
}



var replicationTableCreationTemplate = compileTemplate("replicationTableCreation",
	// language=GoTemplate
	`
CREATE TABLE {{.Schema.ID}}
(
{{- range $i, $prop := .Schema.ColumnsKeysFirst }}
	{{$prop.ID}} {{ $prop.SQLType }},
{{- end }}
	PRIMARY KEY ({{ $.Schema.Keys | join ", " }})
)
create index Versions_GroupID_index
	on {{.Schema.ID}} ({{.GroupIDColumn}})

` )

type ReplicationTableCreationDDL struct {
	Schema *meta.Schema
	ParentTable string
	GroupIDColumn string
	UpdatedAtColumn string
	CreatedAtColumn string
}

func (r ReplicationTableCreationDDL) String() string {
	x, _ := json.Marshal(r)
	return string(x)
}

func RenderReplicationTableCreationDDLArgs(args ReplicationTableCreationDDL) (string, error) {
	args.CreatedAtColumn = constants.CreatedAt
	args.UpdatedAtColumn = constants.UpdatedAt
	args.GroupIDColumn = constants.GroupID
	return renderTemplate(replicationTableCreationTemplate, args)
}


var replicationVersionMergeTemplate = compileTemplate("replicationVersionMerge",
	// language=GoTemplate
	`
{{- if or (eq .Record.Action 3) (not .Record.UnmarshalledVersions) -}}
-- if the record action is 3, or there are no versions, we should delete all the versions in the group
DELETE FROM {{.Schema.ID}}
WHERE {{.GroupIDColumn}} = '{{ .Record.RecordId }}'
{{ else }}

MERGE INTO {{.Schema.ID}} AS Target
USING (VALUES
{{- range $i, $version := .Record.UnmarshalledVersions -}}
    {{- if gt $i 0 -}}, {{else}}  {{- end -}}
              ({{ $.Schema.Columns.MakeSQLValuesFromMap $version.Data }})
{{- end -}}
        )
    AS Source ({{$.Schema.Columns.MakeSQLColumnNameList}})
ON Target.{{.JobIDColumn}} = Source.{{.JobIDColumn}}
    AND Target.{{.RecordIDColumn}} = Source.{{.RecordIDColumn}}
WHEN MATCHED THEN
    UPDATE SET
{{- range $i, $col := $.Schema.Columns.OmitKeys.OmitIDs .UpdatedAtColumn }}
    {{ $col.ID }} = Source.{{ $col.ID }},
{{ end -}}       
	{{.UpdatedAtColumn}}  = GETDATE()
-- Insert all versions which have been newly added to this group
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({{($.Schema.Columns.OmitIDs $.CreatedAtColumn $.UpdatedAtColumn).MakeSQLColumnNameList }}, {{.CreatedAtColumn}}, {{.UpdatedAtColumn}}) 
    VALUES ({{($.Schema.Columns.OmitIDs $.CreatedAtColumn $.UpdatedAtColumn).MakeSQLColumnNameList}}, GETDATE(), GETDATE())
-- Delete all versions which were in this group but are not any more.
WHEN NOT MATCHED BY SOURCE 
 	AND Target.{{.GroupIDColumn}} = '{{.Record.RecordId}}'
 	THEN DELETE;
{{- end -}}
` )

type ReplicationVersionMerge struct {
	Schema *meta.Schema
	Record *pub.UnmarshalledRecord
	GroupIDColumn string
	UpdatedAtColumn string
	CreatedAtColumn string
	JobIDColumn string
	RecordIDColumn string

}

func (r ReplicationVersionMerge) String() string {
	x, _ := json.Marshal(r)
	return string(x)
}
func (r ReplicationVersionMerge) Render() (string, error) {
	r.CreatedAtColumn = constants.CreatedAt
	r.UpdatedAtColumn = constants.UpdatedAt
	r.GroupIDColumn = constants.GroupID
	r.JobIDColumn = constants.JobID
	r.RecordIDColumn = constants.RecordID
	return renderTemplate(replicationVersionMergeTemplate, r)
}


var replicationGoldenMergeTemplate = compileTemplate("replicationGoldenMerge",
	// language=GoTemplate
	`
{{- if or (eq .Record.Action 3) (not .Record.UnmarshalledVersions) -}}
-- if the record action is 3, we should delete the group
DELETE FROM {{.Schema.ID}}
WHERE {{.GroupIDColumn}} = '{{ .Record.RecordId }}'
{{ else }}
MERGE INTO {{.Schema.ID}} AS Target
USING (VALUES
       ({{ $.Schema.Columns.MakeSQLValuesFromMap .Record.Data }})
      )
    AS Source ({{$.Schema.Columns.MakeSQLColumnNameList}})
ON Target.{{.GroupIDColumn}} = Source.{{.GroupIDColumn}}
WHEN MATCHED THEN
    UPDATE SET
{{- range $i, $col := $.Schema.Columns.OmitKeys.OmitIDs .UpdatedAtColumn }}
    {{ $col.ID }} = Source.{{ $col.ID }},
{{ end -}}      
	{{.UpdatedAtColumn}}  = GETDATE()
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({{($.Schema.Columns.OmitIDs $.CreatedAtColumn $.UpdatedAtColumn).MakeSQLColumnNameList }}, {{.CreatedAtColumn}}, {{.UpdatedAtColumn}}) 
    VALUES ({{($.Schema.Columns.OmitIDs $.CreatedAtColumn $.UpdatedAtColumn).MakeSQLColumnNameList}}, GETDATE(), GETDATE());
{{- end -}}
` )

type ReplicationGoldenMerge struct {
	Schema *meta.Schema
	Record *pub.UnmarshalledRecord
	GroupIDColumn string
	UpdatedAtColumn string
	CreatedAtColumn string
}

func (r ReplicationGoldenMerge) String() string {
	x, _ := json.Marshal(r)
	return string(x)
}

func (r ReplicationGoldenMerge) Render() (string, error) {
	r.CreatedAtColumn = constants.CreatedAt
	r.UpdatedAtColumn = constants.UpdatedAt
	r.GroupIDColumn = constants.GroupID
	return renderTemplate(replicationGoldenMergeTemplate, r)
}
