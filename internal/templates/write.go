package templates

import (
	"encoding/json"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
)

var replicationMetadataTemplate = compileTemplate("replicationMetadata",
	// language=GoTemplate
	`
IF OBJECT_ID('{{.SQLSchema}}.NaveegoReplicationMetadata', 'U') IS NULL
	create table {{.SQLSchema}}.NaveegoReplicationMetadata
	(
		ID int IDENTITY PRIMARY KEY,
		Timestamp DATETIME2,
		ShapeID NVARCHAR(30),
		Settings NVARCHAR(max)
	)

IF NOT EXISTS(SELECT * FROM sys.indexes WHERE name = 'NaveegoReplicationMetadata_ID_uindex' AND object_id = OBJECT_ID('{{.SQLSchema}}.NaveegoReplicationMetadata'))
create unique index NaveegoReplicationMetadata_ID_uindex
	on {{.SQLSchema}}.NaveegoReplicationMetadata (ID)

` )

type ReplicationMetadataDDLArgs struct {
	SQLSchema string
}

func RenderReplicationMetadataDDLArgs(args ReplicationMetadataDDLArgs) (string, error) {
	return renderTemplate(replicationMetadataTemplate, args)
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
` )

type ReplicationTableCreationDDLArgs struct {
	Schema *meta.Schema
	ParentTable string
}

func (r ReplicationTableCreationDDLArgs) String() string {
	x, _ := json.Marshal(r)
	return string(x)
}

func RenderReplicationTableCreationDDLArgs(args ReplicationTableCreationDDLArgs) (string, error) {
	return renderTemplate(replicationTableCreationTemplate, args)
}
