package constants


const (
	UpdatedAt = "NaveegoUpdatedAt"
	CreatedAt = "NaveegoCreatedAt"
	RecordID = "NaveegoRecordID"
	GroupID = "NaveegoGroupID"
	JobID = "NaveegoJobID"
	JobName = "NaveegoJobName"
	ConnectionID ="NaveegoConnectionID"
	ConnectionName ="NaveegoConnectionName"
	SchemaID ="NaveegoSchemaID"
	SchemaName ="NaveegoSchemaName"

	ReplicationVersioningTable = "NaveegoReplicationVersioning"
	ReplicationMetadataTable = "NaveegoReplicationMetadata"
)

var NaveegoMetadataColumnNames = map[string]bool{
	UpdatedAt:true,
	CreatedAt:true,
	RecordID:true,
	GroupID:true,
	JobID:true,
	JobName:true,
	ConnectionID:true,
	ConnectionName:true,
	SchemaID:true,
	SchemaName:true,
}