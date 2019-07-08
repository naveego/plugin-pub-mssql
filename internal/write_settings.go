package internal

import (
	"encoding/json"
	"github.com/naveego/plugin-pub-mssql/internal/pub"
)


// Settings object for write requests
// Contains target schema and the commit sla timeout
type WriteSettings struct {
	Schema		*pub.Schema   `json:"schema"`
	CommitSLA	int32		  `json:"commitSla"`
}

type ReplicationSettings struct {

	SQLSchema string `json:"sqlSchema" title:"Schema" description:"The schema in which to create the replication tables (will be created if it does not exist)." required:"true"`
	GoldenRecordTable string `json:"goldenRecordTable" title:"Golden Record Table" description:"The table to store golden records in (will be created if it does not exist)." required:"true"`
	VersionRecordTable string `json:"versionRecordTable" title:"Version Record Table" description:"The table to store version records in (will be created if it does not exist)." required:"true"`

}

func (r ReplicationSettings) GetNamespacedGoldenRecordTable() string {
	return GetSchemaID(r.SQLSchema, r.GoldenRecordTable)
}
func (r ReplicationSettings) GetNamespacedVersionRecordTable() string {
	return GetSchemaID(r.SQLSchema, r.VersionRecordTable)
}
func (r ReplicationSettings) JSON() string {
	j, _ := json.Marshal(r)
	return string(j)
}