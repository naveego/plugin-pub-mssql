package internal

import (
	"github.com/naveego/plugin-pub-mssql/internal/pub"
)


// Settings object for write requests
// Contains target schema and the commit sla timeout
type WriteSettings struct {
	Schema		*pub.Schema   `json:"schema"`
	CommitSLA	int32		  `json:"commitSla"`
}
