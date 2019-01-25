package internal

import (
	"encoding/json"
	"fmt"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"strings"
)

type Cause struct {
	Action         string `json:"action,omitempty"`
	Table          string `json:"table,omitempty"`
	DependencyKeys meta.RowKeys   `json:"dep,omitempty"`
}

func (c *Cause) String() string {
	if c == nil {
		return ""
	}
	w := new(strings.Builder)
	switch c.Action {
	case "I":
	fmt.Fprint(w, "Insert")
	case "U":
	fmt.Fprint(w, "Update")
	case "D":
	fmt.Fprint(w, "Delete")
	default:
		return ""
	}
	fmt.Fprintf(w, " in %s at ", c.Table)
	for i, k := range c.DependencyKeys {
		if i > 0 {
		fmt.Fprint(w, ", ")
		}
		fmt.Fprintf(w, "%s=%v", k.ColumnID, k.Value)
	}
	return w.String()
}

func ParseCause(cause string) Cause {
	var out Cause
	_ = json.Unmarshal([]byte(cause), &out)
	return out
}
