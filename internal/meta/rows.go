package meta

import "fmt"

type RowKey struct {
	ColumnID string
	Value    interface{}
}

func (r RowKey) String() string {
	return fmt.Sprintf("%s:%v", r.ColumnID, r.Value)
}

// Maps a string key to a value.
type SchemaRowKey struct {
	SchemaID string
	RowKey
}

type RowKeys []RowKey

func (r RowKeys) String() string {
	return fmt.Sprintf("%v", []RowKey(r))
}


