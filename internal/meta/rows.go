package meta

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"strings"
)

// RowMap is a map of column name to column value.
// It is created from a SQL result.
type RowMap map[string]interface{}

// ExtractPrefixedRowKeys returns a RowKeys with the values for entry in the RowMap
// which has a key which matches a column in the schema.
// Returns an error if data does not have an entry for each key column in the schema.
func (r RowMap) ExtractRowKeys(s *Schema) (RowKeys, error) {
	return r.ExtractPrefixedRowKeys(s, "")
}

// ExtractPrefixedRowKeys returns a RowKeys with the values for entry in the RowMap
// which has a key which matches a column in the schema when the prefix is removed.
// Returns an error if data does not have an entry for each key column in the schema.
func (r RowMap) ExtractPrefixedRowKeys(s *Schema, prefix string) (RowKeys, error) {
	var rk RowKeys
	for _, c := range s.KeyColumns() {
		if v, ok := r[prefix+c.ID]; ok {
			rk = append(rk, RowKey{c.ID, v})
		} else if v, ok := r[prefix+c.OpaqueName()]; ok {
			rk = append(rk, RowKey{c.ID, v})
		} else {
			return nil, errors.Errorf("data missing key %q", c.ID)
		}
	}
	return rk, nil
}

// ExtractRowValues returns a RowValues with the values for entry in the RowMap
// which has a key which matches a column in the schema.
// Returns an error if data does not have an entry for each column in the schema.
func (r RowMap) ExtractRowValues(s *Schema) (RowValues, error) {
	return r.ExtractPrefixedRowValues(s, "")
}

// ExtractPrefixedRowValues returns a RowValues with the values for entry in the RowMap
// which has a key which matches a column in the schema when the prefix is removed.
// Returns an error if data does not have an entry for each column in the schema.
func (r RowMap) ExtractPrefixedRowValues(s *Schema, prefix string) (RowValues, error) {
	var rv RowValues
	for _, c := range s.Columns() {
		if v, ok := r[prefix+c.ID]; ok {
			rv = append(rv, RowValue{c.ID, v})
		} else if v, ok := r[prefix+c.OpaqueName()]; ok {
			rv = append(rv, RowValue{c.ID, v})
		} else {
			return nil, errors.Errorf("data missing column %q", c.ID)
		}
	}
	return rv, nil
}

// JSON returns the JSON representation of the RowMap.
func (r RowMap) JSON() string {
	b, _ := json.Marshal(r)
	return string(b)
}



type RowValue struct {
	ColumnID string
	Value    interface{}
}

type RowKey RowValue

func (r RowKey) String() string {
	return fmt.Sprintf("%s:%v", r.ColumnID, r.Value)
}
func (r RowValue) String() string {
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

// ToMapKey returns a string which contains all the keys.
func (r RowKeys) ToMapKey() string {
	w := new(strings.Builder)
	for _, c := range r{
		fmt.Fprintf(w, "%v|",c.Value)
	}
	return w.String()
}

// HasNils returns true if any values are nil.
func (r RowKeys) HasNils() bool {
	for _, x := range r {
		if x.Value == nil {
			return true
		}
	}
	return false
}


func (r RowValues) String() string {
	return fmt.Sprintf("%v", []RowValue(r))
}

func (r RowKeys) Marshal() []byte {
	b, _ := json.Marshal(r)
	return b
}

func UnmarshalRowKeys(b []byte) (RowKeys, error) {
	var r RowKeys
	err := json.Unmarshal(b, &r)
	return r, err
}

type RowValues []RowValue

func (r RowValues) Hash() []byte {
	if len(r) == 0 {
		return nil
	}
	h := sha256.New()
	for _, e := range r {
		fmt.Fprint(h, e)
	}
	return h.Sum(nil)
}
