package internal

import (
	"bytes"
	"github.com/naveego/plugin-pub-mssql/internal/meta"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

type TxHelper struct {
	tx *bolt.Tx
}

func NewTxHelper(tx *bolt.Tx) TxHelper {
	return TxHelper{tx}
}

type StoreResult int

const (
	StoreNoop    = StoreResult(0)
	StoreAdded   = StoreResult(1)
	StoreChanged = StoreResult(2)
	StoreDeleted = StoreResult(3)
)

// SetSchemaHash updates the store to link the keys to the values,
// to allow insert/update detection.
//
// The return value will be StoreAdded  if the keys were not present in the store.
// The return value will be StoreChanged if the keys were present in the store, but with a different value.
// The return value will be StoreDeleted if the values were nil, whether or not they were present.
//
// If the `values` parameter is nil, the keys will be deleted from the store and the method will return false, false, nil.
func (t TxHelper) SetSchemaHash(schemaID string, keys meta.RowKeys, values meta.RowValues) (result StoreResult, err error) {
	b, err := t.tx.CreateBucketIfNotExists([]byte(schemaID))
	if err != nil {
		return StoreNoop, err
	}

	keyBytes := keys.Marshal()

	if values == nil {
		result = StoreDeleted
		err = b.Delete(keyBytes)
		return
	}

	valueBytes := values.Hash()

	storedValue := b.Get(keyBytes)
	if storedValue == nil {
		result = StoreAdded
	} else if !bytes.Equal(valueBytes, storedValue) {
		result = StoreChanged
	} else {
		return StoreNoop, nil
	}

	err = b.Put(keyBytes, valueBytes)

	return
}

func (t TxHelper) RegisterDependency(dependencyID string, schemaID string, dependencyKeys meta.RowKeys, schemaKeys meta.RowKeys) error {

	b, err := t.tx.CreateBucketIfNotExists([]byte(schemaID))
	if err != nil {
		return errors.WithStack(err)
	}
	b, err = b.CreateBucketIfNotExists([]byte(dependencyID))
	if err != nil {
		return errors.WithStack(err)
	}

	schemaBytes := schemaKeys.Marshal()
	dependencyPrefix := dependencyKeys.Marshal()

	c := b.Cursor()

	for k, v := c.Seek(dependencyPrefix); k != nil && bytes.HasPrefix(k, dependencyPrefix); k, v = c.Next() {
		if bytes.Equal(v, schemaBytes) {
			// dependency already registered
			return nil
		}
	}

	keyBytes := append(dependencyPrefix, schemaBytes...)
	err = b.Put(keyBytes, schemaBytes)
	return err
}

// UnregisterDependency removes the dependency linkage between the dependencyKeys and the schemaKeys.
// If schemaKeys is nil, all dependencies for the dependencyKeys are removed.
func (t TxHelper) UnregisterDependency(dependencyID string, schemaID string, dependencyKeys meta.RowKeys, schemaKeys meta.RowKeys) error {

	b, err := t.tx.CreateBucketIfNotExists([]byte(schemaID))
	if err != nil {
		return errors.WithStack(err)
	}
	b, err = b.CreateBucketIfNotExists([]byte(dependencyID))
	if err != nil {
		return errors.WithStack(err)
	}

	dependencyPrefix := dependencyKeys.Marshal()
	if schemaKeys != nil {
		schemaBytes := schemaKeys.Marshal()
		keyBytes := append(dependencyPrefix, schemaBytes...)
		err = b.Delete(keyBytes)
		if err != nil {
			return err
		}
	} else {
		dependencyPrefix := dependencyKeys.Marshal()
		c := b.Cursor()
		for k, _ := c.Seek(dependencyPrefix); k != nil && bytes.HasPrefix(k, dependencyPrefix); k, _ = c.Next() {
			err = c.Delete()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (t TxHelper) GetDependentSchemaKeys(dependencyID string, schemaID string, dependencyKeys meta.RowKeys) ([]meta.RowKeys, error) {

	b := t.tx.Bucket([]byte(schemaID))
	if b == nil {
		return nil, nil
	}
	b = b.Bucket([]byte(dependencyID))
	if b == nil {
		return nil, nil
	}

	var out []meta.RowKeys

	dependencyPrefix := dependencyKeys.Marshal()
	c := b.Cursor()
	for k, v := c.Seek(dependencyPrefix); k != nil && bytes.HasPrefix(k, dependencyPrefix); k, v = c.Next() {
		r, err := meta.UnmarshalRowKeys(v)
		if err != nil {
			return nil, errors.Errorf("invalid schema keys %s stored at %s: %s", string(v), string(k), err)
		}

		out = append(out, r)
	}

	return out, nil
}
