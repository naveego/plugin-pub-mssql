package store

import "github.com/pkg/errors"

type Store interface{
	AddOrUpdateSchemaRecord(schema string, key []byte, hash []byte) (added bool, changed bool, err error)
	AddDependency(dependencySchemaID string, dependencyKey []byte, schemaKey []byte) error
	GetDependentKeys(dependencySchemaID string, dependencyKey []byte) [][]byte
}


type StoreType string

const (
	StoreTypeBolt = StoreType("bolt")
	StoreTypeMemory = StoreType("memory")
)

func GetStore(path string, storeType StoreType) (Store, error) {

	switch storeType {
	case StoreTypeBolt:
		return getBoltStore(path)
	case StoreTypeMemory:
		return getMemoryStore(path)
	}

	panic(errors.Errorf("unknown store type %q", storeType))
}

func ReleaseStore(store Store) error {
	switch s := store.(type) {
	case boltStore:
		return releaseBoltStore(s)
	case memoryStore:
		return releaseMemoryStore(s)
	}
	panic(errors.Errorf("unknown store type %T", store))
}