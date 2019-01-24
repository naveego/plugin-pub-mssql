package store

import (
	"go.etcd.io/bbolt"
	"sync"
	"time"
)


type boltStore struct {
	db   *bolt.DB
	path string
}

func (b boltStore) AddOrUpdateSchemaRecord(schema string, key []byte, hash []byte) (added bool, changed bool, err error) {
	panic("implement me")
}

func (b boltStore) AddDependency(dependencySchemaID string, dependencyKey []byte, schemaKey []byte) error {
	panic("implement me")
}

func (b boltStore) GetDependentKeys(dependencySchemaID string, dependencyKey []byte) [][]byte {
	panic("implement me")
}

var boltLock = new(sync.Mutex)
var boltPool = map[string]boltStore{}

func getBoltStore(path string) (Store, error) {
	boltLock.Lock()
	defer boltLock.Unlock()

	store, ok := boltPool[path]
	if ok {
		return store, nil
	}

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout:5*time.Second})
	if err != nil {
		return boltStore{}, err
	}

	store = boltStore{
		db:   db,
		path: path,
	}
	boltPool[path] = store

	return store, nil
}

func releaseBoltStore(store boltStore) error {
	boltLock.Lock()
	defer boltLock.Unlock()

	store, ok := boltPool[store.path]
	if ok {
		err := store.db.Close()

		if err != nil {
			return err
		}
		delete(boltPool, store.path)
	}

	return nil
}

