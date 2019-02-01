package store

import (
	"go.etcd.io/bbolt"
	"os"
	"sync"
	"time"
)


type BoltStore struct {
	DB   *bolt.DB
	path string
}

var boltLock = new(sync.Mutex)
var boltPool = map[string]BoltStore{}


func GetBoltStore(path string) (BoltStore, error) {
	boltLock.Lock()
	defer boltLock.Unlock()

	store, ok := boltPool[path]
	if ok {
		return store, nil
	}

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout:5*time.Second})
	if err != nil {
		return BoltStore{}, err
	}

	store = BoltStore{
		DB:   db,
		path: path,
	}
	boltPool[path] = store

	return store, nil
}

func ReleaseBoltStore(store BoltStore) error {
	boltLock.Lock()
	defer boltLock.Unlock()

	store, ok := boltPool[store.path]
	if ok {
		err := store.DB.Close()

		if err != nil {
			return err
		}
		delete(boltPool, store.path)
	}

	return nil
}

func DestroyBoltStore(store BoltStore) error {
	boltLock.Lock()
	defer boltLock.Unlock()

	store, ok := boltPool[store.path]
	if ok {
		err := store.DB.Close()

		if err != nil {
			return err
		}
		delete(boltPool, store.path)
	}

	err := os.Remove(store.path)

	return err
}

