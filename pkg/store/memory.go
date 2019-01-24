package store

import (
	"fmt"
	"sync"
)

var memoryLock = new(sync.Mutex)
var memoryPool = map[string]memoryStore{}

type memoryStore struct {
	mu      *sync.Mutex
	schemas map[string]map[string]string
	deps map[string]map[string][][]byte
	path    string
}

func (m memoryStore) GetDependentKeys(dependencySchemaID string, dependencyKey []byte) [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	dependencyKeyString := fmt.Sprintf("%x", dependencyKey)

	return m.deps[dependencySchemaID][dependencyKeyString]
}

func (m memoryStore) AddDependency(dependencySchemaID string, dependencyKey []byte, schemaKey []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	depEntry, ok := m.deps[dependencySchemaID]
	if !ok {
		depEntry = map[string][][]byte{}
		m.deps[dependencySchemaID] = depEntry
	}

	dependencyKeyString := fmt.Sprintf("%x", dependencyKey)
	depEntry[dependencyKeyString] = append(depEntry[dependencyKeyString], schemaKey)

	return nil
}

func (m memoryStore) AddOrUpdateSchemaRecord(schema string, key []byte, hash []byte) (added bool, changed bool, err error) {
	m.mu.Lock()
	defer  m.mu.Unlock()
	var ok bool
	var schemaEntry map[string]string

	if schemaEntry, ok = m.schemas[schema]; !ok {
		schemaEntry = map[string]string{}
		m.schemas[schema] = schemaEntry
	}

	k := fmt.Sprintf("%x", key)
	h := fmt.Sprintf("%x", hash)

	hashEntry, ok := schemaEntry[k]
	added = !ok

	if !added {
		changed = hashEntry != h
	}
	schemaEntry[k] = h
	return
}

func getMemoryStore(path string) (Store, error) {
	memoryLock.Lock()
	defer memoryLock.Unlock()

	store, ok := memoryPool[path]
	if ok {
		return store, nil
	}

	store = memoryStore{
		mu:      new(sync.Mutex),
		schemas: map[string]map[string]string{},
		deps: map[string]map[string][][]byte{},

		path:    path,
	}
	memoryPool[path] = store

	return store, nil
}

func releaseMemoryStore(store memoryStore) error {
	memoryLock.Lock()
	defer memoryLock.Unlock()

	delete(memoryPool, store.path)
	return nil
}
