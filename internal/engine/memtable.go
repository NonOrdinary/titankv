/*
FILE Details
- Memtable ds{
		ReadWrite Mutex
		Map[string] -> record{ slice of byte, boolean delete}
	}
- Methods associated to this Memtable data structure as this lang doesn't have classes
-- Put(key, value) : Does put the key and value inside the map, allows single writer
-- Get(key) : Returns the value or NIL, depending upon if key was present in map or not, allows multiple readers
-- Delete(key) : Delete's key by placing a tombstone in the record
*/

package engine

import (
	"sync"
)

// Record represents a single entry in our database, value and at tombstone flag
type Record struct {
	Value []byte
	// Deleted acts as our Tombstone. If true, the key is considered dead.
	Deleted bool
}

// Our memtable storage
type MemTable struct {
	mu sync.RWMutex

	data map[string]Record // standard map[string] -> record
}

// NewMemTable is the constructor, is a function but since no classes, thus it works
func NewMemTable() *MemTable {
	// I initialized map in Go before using it, becuase Go sets NIL value, which on write would crash(null pointer derefernce)
	// make allocates Heap Memory for the data structure
	return &MemTable{
		data: make(map[string]Record),
	}
}

// Put inserts or updates a key-value pair.
func (m *MemTable) Put(key string, value []byte) {
	// lock for write, no goroutine must enter during write
	m.mu.Lock()

	// defer guarantees that Unlock() will be called right before
	// the function exits, even if the code panics.
	defer m.mu.Unlock() // very important and cool trick to have -- Absolute Genius

	// Insert the record and ensure the tombstone is false
	m.data[key] = Record{Value: value, Deleted: false}
}

// Get retrieves a record by its key.
func (m *MemTable) Get(key string) (Record, bool) {
	// Acquire a Read lock. Other goroutines can also acquire read locks
	// at the same time, making our database very fast for read-heavy workloads.

	// This function expects to return slice of byte , if key exist and NIL otherwise
	m.mu.RLock()
	defer m.mu.RUnlock()

	/* Map returns value, T\F depending if key is present in map or not*/
	record, exists := m.data[key]
	// We return the whole Record. The caller will have to check record.Deleted
	// to know if they hit a tombstone.
	return record, exists
}

func (m *MemTable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// TOMBSTONE: We overwrite the existing key with a nil value and Deleted: true
	m.data[key] = Record{Value: nil, Deleted: true}
}
