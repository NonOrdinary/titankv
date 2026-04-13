package engine

import "sync"

// skipListNodeOverhead accounts for the structural memory cost of a SkipList node.
// Assuming maxLevel = 16, a slice of 16 pointers (8 bytes each) + struct overhead ≈ 130 bytes.
const skipListNodeOverhead = 130

// MemTable is our thread-safe, in-memory storage engine.
type MemTable struct {
	mu        sync.RWMutex
	data      *SkipList
	sizeBytes int // Tracks physical data + structural memory usage
}

// NewMemTable initializes a new MVCC-ready MemTable.
func NewMemTable() *MemTable {
	return &MemTable{
		data:      NewSkipList(),
		sizeBytes: 0,
	}
}

// Put inserts a new version of a key into the MemTable.
// Note: It requires the Orchestrator (db.go) to pass the SeqNum.
func (m *MemTable) Put(userKey []byte, value []byte, seqNum uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Pack the metadata into our fixed-length suffix byte slice
	internalKey := EncodeInternalKey(userKey, seqNum, TypePut)

	// 2. Insert into the Skip List
	m.data.Insert(internalKey, value)

	// 3. Track the physical size of the bytes + node pointer overhead
	m.sizeBytes += len(internalKey) + len(value) + skipListNodeOverhead
}

// Get retrieves the most recent version of a key that is <= the requested SeqNum.
// It returns (value, isDeleted, exists).
func (m *MemTable) Get(userKey []byte, targetSeqNum uint64) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Create a "dummy" search key.
	// CRITICAL: We use TypePut (1) because when searching an LSM tree,
	// the dummy key must use the MAXIMUM possible byte value for the type.
	// This ensures the comparator sorts it BEFORE any identical timestamp
	// that might have a TypeDelete (0) flag.
	searchKey := EncodeInternalKey(userKey, targetSeqNum, TypePut)

	// Search the Skip List. It guarantees it will return the FIRST node
	// whose SeqNum is <= our targetSeqNum.
	val, keyType, found := m.data.Search(searchKey)

	if !found {
		return nil, false, false
	}

	// If we found the key, but its type is a Tombstone, we tell the caller
	// that at this specific point in time (targetSeqNum), the key was deleted.
	if keyType == TypeDelete {
		return nil, true, true
	}

	return val, false, true
}

// Delete acts exactly like a Put, but appends a Tombstone marker instead of a value.
func (m *MemTable) Delete(userKey []byte, seqNum uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	internalKey := EncodeInternalKey(userKey, seqNum, TypeDelete)

	// Value is nil because it's a delete marker
	m.data.Insert(internalKey, nil)

	// Track size including structural overhead
	m.sizeBytes += len(internalKey) + skipListNodeOverhead
}

// Iterate safely locks the MemTable and streams the InternalKeys to the disk.
// Used exclusively by the sstable_builder during a 4MB flush.
func (m *MemTable) Iterate(cb func(internalKey []byte, value []byte)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.data.Iterate(cb)
}

// ApproximateSize returns the current byte size of the MemTable.
// The DB orchestrator calls this to check if we have hit the flush limit.
func (m *MemTable) ApproximateSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}
