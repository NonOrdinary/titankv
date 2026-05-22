package engine

import "sync"

const skipListNodeOverhead = 130

type MemTable struct {
	mu        sync.RWMutex
	data      *SkipList
	sizeBytes int
}

func NewMemTable() *MemTable {
	return &MemTable{
		data:      NewSkipList(),
		sizeBytes: 0,
	}
}

func (m *MemTable) Put(userKey []byte, value []byte, seqNum uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	internalKey := EncodeInternalKey(userKey, seqNum, TypePut)

	m.data.Insert(internalKey, value)

	m.sizeBytes += len(internalKey) + len(value) + skipListNodeOverhead
}

func (m *MemTable) Get(userKey []byte, targetSeqNum uint64) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	searchKey := EncodeInternalKey(userKey, targetSeqNum, TypePut)

	val, keyType, found := m.data.Search(searchKey)

	if !found {
		return nil, false, false
	}

	if keyType == TypeDelete {
		return nil, true, true
	}

	return val, false, true
}

func (m *MemTable) Delete(userKey []byte, seqNum uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	internalKey := EncodeInternalKey(userKey, seqNum, TypeDelete)

	m.data.Insert(internalKey, nil)

	m.sizeBytes += len(internalKey) + skipListNodeOverhead
}

func (m *MemTable) Iterate(cb func(internalKey []byte, value []byte)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.data.Iterate(cb)
}

func (m *MemTable) ApproximateSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeBytes
}
