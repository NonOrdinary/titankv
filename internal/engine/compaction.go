package engine

import (
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type HeapItem struct {
	Key         string
	Value       []byte
	IsTombstone bool
	IterIdx     int // Tracks which file this came from (higher = newer)
}

type KVHeap []*HeapItem

func (h KVHeap) Len() int { return len(h) }
func (h KVHeap) Less(i, j int) bool {
	if h[i].Key == h[j].Key {
		// If keys are identical, sort by IterIdx DESCENDING (Newer file wins)
		return h[i].IterIdx > h[j].IterIdx
	}
	return h[i].Key < h[j].Key
}
func (h KVHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *KVHeap) Push(x interface{}) { *h = append(*h, x.(*HeapItem)) }
func (h *KVHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// Compact merges the oldest 4 SSTables into 1.
func (db *DB) Compact() error {
	defer func() {
		db.mu.Lock()
		db.isCompacting = false
		db.mu.Unlock()
	}()

	db.mu.RLock()
	// Snapshot the oldest 4 tables. Leave them in db.sstables to serve live traffic.
	tablesToMerge := make([]*SSTableReader, 4)
	copy(tablesToMerge, db.sstables[:4])
	db.mu.RUnlock()

	var iterators []*SSTableIterator
	pq := &KVHeap{}
	heap.Init(pq)

	// Open iterators and push the first item from each into the Heap
	for i, table := range tablesToMerge {
		iter, err := NewSSTableIterator(table.Path, table.bloomStartOffset)
		if err != nil {
			return err
		}
		defer iter.Close()
		iterators = append(iterators, iter)

		kv, err := iter.Next()
		if err == nil {
			heap.Push(pq, &HeapItem{
				Key:         kv.Key,
				Value:       kv.Value,
				IsTombstone: kv.IsTombstone,
				IterIdx:     i,
			})
		}
	}

	newSSTPath := filepath.Join(db.dir, fmt.Sprintf("sst_compacted_%d.sst", time.Now().UnixNano()))
	builder, err := NewSSTableBuilder(newSSTPath)
	if err != nil {
		return err
	}

	for pq.Len() > 0 {
		// The top item is the smallest key, AND the newest version of it
		top := heap.Pop(pq).(*HeapItem)
		currentKey := top.Key

		// L0 Merge: MUST write tombstones to the new L0 file to prevent zombie data
		builder.Add(top.Key, top.Value, top.IsTombstone)

		// Advance the winning iterator
		nextKV, err := iterators[top.IterIdx].Next()
		if err == nil {
			heap.Push(pq, &HeapItem{Key: nextKV.Key, Value: nextKV.Value, IsTombstone: nextKV.IsTombstone, IterIdx: top.IterIdx})
		}

		// Discard any older versions of this EXACT SAME key still in the heap
		for pq.Len() > 0 && (*pq)[0].Key == currentKey {
			older := heap.Pop(pq).(*HeapItem)
			nextOlder, err := iterators[older.IterIdx].Next()
			if err == nil {
				heap.Push(pq, &HeapItem{Key: nextOlder.Key, Value: nextOlder.Value, IsTombstone: nextOlder.IsTombstone, IterIdx: older.IterIdx})
			}
		}
	}

	builder.Finish()
	newReader, _ := NewSSTableReader(newSSTPath)

	// FIX 1: The Atomic Swap and Manifest Sync
	// Hold the lock for the ENTIRE state transition to prevent data loss from concurrent flushes
	db.mu.Lock()
	db.sstables = append([]*SSTableReader{newReader}, db.sstables[4:]...)

	db.manifest.Append(ManifestRecord{Action: "ADD", Path: newSSTPath, MinKey: newReader.MinKey, MaxKey: newReader.MaxKey})
	for _, oldTable := range tablesToMerge {
		db.manifest.Append(ManifestRecord{Action: "REMOVE", Path: oldTable.Path})
	}

	var activeRecords []ManifestRecord
	for _, sst := range db.sstables {
		activeRecords = append(activeRecords, ManifestRecord{Path: sst.Path, MinKey: sst.MinKey, MaxKey: sst.MaxKey})
	}

	// Compact manifest WHILE holding the global lock
	db.manifest.Compact(activeRecords)
	db.mu.Unlock() // Safe to release now

	// FIX 2: The "Rug Pull" Protection (Delayed GC)
	// Spawn a background janitor to clean up the physical files
	go func(tables []*SSTableReader) {
		// 10 seconds is more than enough time for any in-flight Get()
		// request holding an old pointer to finish reading.
		time.Sleep(10 * time.Second)

		for _, oldTable := range tables {
			oldTable.file.Close()
			os.Remove(oldTable.Path)
		}
	}(tablesToMerge)

	return nil
}
