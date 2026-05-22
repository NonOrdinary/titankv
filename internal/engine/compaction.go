package engine

import (
	"bytes"
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type HeapItem struct {
	InternalKey []byte
	Value       []byte
	IterIdx     int
}

type KVHeap []*HeapItem

func (h KVHeap) Len() int { return len(h) }
func (h KVHeap) Less(i, j int) bool {
	return CompareInternalKeys(h[i].InternalKey, h[j].InternalKey) < 0
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

func (db *DB) Compact() error {
	defer func() {
		db.mu.Lock()
		db.isCompacting = false
		db.mu.Unlock()
	}()

	watermark := uint64(time.Now().Add(-24 * time.Hour).UnixNano())

	db.mu.RLock()
	if len(db.sstables) < 4 {
		db.mu.RUnlock()
		return nil
	}
	tablesToMerge := make([]*SSTableReader, 4)
	copy(tablesToMerge, db.sstables[:4])
	db.mu.RUnlock()

	var iterators []*SSTableIterator
	pq := &KVHeap{}
	heap.Init(pq)

	for i, table := range tablesToMerge {
		iter, err := NewSSTableIterator(table.Path, table.bloomStartOffset)
		if err != nil {
			return err
		}
		defer iter.Close()
		iterators = append(iterators, iter)

		if kv, err := iter.Next(); err == nil {
			heap.Push(pq, &HeapItem{InternalKey: kv.InternalKey, Value: kv.Value, IterIdx: i})
		}
	}

	newSSTPath := filepath.Join(db.dir, fmt.Sprintf("sst_compacted_%d.sst", time.Now().UnixNano()))
	builder, err := NewSSTableBuilder(newSSTPath)
	if err != nil {
		return err
	}

	for pq.Len() > 0 {
		top := heap.Pop(pq).(*HeapItem)
		userKey, seqNum, keyType := ParseInternalKey(top.InternalKey)

		keepVersion := true

		if seqNum <= watermark && keyType == TypeDelete {
			keepVersion = false
		}

		if keepVersion {
			builder.Add(top.InternalKey, top.Value)
		}
		for {
			nextKV, err := iterators[top.IterIdx].Next()
			if err != nil {
				break
			}

			nUserKey, _, _ := ParseInternalKey(nextKV.InternalKey)
			if !bytes.Equal(nUserKey, userKey) {
				heap.Push(pq, &HeapItem{InternalKey: nextKV.InternalKey, Value: nextKV.Value, IterIdx: top.IterIdx})
				break
			}

			if seqNum <= watermark {
				continue
			}

			heap.Push(pq, &HeapItem{InternalKey: nextKV.InternalKey, Value: nextKV.Value, IterIdx: top.IterIdx})
			break
		}
	}

	builder.Finish()
	newReader, _ := NewSSTableReader(newSSTPath)

	// Atomic Swap and Manifest Sync
	db.mu.Lock()
	db.sstables = append([]*SSTableReader{newReader}, db.sstables[4:]...)
	db.manifest.Append(ManifestRecord{Action: "ADD", Path: newSSTPath, MinKey: newReader.MinKey, MaxKey: newReader.MaxKey})
	for _, oldTable := range tablesToMerge {
		db.manifest.Append(ManifestRecord{Action: "REMOVE", Path: oldTable.Path})
	}
	db.mu.Unlock()
	go func(tables []*SSTableReader) {
		time.Sleep(10 * time.Second)
		for _, oldTable := range tables {
			oldTable.file.Close()
			os.Remove(oldTable.Path)
		}
	}(tablesToMerge)

	return nil
}
