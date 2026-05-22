package engine

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type flushTask struct {
	mt  *MemTable
	wal *WAL
}

type DB struct {
	mu sync.RWMutex

	dir               string
	activeMemTable    *MemTable
	immutableMemTable *MemTable
	activeWAL         *WAL
	manifest          *Manifest
	isCompacting      bool
	maxMemtableSize   int

	nextSeqNum uint64

	sstables  []*SSTableReader
	flushChan chan *flushTask
}

func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	manifestPath := filepath.Join(dir, "MANIFEST.log")
	activeRecords, err := RecoverManifest(manifestPath)
	if err != nil {
		return nil, err
	}

	var sstables []*SSTableReader
	for _, rec := range activeRecords {
		reader, err := NewSSTableReader(rec.Path)
		if err != nil {
			return nil, err
		}
		sstables = append(sstables, reader)
	}

	manifest, err := NewManifest(manifestPath)
	if err != nil {
		return nil, err
	}

	walPath := filepath.Join(dir, "active.wal")
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	mt := NewMemTable()

	maxWalSeq, err := wal.Recover(mt)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dir:             dir,
		activeMemTable:  mt,
		activeWAL:       wal,
		manifest:        manifest,
		maxMemtableSize: 4 * 1024 * 1024,
		sstables:        sstables,
		flushChan:       make(chan *flushTask),
	}

	baseSeq := uint64(time.Now().UnixNano())
	if maxWalSeq >= baseSeq {
		baseSeq = maxWalSeq + 1
	}
	db.nextSeqNum = baseSeq

	go db.flushWorker()

	return db, nil
}

func (db *DB) Put(key string, value []byte) error {
	userKey := []byte(key)

	db.mu.Lock()

	for db.immutableMemTable != nil {
		db.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		db.mu.Lock()
	}

	seqNum := atomic.AddUint64(&db.nextSeqNum, 1)

	internalKey := EncodeInternalKey(userKey, seqNum, TypePut)

	if err := db.activeWAL.WriteRecord(internalKey, value); err != nil {
		db.mu.Unlock()
		return err
	}

	db.activeMemTable.Put(userKey, value, seqNum)

	if db.activeMemTable.ApproximateSize() >= db.maxMemtableSize {
		db.triggerFlush()
	} else {
		db.mu.Unlock()
	}

	return nil
}

func (db *DB) Delete(key string) error {
	userKey := []byte(key)

	db.mu.Lock()

	for db.immutableMemTable != nil {
		db.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		db.mu.Lock()
	}

	seqNum := atomic.AddUint64(&db.nextSeqNum, 1)
	internalKey := EncodeInternalKey(userKey, seqNum, TypeDelete)

	if err := db.activeWAL.WriteRecord(internalKey, nil); err != nil {
		db.mu.Unlock()
		return err
	}

	db.activeMemTable.Delete(userKey, seqNum)

	if db.activeMemTable.ApproximateSize() >= db.maxMemtableSize {
		db.triggerFlush()
	} else {
		db.mu.Unlock()
	}

	return nil
}

func (db *DB) Get(key string) ([]byte, bool, error) {
	return db.GetAt(key, math.MaxUint64)
}

func (db *DB) GetAt(key string, targetSeqNum uint64) ([]byte, bool, error) {
	userKey := []byte(key)

	db.mu.RLock()
	defer db.mu.RUnlock()

	val, isDeleted, exists := db.activeMemTable.Get(userKey, targetSeqNum)
	if exists {
		if isDeleted {
			return nil, false, nil
		}
		return val, true, nil
	}

	if db.immutableMemTable != nil {
		val, isDeleted, exists = db.immutableMemTable.Get(userKey, targetSeqNum)
		if exists {
			if isDeleted {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	for i := len(db.sstables) - 1; i >= 0; i-- {
		sst := db.sstables[i]

		if key < sst.MinKey || key > sst.MaxKey {
			continue
		}

		val, isDeleted, found, err := sst.Get(userKey, targetSeqNum)
		if err != nil {
			return nil, false, err
		}
		if found {
			if isDeleted {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	return nil, false, nil
}

func (db *DB) triggerFlush() {
	frozenMemTable := db.activeMemTable
	frozenWAL := db.activeWAL

	db.immutableMemTable = frozenMemTable
	db.activeMemTable = NewMemTable()

	newWALPath := filepath.Join(db.dir, fmt.Sprintf("wal_%d.log", time.Now().UnixNano()))
	newWAL, _ := NewWAL(newWALPath)
	db.activeWAL = newWAL

	db.mu.Unlock()

	go func(task *flushTask) {
		db.flushChan <- task
	}(&flushTask{
		mt:  frozenMemTable,
		wal: frozenWAL,
	})
}

func (db *DB) flushWorker() {
	for task := range db.flushChan {
		sstPath := filepath.Join(db.dir, fmt.Sprintf("sst_%d.sst", time.Now().UnixNano()))
		builder, _ := NewSSTableBuilder(sstPath)

		task.mt.Iterate(func(internalKey []byte, value []byte) {
			builder.Add(internalKey, value)
		})
		builder.Finish()

		reader, _ := NewSSTableReader(sstPath)

		db.mu.Lock()

		db.manifest.Append(ManifestRecord{
			Action: "ADD",
			Path:   sstPath,
			MinKey: reader.MinKey,
			MaxKey: reader.MaxKey,
		})

		db.sstables = append(db.sstables, reader)
		db.immutableMemTable = nil

		numTables := len(db.sstables)
		isComp := db.isCompacting

		if numTables >= 4 && !isComp {
			db.isCompacting = true
			go db.Compact()
		}

		db.mu.Unlock()

		task.wal.Close()
		os.Remove(task.wal.file.Name())
	}
	fmt.Println("Flush worker shut down cleanly.")
}

func (db *DB) Close() error {
	close(db.flushChan)

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.activeWAL.Close(); err != nil {
		return err
	}

	if err := db.manifest.Close(); err != nil {
		return err
	}

	for _, sst := range db.sstables {
		if err := sst.file.Close(); err != nil {
			return err
		}
	}

	return nil
}
