package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type flushTask struct {
	mt  *MemTable
	wal *WAL
}

type DB struct {
	mu sync.RWMutex
	// we require RW mutex over mutex it due to reader writer problem
	dir               string
	activeMemTable    *MemTable
	immutableMemTable *MemTable // this is required to enable get requests while Memtable is being flushed to DISK
	activeWAL         *WAL
	manifest          *Manifest
	memtableSize      int
	isCompacting      bool
	maxMemtableSize   int

	sstables []*SSTableReader // we don't keep instance of SS tables, but rather the image of SS table reader
	// these are always active for now,i have a fix for it(durable storage)
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
			return nil, err // If a file in the manifest is missing/corrupted, we fail safely
		}
		sstables = append(sstables, reader)
	}

	// Open the Manifest for appending future flushes
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
	if err := wal.Recover(mt); err != nil {
		return nil, err
	}

	db := &DB{
		dir:             dir,
		activeMemTable:  mt,
		activeWAL:       wal,
		manifest:        manifest,
		memtableSize:    0,
		maxMemtableSize: 4 * 1024 * 1024,
		sstables:        sstables,
		flushChan:       make(chan *flushTask),
	}

	go db.flushWorker()

	return db, nil
}

func (db *DB) Put(key string, value []byte) error {

	db.mu.Lock()

	// 1. ATOMIC BACKPRESSURE (Write Stalling)
	// If the immutable table is occupied, the disk is too slow. Force the user to wait.
	for db.immutableMemTable != nil {
		db.mu.Unlock()
		time.Sleep(2 * time.Millisecond) // Yield CPU to the background worker
		db.mu.Lock()                     // Re-acquire and check again safely
	}

	if err := db.activeWAL.WriteRecord(key, value, false); err != nil {
		db.mu.Unlock()
		return err
	}

	db.activeMemTable.Put(key, value)
	recordSize := 1 + 4 + len(key) + 4 + len(value)
	db.memtableSize += recordSize

	if db.memtableSize >= db.maxMemtableSize {
		db.triggerFlush()
	} else {
		db.mu.Unlock()
	}

	return nil
}

func (db *DB) Get(key string) ([]byte, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	record, exists := db.activeMemTable.Get(key)
	if exists {
		if record.Deleted {
			return nil, false, nil
		}
		return record.Value, true, nil
	}

	if db.immutableMemTable != nil {
		record, exists := db.immutableMemTable.Get(key)
		if exists {
			if record.Deleted {
				return nil, false, nil
			}
			return record.Value, true, nil
		}
	}

	for i := len(db.sstables) - 1; i >= 0; i-- {
		sst := db.sstables[i]

		if key < sst.MinKey || key > sst.MaxKey {
			continue
		}

		val, found, err := sst.Get(key)
		if err != nil {
			return nil, false, err
		}
		if found {
			if val == nil {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	return nil, false, nil
}

func (db *DB) Delete(key string) error {
	// FIX 2: Check for backpressure
	db.mu.Lock()

	// 1. ATOMIC BACKPRESSURE (Write Stalling)
	// If the immutable table is occupied, the disk is too slow. Force the user to wait.
	for db.immutableMemTable != nil {
		db.mu.Unlock()
		time.Sleep(2 * time.Millisecond) // Yield CPU to the background worker
		db.mu.Lock()                     // Re-acquire and check again safely
	}

	if err := db.activeWAL.WriteRecord(key, nil, true); err != nil {
		db.mu.Unlock()
		return err
	}

	db.activeMemTable.Delete(key)
	db.memtableSize += 1 + 4 + len(key) + 4

	if db.memtableSize >= db.maxMemtableSize {
		db.triggerFlush()
	} else {
		db.mu.Unlock()
	}

	return nil
}

func (db *DB) triggerFlush() {
	frozenMemTable := db.activeMemTable
	frozenWAL := db.activeWAL

	db.immutableMemTable = frozenMemTable
	db.activeMemTable = NewMemTable()
	db.memtableSize = 0

	newWALPath := filepath.Join(db.dir, fmt.Sprintf("wal_%d.log", time.Now().UnixNano()))
	newWAL, _ := NewWAL(newWALPath)
	db.activeWAL = newWAL

	// Unlock instantly to unblock reads and future writes
	db.mu.Unlock()

	// FIX 3: Asynchronous Handoff.
	// We spawn a tiny goroutine just to send the task into the unbuffered channel.
	// This means triggerFlush() finishes in nanoseconds, and the user isn't blocked.
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

		// FIX 1: Stream the RAM directly into the file without duplicating memory
		task.mt.Iterate(func(key string, record Record) {
			builder.Add(key, record.Value, record.Deleted)
		})
		builder.Finish()

		reader, _ := NewSSTableReader(sstPath)

		db.manifest.Append(ManifestRecord{
			Action: "ADD",
			Path:   sstPath,
			MinKey: reader.MinKey,
			MaxKey: reader.MaxKey,
		})
		db.mu.Lock()
		db.sstables = append(db.sstables, reader)
		db.immutableMemTable = nil // Flush complete, unblock incoming Puts

		numTables := len(db.sstables)
		isComp := db.isCompacting

		// Trigger if we have 4+ tables AND a compaction isn't already running
		if numTables >= 4 && !isComp {
			db.isCompacting = true
			go db.Compact()
		}
		db.mu.Unlock()

		task.wal.Close()
		os.Remove(task.wal.file.Name())
	}
}
