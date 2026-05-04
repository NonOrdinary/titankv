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

	// The Global Clock: Atomic counter for strict chronological ordering
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

	// CONSUME YOUR NEW WAL FIX HERE
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

	// THE CLOCK SYNC LOGIC
	// We seed with the physical clock to avoid the "Empty WAL Trap",
	// but we strictly enforce that the new sequence number MUST be greater
	// than the highest sequence number recovered from the WAL.
	// This protects against NTP clock-rewinds during server reboots!
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

	// 1. ATOMIC BACKPRESSURE
	for db.immutableMemTable != nil {
		db.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		db.mu.Lock()
	}

	// 2. Generate our atomic Sequence Number
	seqNum := atomic.AddUint64(&db.nextSeqNum, 1)

	// 3. Pre-compute the InternalKey for the WAL
	internalKey := EncodeInternalKey(userKey, seqNum, TypePut)

	if err := db.activeWAL.WriteRecord(internalKey, value); err != nil {
		db.mu.Unlock()
		return err
	}

	// 4. Put into MemTable (it handles its own InternalKey encoding)
	db.activeMemTable.Put(userKey, value, seqNum)

	// 5. Use the new smart size tracker
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

// Get defaults to querying the absolute latest state of the database.
func (db *DB) Get(key string) ([]byte, bool, error) {
	return db.GetAt(key, math.MaxUint64)
}

// GetAt is our new TIME-TRAVEL API. It returns the state of a key exactly as it existed at seqNum.
func (db *DB) GetAt(key string, targetSeqNum uint64) ([]byte, bool, error) {
	userKey := []byte(key)

	db.mu.RLock()
	defer db.mu.RUnlock()

	// 1. Check Active MemTable
	val, isDeleted, exists := db.activeMemTable.Get(userKey, targetSeqNum)
	if exists {
		if isDeleted {
			return nil, false, nil
		}
		return val, true, nil
	}

	// 2. Check Immutable MemTable
	if db.immutableMemTable != nil {
		val, isDeleted, exists = db.immutableMemTable.Get(userKey, targetSeqNum)
		if exists {
			if isDeleted {
				return nil, false, nil
			}
			return val, true, nil
		}
	}

	// 3. Check SSTables
	for i := len(db.sstables) - 1; i >= 0; i-- {
		sst := db.sstables[i]

		// NOTE: SSTables Min/Max bounds must now be based purely on UserKeys
		if key < sst.MinKey || key > sst.MaxKey {
			continue
		}

		// Phase 3 Todo: We will update sst.Get to accept targetSeqNum next!
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

		// The new signature of our Iterator streams perfectly into the Builder
		task.mt.Iterate(func(internalKey []byte, value []byte) {
			builder.Add(internalKey, value)
		})
		builder.Finish()

		reader, _ := NewSSTableReader(sstPath)

		// FIX: Acquire the global lock BEFORE touching the manifest or the sstables array
		db.mu.Lock()

		// Atomically commit the file to disk registry AND to RAM
		db.manifest.Append(ManifestRecord{
			Action: "ADD",
			Path:   sstPath,
			MinKey: reader.MinKey, // Note: Ensure this is just the UserKey string
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

		// Release the lock only after both states are perfectly synced
		db.mu.Unlock()

		task.wal.Close()
		os.Remove(task.wal.file.Name())
	}
	fmt.Println("Flush worker shut down cleanly.")
}

// Close gracefully shuts down the database.
func (db *DB) Close() error {
	// 1. Shut down the flush worker
	close(db.flushChan)

	// 2. Lock the database to ensure no in-flight writes are happening
	db.mu.Lock()
	defer db.mu.Unlock()

	// 3. Close the active WAL
	if err := db.activeWAL.Close(); err != nil {
		return err
	}

	// 4. Close the Manifest
	if err := db.manifest.Close(); err != nil {
		return err
	}

	// 5. Close all active SSTable Readers
	for _, sst := range db.sstables {
		if err := sst.file.Close(); err != nil {
			return err
		}
	}

	return nil
}
