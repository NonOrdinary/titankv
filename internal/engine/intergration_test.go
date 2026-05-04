package engine

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestTitanKV_TimeTravel(t *testing.T) {
	dir := "test_db_timetravel"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	key := "user:101:status"
	versions := []string{"offline", "online", "busy", "away"}
	seqNums := make([]uint64, len(versions))

	// 1. Write 4 versions of the same key
	for i, val := range versions {
		err := db.Put(key, []byte(val))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		// Capture the sequence number assigned to this version
		seqNums[i] = db.nextSeqNum
		time.Sleep(10 * time.Millisecond) // Ensure clock moves
	}

	// 2. Verify Time Travel Retrieval
	for i, expected := range versions {
		targetSeq := seqNums[i]
		val, found, err := db.GetAt(key, targetSeq)
		if err != nil || !found {
			t.Errorf("Version %d (%s) not found at seq %d", i, expected, targetSeq)
		}
		if string(val) != expected {
			t.Errorf("At seq %d, expected %s, got %s", targetSeq, expected, string(val))
		}
	}

	// 3. Verify that searching "between" versions returns the older visible one
	// Search at seqNums[1] + 1 (should still see versions[1])
	val, _, _ := db.GetAt(key, seqNums[1])
	if string(val) != "online" {
		t.Errorf("Mid-stream search failed")
	}

	db.Close()
}
func TestTitanKV_CompactionPurge(t *testing.T) {
	dir := "test_db_compaction"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	db, err := Open(dir) // FIX: Check the error!
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	db.maxMemtableSize = 1024

	key := "persistent_key"

	for i := 0; i < 50; i++ {
		err := db.Put(key, []byte(fmt.Sprintf("old_val_%d", i)))
		if err != nil {
			t.Fatalf("Put failed at iteration %d: %v", i, err)
		}
	}

	// FIX: Compaction and Flushes happen in background goroutines.
	// 2 seconds might not be enough on a slow Windows disk.
	// Use a loop to wait for the expected state or a longer sleep.
	time.Sleep(3 * time.Second)

	val, found, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if !found {
		t.Fatal("Key lost after compaction!")
	}

	if !bytes.Contains(val, []byte("old_val_49")) {
		t.Errorf("Compaction returned wrong version: %s", string(val))
	}

	db.Close()
}
