package engine

import (
	"bytes"
	"sync"
	"testing"
)

func TestMemTable_PutAndGet(t *testing.T) {
	mt := NewMemTable()
	key := "hunter"
	val := []byte("sung_jinwoo")

	mt.Put(key, val)

	record, exists := mt.Get(key)
	if !exists {
		t.Fatalf("expected key %q to exist", key)
	}
	if record.Deleted {
		t.Fatalf("expected key %q to not be deleted", key)
	}

	// We use bytes.Equal to compare byte slices in Go Because == on a slice only checks if they point to the same memory address
	if !bytes.Equal(record.Value, val) {
		t.Fatalf("expected value %q, got %q", val, record.Value)
	}
}

func TestMemTable_DeleteTombstone(t *testing.T) {
	mt := NewMemTable()
	key := "demon"
	val := []byte("kibutsuji")

	mt.Put(key, val)
	mt.Delete(key) // This should trigger the tombstone

	record, exists := mt.Get(key)
	if !exists {
		t.Fatalf("expected key %q to exist in map (as a tombstone)", key)
	}
	if !record.Deleted {
		t.Fatalf("expected key %q to be marked as Deleted: true", key)
	}
}

func TestMemTable_Concurrency(t *testing.T) {
	mt := NewMemTable()
	var wg sync.WaitGroup

	// We use a WaitGroup to tell the test to wait for all goroutines to finish
	// before exiting. Otherwise, the test finishes before the threads do!

	// Fire up 100 concurrent writers

	// the function here defined is fastGoroutine type function which would typically look like
	/**
			go func(msg string) {
	        fmt.Println(msg)
	    	}("going : it is the argument to the goroutine")
	*/
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mt.Put("shared_key", []byte("concurrent_value"))
		}()
	}

	// Fire up 100 concurrent readers at the exact same time
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mt.Get("shared_key")
		}()
	}

	// Waiting here for all goroutines to finish their work
	wg.Wait()
}
