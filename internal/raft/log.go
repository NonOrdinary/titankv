package raft

import (
	"errors"
	"sync"
)

var (
	ErrLogOutOfBounds = errors.New("log index out of bounds")
)

// LogEntry represents a single deterministic operation in the state machine.
type LogEntry struct {
	Term    uint64 // The term in which the entry was received by the leader
	Index   uint64 // The monotonic index of the entry
	Command []byte // The serialized Phase 4 Request (e.g., PUT or DEL)
}

// Log represents the in-memory ledger of all state machine commands.
// In a full production system, this is durable and backed by a specialized WAL.
type Log struct {
	mu      sync.RWMutex
	entries []LogEntry
}

// NewLog initializes a new Raft log.
func NewLog() *Log {
	// Standard Raft implementation relies on 1-based indexing to simplify edge-case math.
	// We inject a dummy entry at Index 0, Term 0 to act as the mathematical floor.
	return &Log{
		entries: []LogEntry{
			{Term: 0, Index: 0, Command: nil},
		},
	}
}

// Append adds new entries to the end of the log.
func (l *Log) Append(entries ...LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
}

// Get fetches a specific entry by its Raft index.
func (l *Log) Get(index uint64) (LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 || index >= uint64(len(l.entries)) {
		return LogEntry{}, ErrLogOutOfBounds
	}
	return l.entries[index], nil
}

// LastInfo returns the index and term of the most recently appended log entry.
// This is heavily used during Leader Elections to determine which candidate has the most up-to-date state.
func (l *Log) LastInfo() (lastIndex uint64, lastTerm uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	last := l.entries[len(l.entries)-1]
	return last.Index, last.Term
}

// TermAtIndex safely looks up the term for a given index.
func (l *Log) TermAtIndex(index uint64) (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index >= uint64(len(l.entries)) {
		return 0, ErrLogOutOfBounds
	}
	return l.entries[index].Term, nil
}

// TruncateSuffix strictly enforces the Log Matching Property.
// If a Follower's log conflicts with the Leader's log (same index, different term),
// the Follower MUST delete the conflicting entry and all that follow it.
func (l *Log) TruncateSuffix(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index >= uint64(len(l.entries)) {
		return ErrLogOutOfBounds
	}

	// Slicing re-uses the underlying array but limits the accessible bound,
	// allowing GC to overwrite the uncommitted/conflicting data later.
	l.entries = l.entries[:index]
	return nil
}

// EntriesAfter returns a slice of all entries sequentially following the given index.
// This is used by the Leader to replicate missing data to stale Followers.
func (l *Log) EntriesAfter(index uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index+1 >= uint64(len(l.entries)) {
		return nil
	}

	// Return a deep copy to prevent data races when serializing over RPC
	out := make([]LogEntry, uint64(len(l.entries))-(index+1))
	copy(out, l.entries[index+1:])
	return out
}
