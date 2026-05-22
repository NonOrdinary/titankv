package raft

import (
	"errors"
	"sync"
)

var (
	ErrLogOutOfBounds = errors.New("log index out of bounds")
)

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

type Log struct {
	mu      sync.RWMutex
	entries []LogEntry
}

func NewLog() *Log {
	return &Log{
		entries: []LogEntry{
			{Term: 0, Index: 0, Command: nil},
		},
	}
}

func (l *Log) Append(entries ...LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entries...)
}

func (l *Log) Get(index uint64) (LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 || index >= uint64(len(l.entries)) {
		return LogEntry{}, ErrLogOutOfBounds
	}
	return l.entries[index], nil
}

func (l *Log) LastInfo() (lastIndex uint64, lastTerm uint64) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	last := l.entries[len(l.entries)-1]
	return last.Index, last.Term
}

func (l *Log) TermAtIndex(index uint64) (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index >= uint64(len(l.entries)) {
		return 0, ErrLogOutOfBounds
	}
	return l.entries[index].Term, nil
}

func (l *Log) TruncateSuffix(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index >= uint64(len(l.entries)) {
		return ErrLogOutOfBounds
	}

	l.entries = l.entries[:index]
	return nil
}

func (l *Log) EntriesAfter(index uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index+1 >= uint64(len(l.entries)) {
		return nil
	}

	out := make([]LogEntry, uint64(len(l.entries))-(index+1))
	copy(out, l.entries[index+1:])
	return out
}
