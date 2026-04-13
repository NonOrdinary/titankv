package engine

import (
	"bytes"
	"math/rand"
	"time"
)

const maxLevel = 32

// Node now stores raw byte slices. The InternalKey handles the metadata.
type Node struct {
	key     []byte // This is the InternalKey: [UserKey][SeqNum][Type]
	value   []byte // The raw data payload
	forward []*Node
}

type SkipList struct {
	head  *Node
	level int
}

func newNode(key []byte, value []byte, level int) *Node {
	return &Node{
		key:     key,
		value:   value,
		forward: make([]*Node, level+1),
	}
}

func NewSkipList() *SkipList {
	// Note: rand.Seed is deprecated in Go 1.20+, but keeping it as you had it for now.
	// In production, use a local rand.Rand instance to avoid global lock contention.
	rand.Seed(time.Now().UnixNano())

	return &SkipList{
		head:  newNode(nil, nil, maxLevel),
		level: 0,
	}
}

func randomLevel() int {
	level := 0
	for rand.Float32() < 0.5 && level < maxLevel-1 {
		level++
	}
	return level
}

// Search looks for the FIRST version of a UserKey that is <= the target SeqNum.
// searchKey must be a fully encoded InternalKey (e.g., [UserKey][TargetSeqNum][TypePut]).
// It returns (value, keyType, found).
func (s *SkipList) Search(searchKey []byte) ([]byte, byte, bool) {
	current := s.head

	for i := s.level; i >= 0; i-- {
		// We use our custom MVCC comparator here!
		// Keep moving right as long as the current node comes BEFORE our search key.
		for current.forward[i] != nil && CompareInternalKeys(current.forward[i].key, searchKey) < 0 {
			current = current.forward[i]
		}
	}

	current = current.forward[0]

	if current != nil {
		// We landed on a node. We must verify it's the SAME UserKey we were looking for.
		// (e.g., if we searched for "apple" but it didn't exist, we might have landed on "banana").
		userKeyNode, _, keyType := ParseInternalKey(current.key)
		userKeySearch, _, _ := ParseInternalKey(searchKey)

		if bytes.Equal(userKeyNode, userKeySearch) {
			return current.value, keyType, true
		}
	}

	return nil, 0, false
}

// Insert adds a new InternalKey.
// Because SeqNums always increase, we almost never update in-place anymore; we just append.
func (s *SkipList) Insert(internalKey []byte, value []byte) {
	update := make([]*Node, maxLevel)
	current := s.head

	for i := s.level; i >= 0; i-- {
		for current.forward[i] != nil && CompareInternalKeys(current.forward[i].key, internalKey) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	// Extreme Edge Case Safety: If somehow a write happens with the exact same
	// UserKey AND the exact same SeqNum, we overwrite to prevent list corruption.
	if current != nil && bytes.Equal(current.key, internalKey) {
		current.value = value
		return
	}

	lvl := randomLevel()

	if lvl > s.level {
		for i := s.level + 1; i <= lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	newNode := newNode(internalKey, value, lvl)

	for i := 0; i <= lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
}

// Iterate streams the exact InternalKeys and Values.
// Used during MemTable Flushing to disk.
func (s *SkipList) Iterate(cb func(internalKey []byte, value []byte)) {
	current := s.head.forward[0]
	for current != nil {
		cb(current.key, current.value)
		current = current.forward[0]
	}
}
