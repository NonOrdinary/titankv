package engine

import (
	"bytes"
	"math/rand"
	"time"
)

const maxLevel = 32

type Node struct {
	key     []byte
	value   []byte
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

func (s *SkipList) Search(searchKey []byte) ([]byte, byte, bool) {
	current := s.head

	for i := s.level; i >= 0; i-- {
		for current.forward[i] != nil && CompareInternalKeys(current.forward[i].key, searchKey) < 0 {
			current = current.forward[i]
		}
	}

	current = current.forward[0]

	if current != nil {
		userKeyNode, _, keyType := ParseInternalKey(current.key)
		userKeySearch, _, _ := ParseInternalKey(searchKey)

		if bytes.Equal(userKeyNode, userKeySearch) {
			return current.value, keyType, true
		}
	}

	return nil, 0, false
}

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

func (s *SkipList) Iterate(cb func(internalKey []byte, value []byte)) {
	current := s.head.forward[0]
	for current != nil {
		cb(current.key, current.value)
		current = current.forward[0]
	}
}
