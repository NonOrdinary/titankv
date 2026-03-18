/**
 * THEORY or Basic idea about this data structure
 * 1.Probabilistic data structure, can perform a negative test in logN time
 * 2.Key will remain sorted, as it is the requirement of our Memtable
 * 3.Height of each node is actually found by coin toss, and it is ultimately a sorted liinked list
 * */
package engine

import (
	"math/rand"
	"time"
)

// 32 is the standard used by LevelDB. It can handle billions of elements.
// we can search 2^32 elements in 32 jumps, super fast
const maxLevel = 32

// structure of our NODE
type Node struct {
	key    string
	record Record // Record, our basic unit of stored value in the memtable

	// forward[0] points to the next node in the standard linked list.
	// forward[1] points to the next node in the Level 1 express lane, etc.
	forward []*Node
}

type SkipList struct {
	// head is a dummy node. It doesn't store data, it just holds the
	// starting pointers for all the express lanes at all the levels
	head *Node

	// level is the current highest express lane in use.
	level int
}

// constructor of node
func newNode(key string, record Record, level int) *Node {
	return &Node{
		key:    key,
		record: record,
		// We make the slice size level+1 because arrays are 0-indexed,obvious
		forward: make([]*Node, level+1),
	}
}

// NewSkipList initializes an empty Skip List.
func NewSkipList() *SkipList {
	// random generator for coin toss ,initialisation.
	rand.Seed(time.Now().UnixNano())

	return &SkipList{
		// The dummy head node starts with the maximum possible levels.
		// it actually points to the first node that user will insert.
		head:  newNode("", Record{}, maxLevel),
		level: 0,
	}
}

// rossing the coin to determine the height of node.
func randomLevel() int {
	level := 0
	// rand.Float32() < 0.5 is our 50/50 coin flip.
	for rand.Float32() < 0.5 && level < maxLevel-1 {
		level++
	}
	return level
}

// Search looks for a key and returns its Record in O(log n) time.
func (s *SkipList) Search(key string) (Record, bool) {
	current := s.head

	// Start from the highest active express lane and work downwards.
	for i := s.level; i >= 0; i-- {
		// Keep moving forward on this specific level as long as the next key is smaller.
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
		// When the next key is >= our target, we drop down one level and repeat.
	}

	current = current.forward[0]
	// we should be at the node,if not then it's not available
	if current != nil && current.key == key {
		return current.record, true
	}

	return Record{}, false
}

// Insert adds a new key or updates an existing one in O(log n) time.
func (s *SkipList) Insert(key string, record Record) {
	// The 'update' array holds just the previous pointers to where out node would be inserted
	// It remembers the exact nodes where we dropped down a level during our search.
	update := make([]*Node, maxLevel)
	current := s.head

	for i := s.level; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
		update[i] = current // save when going down, it's the time we see this level for last time
	}

	// exact place of insertion ,if it doesn't already exist
	current = current.forward[0]

	//If the key already exists, just the record (or Tombstone) and return.
	if current != nil && current.key == key {
		current.record = record
		return
	}

	// get the levels, by coin tosses and map mark the update[i] = current[i]
	lvl := randomLevel()

	// If the coin flip gave us a taller node than currently exists in the list,
	// we need to update the skip list's max level and make the header point to
	// our new node at this height level,this ensures that this height is reachable when
	// we are starting the search
	if lvl > s.level {
		for i := s.level + 1; i <= lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	newNode := newNode(key, record, lvl)

	// Set up new node pointers and make sure the prev node points to this node
	for i := 0; i <= lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
}
