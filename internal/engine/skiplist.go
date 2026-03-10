package engine

import (
	"math/rand"
	"time"
)

// 32 is the standard used by LevelDB. It can handle billions of elements.
// we can search 2^32 elements in 32 jumps, super fast
const maxLevel = 32

// Node represents a single element in our Skip List.
type Node struct {
	key    string
	record Record // Record, our basic unit of stored value in the memtable

	// forward[0] points to the next node in the standard linked list.
	// forward[1] points to the next node in the Level 1 express lane, etc.
	forward []*Node
}

// SkipList is the actual data structure that will replace our Go map.
type SkipList struct {
	// head is a dummy node. It doesn't store data, it just holds the
	// starting pointers for all our express lanes at all the levels
	head *Node

	// level is the current highest express lane in use.
	level int
}

// newNode is a helper to create a node with correctly sized forward pointers.
func newNode(key string, record Record, level int) *Node {
	return &Node{
		key:    key,
		record: record,
		// We make the slice size level+1 because arrays are 0-indexed
		forward: make([]*Node, level+1),
	}
}

// NewSkipList initializes an empty Skip List.
func NewSkipList() *SkipList {
	// Seed the random number generator. We need this later to
	// "flip a coin" when deciding if a new node gets an express lane.
	rand.Seed(time.Now().UnixNano())

	return &SkipList{
		// The dummy head node starts with the maximum possible levels.
		// it actually points to the first node that user will insert.
		head:  newNode("", Record{}, maxLevel),
		level: 0,
	}
}

// randomLevel flips a coin to determine the node's height.
func randomLevel() int {
	level := 0
	// rand.Float32() < 0.5 is our 50/50 coin flip.
	// We cap the height at maxLevel - 1 to prevent out-of-bounds errors.
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

	// We are now at Level 0, right before where our key should be.
	// Step forward one last time.
	current = current.forward[0]

	// If the key matches, we found it!
	if current != nil && current.key == key {
		return current.record, true
	}

	return Record{}, false
}

// Insert adds a new key or updates an existing one in O(log n) time.
func (s *SkipList) Insert(key string, record Record) {
	// The 'update' array holds the "breadcrumbs".
	// It remembers the exact nodes where we dropped down a level during our search.
	// We need these breadcrumbs to know exactly where to splice in our new node.
	update := make([]*Node, maxLevel)
	current := s.head

	// Step 1: Find the insertion point, leaving breadcrumbs along the way.
	for i := s.level; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
		update[i] = current
	}

	// Step to the exact spot at Level 0.
	current = current.forward[0]

	// Step 2: If the key already exists, just update the record (or Tombstone) and return.
	if current != nil && current.key == key {
		current.record = record
		return
	}

	// Step 3: It's a brand new key. Flip the coin to see how many express lanes it gets.
	lvl := randomLevel()

	// If the coin flip gave us a taller node than currently exists in the list,
	// we need to update the skip list's max level and our breadcrumbs.
	if lvl > s.level {
		for i := s.level + 1; i <= lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	// Step 4: Create the new node.
	newNode := newNode(key, record, lvl)

	// Step 5: Splice the node into the linked lists using our breadcrumbs.
	for i := 0; i <= lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
}
