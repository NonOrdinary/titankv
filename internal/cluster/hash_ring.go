package cluster

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// HashRing manages the mapping of keys to physical network nodes.
type HashRing struct {
	mu           sync.RWMutex
	nodes        []uint32          // Sorted list of hashed node positions
	nodeMap      map[uint32]string // Maps a hash position back to a node ID (e.g., "127.0.0.1:8080")
	virtualNodes int               // Number of "Vnodes" per physical node to prevent hotspots
}

// NewHashRing creates a ring with a specific number of virtual nodes.
// Virtual nodes ensure that data is spread evenly even if physical node hashes are close together.
func NewHashRing(virtualNodes int) *HashRing {
	return &HashRing{
		nodeMap:      make(map[uint32]string),
		virtualNodes: virtualNodes,
	}
}

// AddNode registers a new physical server into the cluster.
func (r *HashRing) AddNode(nodeId string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.virtualNodes; i++ {
		// We hash "node_id:vnode_index" to create different points on the ring
		hash := crc32.ChecksumIEEE([]byte(nodeId + ":" + strconv.Itoa(i)))
		r.nodes = append(r.nodes, hash)
		r.nodeMap[hash] = nodeId
	}

	// Raft requires things to be deterministic. We must keep the ring sorted.
	sort.Slice(r.nodes, func(i, j int) bool {
		return r.nodes[i] < r.nodes[j]
	})
}

// GetNode finds the responsible node for a given key.
func (r *HashRing) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))

	// Binary search (O(log N)) to find the first node hash >= key hash
	idx := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i] >= hash
	})

	// If we reached the end of the slice, we "wrap around" to the first node (index 0)
	if idx == len(r.nodes) {
		idx = 0
	}

	return r.nodeMap[r.nodes[idx]]
}
