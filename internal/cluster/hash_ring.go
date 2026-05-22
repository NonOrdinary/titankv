package cluster

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type HashRing struct {
	mu           sync.RWMutex
	nodes        []uint32
	nodeMap      map[uint32]string
	virtualNodes int
}

func NewHashRing(virtualNodes int) *HashRing {
	return &HashRing{
		nodeMap:      make(map[uint32]string),
		virtualNodes: virtualNodes,
	}
}

func (r *HashRing) AddNode(nodeId string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.virtualNodes; i++ {
		hash := crc32.ChecksumIEEE([]byte(nodeId + ":" + strconv.Itoa(i)))
		r.nodes = append(r.nodes, hash)
		r.nodeMap[hash] = nodeId
	}

	sort.Slice(r.nodes, func(i, j int) bool {
		return r.nodes[i] < r.nodes[j]
	})
}

func (r *HashRing) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.nodes) == 0 {
		return ""
	}

	hash := crc32.ChecksumIEEE([]byte(key))

	idx := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i] >= hash
	})

	if idx == len(r.nodes) {
		idx = 0
	}

	return r.nodeMap[r.nodes[idx]]
}
