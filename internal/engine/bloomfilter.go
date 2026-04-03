package engine

import (
	"hash/fnv"
)

// BloomFilter is a probabilistic data structure that tells us if a key is
// definitely NOT in a set, or POSSIBLY in a set.
type BloomFilter struct {
	bitset    []byte
	numHashes int
}

// NewBloomFilter creates a fresh, empty Bloom Filter for writing.
func NewBloomFilter(sizeInBytes int, numHashes int) *BloomFilter {
	return &BloomFilter{
		bitset:    make([]byte, sizeInBytes),
		numHashes: numHashes,
	}
}

// LoadBloomFilter wraps an existing byte slice (read from disk) for querying.
func LoadBloomFilter(data []byte, numHashes int) *BloomFilter {
	return &BloomFilter{
		bitset:    data,
		numHashes: numHashes,
	}
}

// Add inserts a key into the Bloom Filter.
func (bf *BloomFilter) Add(key string) {
	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()

	for i := 0; i < bf.numHashes; i++ {
		combinedHash := hash + uint32(i)*0x9e3779b9
		bitIdx := combinedHash % (uint32(len(bf.bitset)) * 8)
		byteIdx := bitIdx / 8
		bitOffset := bitIdx % 8
		bf.bitset[byteIdx] |= (1 << bitOffset)
	}
}

// MightContain checks if a key might be in the set.
// If it returns false, the key is 100% NOT in the set.
func (bf *BloomFilter) MightContain(key string) bool {
	if len(bf.bitset) == 0 {
		return true // If there's no filter, assume it might be there to be safe
	}

	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()

	for i := 0; i < bf.numHashes; i++ {
		combinedHash := hash + uint32(i)*0x9e3779b9
		bitIdx := combinedHash % (uint32(len(bf.bitset)) * 8)
		byteIdx := bitIdx / 8
		bitOffset := bitIdx % 8

		if (bf.bitset[byteIdx] & (1 << bitOffset)) == 0 {
			return false
		}
	}
	return true
}

// Bytes returns the underlying bitset for serialization to disk.
func (bf *BloomFilter) Bytes() []byte {
	return bf.bitset
}
