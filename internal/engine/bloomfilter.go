package engine

import (
	"hash/fnv"
)

type BloomFilter struct {
	bitset    []byte
	numHashes int
}

func NewBloomFilter(sizeInBytes int, numHashes int) *BloomFilter {
	return &BloomFilter{
		bitset:    make([]byte, sizeInBytes),
		numHashes: numHashes,
	}
}

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

func (bf *BloomFilter) MightContain(key string) bool {
	if len(bf.bitset) == 0 {
		return true
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

func (bf *BloomFilter) Bytes() []byte {
	return bf.bitset
}
