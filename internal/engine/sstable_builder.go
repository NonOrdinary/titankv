package engine

import (
	"encoding/binary"
	"os"
)

// blockSize for logical mapping for sparse Indexing
const blockSize = 4096 // almost all OS keeps 4kb pages, so it's better to make block size equal to page
// for direct replace of block to page

// IndexEntry represents a single pointer in our Sparse Index.
// It points to the exact byte offset where a 4KB block begins.
type IndexEntry struct {
	Key    string
	Offset uint32
}

// SSTableBuilder constructs an immutable, statically indexed file on disk.
type SSTableBuilder struct {
	file              *os.File
	offset            uint32 // Total bytes written to the file so far
	blockBytesWritten uint32 // Bytes written in the CURRENT 4KB block
	index             []IndexEntry

	minKey string
	maxKey string
	bloom  *BloomFilter
}

// NewSSTableBuilder initializes the file and the sparse index array.
func NewSSTableBuilder(path string) (*SSTableBuilder, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	return &SSTableBuilder{
		file:  f,
		index: make([]IndexEntry, 0, 1000), // preallocate to avoid GC thrashing
		bloom: NewBloomFilter(4096, 3),     // 4KB filter, 3 hash functions
	}, nil
}

// Add the key to our file, also updating min and max keys for max-min index
func (b *SSTableBuilder) Add(key string, value []byte, isTombstone bool) error {
	//  Update Min/Max Keys and Bloom Filter on every insert
	if b.minKey == "" || key < b.minKey {
		b.minKey = key
	}
	if b.maxKey == "" || key > b.maxKey {
		b.maxKey = key
	}
	b.bloom.Add(key)

	// If this is the very first key of a new block, add it to the Sparse Index
	if b.blockBytesWritten == 0 {
		b.index = append(b.index, IndexEntry{Key: key, Offset: b.offset})
	}

	// Serialise each record,then put it inside block , until we hit the trigger
	recordSize := 1 + 4 + len(key) + 4 + len(value)
	buf := make([]byte, recordSize)

	if isTombstone {
		buf[0] = 1
	} else {
		buf[0] = 0
	}

	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(key)))
	offset := 5
	copy(buf[offset:], key)
	offset += len(key)

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(value)))
	offset += 4
	copy(buf[offset:], value)

	// Write to disk
	_, err := b.file.Write(buf)
	if err != nil {
		return err
	}

	// Update our offsets to next positions of write
	b.offset += uint32(recordSize)
	b.blockBytesWritten += uint32(recordSize)

	// 7. The Sparse Index Trigger
	// If this block crossed the 4KB boundary, we reset the counter.
	// NOTE: It ensures that even if the value is 5KB, it would still stay in one file, as blockSize isn't a hard limit
	// This ensures the NEXT call to Add() will record a new IndexEntry.
	if b.blockBytesWritten >= blockSize {
		b.blockBytesWritten = 0
	}

	return nil
}

// Writes the Sparse Index to the file
func (b *SSTableBuilder) Finish() error {
	// Record exactly where the Data Blocks end and the Index begins
	bloomStartOffset := b.offset
	if _, err := b.file.Write(b.bloom.Bytes()); err != nil {
		return err
	}
	b.offset += uint32(len(b.bloom.Bytes()))

	indexStartOffset := b.offset

	// Serialize the Sparse Index
	// First, write how many entries we have (4 bytes)
	countBuf := make([]byte, 4) // 32 bits  = 2 ^ 32 entries tracked
	binary.LittleEndian.PutUint32(countBuf, uint32(len(b.index)))
	if _, err := b.file.Write(countBuf); err != nil {
		return err
	}
	b.offset += 4

	// Write each Index Entry: [KeyLen(4)] + [Key] + [BlockOffset(4)]
	for _, entry := range b.index {
		entrySize := 4 + len(entry.Key) + 4
		entryBuf := make([]byte, entrySize)

		binary.LittleEndian.PutUint32(entryBuf[0:4], uint32(len(entry.Key)))
		copy(entryBuf[4:], entry.Key)
		binary.LittleEndian.PutUint32(entryBuf[4+len(entry.Key):], entry.Offset)

		if _, err := b.file.Write(entryBuf); err != nil {
			return err
		}
		b.offset += uint32(entrySize)
	}
	// Min Max key Range : We need it here too because we want to make sure that even if
	// our main file (MANIFEST : Registry) get's corrupted, we can rebuild it all by reading all files
	metaStartOffset := b.offset

	metaSize := 4 + len(b.minKey) + 4 + len(b.maxKey)
	metaBuf := make([]byte, metaSize)

	binary.LittleEndian.PutUint32(metaBuf[0:4], uint32(len(b.minKey)))
	metaOffset := 4
	copy(metaBuf[metaOffset:], b.minKey)
	metaOffset += len(b.minKey)

	binary.LittleEndian.PutUint32(metaBuf[metaOffset:metaOffset+4], uint32(len(b.maxKey)))
	metaOffset += 4
	copy(metaBuf[metaOffset:], b.maxKey)

	if _, err := b.file.Write(metaBuf); err != nil {
		return err
	}
	b.offset += uint32(metaSize)

	footerBuf := make([]byte, 16)
	// [0 : 3] -> bloom filter offset, it's written 4, but it would go as [l,r)
	binary.LittleEndian.PutUint32(footerBuf[0:4], bloomStartOffset)
	// [4 : 7] -> sparse index offset
	binary.LittleEndian.PutUint32(footerBuf[4:8], indexStartOffset)
	// [8 : 11] -> min - max range data  offset
	binary.LittleEndian.PutUint32(footerBuf[8:12], metaStartOffset)
	//[12 : 15] -> Magic number for protection against corrupted file as if last 4 bytes are written correctly
	// then everything is fine
	binary.LittleEndian.PutUint32(footerBuf[12:16], 0xABCD1234)

	if _, err := b.file.Write(footerBuf); err != nil {
		return err
	}

	// Sync the file to the DISK
	b.file.Sync()
	return b.file.Close()
}
