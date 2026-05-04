package engine

import (
	"bytes"
	"encoding/binary"
	"os"
)

const blockSize = 4096

type IndexEntry struct {
	Key    []byte
	Offset uint32
}

type SSTableBuilder struct {
	file              *os.File
	offset            uint32
	blockBytesWritten uint32
	index             []IndexEntry

	minKey []byte
	maxKey []byte
	bloom  *BloomFilter

	// FIX: Reusable scratch buffer to prevent GC pressure during high-velocity writes
	scratchBuf []byte
}

func NewSSTableBuilder(path string) (*SSTableBuilder, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	return &SSTableBuilder{
		file:       f,
		index:      make([]IndexEntry, 0, 1000),
		bloom:      NewBloomFilter(4096, 3),
		scratchBuf: make([]byte, 1024), // Start with a 1KB buffer
	}, nil
}

func (b *SSTableBuilder) Add(internalKey []byte, value []byte) error {
	userKey, _, _ := ParseInternalKey(internalKey)

	// Update Min/Max using UserKey only
	if b.minKey == nil || bytes.Compare(userKey, b.minKey) < 0 {
		b.minKey = append([]byte(nil), userKey...)
	}
	if b.maxKey == nil || bytes.Compare(userKey, b.maxKey) > 0 {
		b.maxKey = append([]byte(nil), userKey...)
	}

	b.bloom.Add(string(userKey))

	if b.blockBytesWritten == 0 {
		b.index = append(b.index, IndexEntry{
			Key:    append([]byte(nil), userKey...),
			Offset: b.offset,
		})
	}

	// 1. Calculate record size
	recordSize := 4 + len(internalKey) + 4 + len(value)

	// 2. REUSE OR GROW the scratch buffer instead of allocating new memory
	if cap(b.scratchBuf) < recordSize {
		// If we hit a massive value, grow the buffer.
		// Go's slice growth will ensure this happens infrequently.
		b.scratchBuf = make([]byte, recordSize)
	}
	// Slice the buffer to the exact size needed
	buf := b.scratchBuf[:recordSize]

	// 3. Serialize directly into the scratch memory
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(internalKey)))
	offset := 4

	copy(buf[offset:], internalKey)
	offset += len(internalKey)

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(value)))
	offset += 4

	copy(buf[offset:], value)

	// 4. Single system call write
	_, err := b.file.Write(buf)
	if err != nil {
		return err
	}

	b.offset += uint32(recordSize)
	b.blockBytesWritten += uint32(recordSize)

	if b.blockBytesWritten >= blockSize {
		b.blockBytesWritten = 0
	}

	return nil
}

func (b *SSTableBuilder) Finish() error {
	// ... (Rest of the logic for Bloom, Index, and Footer remains as previously established)
	// These are called only once per SSTable, so the single-time allocations here are acceptable.

	bloomStartOffset := b.offset
	if _, err := b.file.Write(b.bloom.Bytes()); err != nil {
		return err
	}
	b.offset += uint32(len(b.bloom.Bytes()))

	indexStartOffset := b.offset

	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(b.index)))
	if _, err := b.file.Write(countBuf); err != nil {
		return err
	}
	b.offset += 4

	for _, entry := range b.index {
		entrySize := 4 + len(entry.Key) + 4
		entryBuf := b.scratchBuf[:entrySize] // Reuse scratchBuf here too!

		binary.LittleEndian.PutUint32(entryBuf[0:4], uint32(len(entry.Key)))
		copy(entryBuf[4:], entry.Key)
		binary.LittleEndian.PutUint32(entryBuf[4+len(entry.Key):], entry.Offset)

		if _, err := b.file.Write(entryBuf); err != nil {
			return err
		}
		b.offset += uint32(entrySize)
	}

	metaStartOffset := b.offset
	metaSize := 4 + len(b.minKey) + 4 + len(b.maxKey)
	metaBuf := b.scratchBuf[:metaSize] // Reuse scratchBuf here too!

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
	binary.LittleEndian.PutUint32(footerBuf[0:4], bloomStartOffset)
	binary.LittleEndian.PutUint32(footerBuf[4:8], indexStartOffset)
	binary.LittleEndian.PutUint32(footerBuf[8:12], metaStartOffset)
	binary.LittleEndian.PutUint32(footerBuf[12:16], 0xABCD1234)

	if _, err := b.file.Write(footerBuf); err != nil {
		return err
	}

	b.file.Sync()
	return b.file.Close()
}
