package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

type SSTableReader struct {
	file             *os.File
	Path             string
	index            []IndexEntry
	indexStartOffset uint32
	bloomStartOffset uint32
	bloom            *BloomFilter
	MinKey           string
	MaxKey           string
}

func NewSSTableReader(path string) (*SSTableReader, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	// 1. Read Footer
	if _, err := f.Seek(-16, io.SeekEnd); err != nil {
		return nil, err
	}
	footerBuf := make([]byte, 16)
	if _, err := io.ReadFull(f, footerBuf); err != nil {
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	bloomStartOffset := binary.LittleEndian.Uint32(footerBuf[0:4])
	indexStartOffset := binary.LittleEndian.Uint32(footerBuf[4:8])
	metaStartOffset := binary.LittleEndian.Uint32(footerBuf[8:12])
	magicNumber := binary.LittleEndian.Uint32(footerBuf[12:16])

	if magicNumber != 0xABCD1234 {
		f.Close()
		return nil, fmt.Errorf("invalid magic number: %x", magicNumber)
	}

	// 2. Load Bloom Filter
	bloomSize := indexStartOffset - bloomStartOffset
	bloomBytes := make([]byte, bloomSize)
	if _, err := f.Seek(int64(bloomStartOffset), io.SeekStart); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(f, bloomBytes); err != nil {
		return nil, fmt.Errorf("failed to read bloom filter: %w", err)
	}

	// 3. Load Sparse Index
	if _, err := f.Seek(int64(indexStartOffset), io.SeekStart); err != nil {
		return nil, err
	}
	countBuf := make([]byte, 4)
	if _, err := io.ReadFull(f, countBuf); err != nil {
		return nil, err
	}
	numEntries := binary.LittleEndian.Uint32(countBuf)

	index := make([]IndexEntry, numEntries)
	for i := 0; i < int(numEntries); i++ { // FIX 1: Corrected increment
		if _, err := io.ReadFull(f, countBuf); err != nil {
			return nil, err
		}
		keyLen := binary.LittleEndian.Uint32(countBuf)
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(f, keyBuf); err != nil { // FIX 4: Error check
			return nil, err
		}
		if _, err := io.ReadFull(f, countBuf); err != nil {
			return nil, err
		}
		offset := binary.LittleEndian.Uint32(countBuf)

		index[i] = IndexEntry{Key: keyBuf, Offset: offset}
	}

	// 4. Load Metadata
	if _, err := f.Seek(int64(metaStartOffset), io.SeekStart); err != nil {
		return nil, err
	}
	lenBuf := make([]byte, 4)

	if _, err := io.ReadFull(f, lenBuf); err != nil {
		return nil, err
	}
	minLen := binary.LittleEndian.Uint32(lenBuf)
	minBuf := make([]byte, minLen)
	if _, err := io.ReadFull(f, minBuf); err != nil {
		return nil, err
	}

	if _, err := io.ReadFull(f, lenBuf); err != nil {
		return nil, err
	}
	maxLen := binary.LittleEndian.Uint32(lenBuf)
	maxBuf := make([]byte, maxLen)
	if _, err := io.ReadFull(f, maxBuf); err != nil {
		return nil, err
	}

	return &SSTableReader{
		file:             f,
		index:            index,
		Path:             path,
		indexStartOffset: indexStartOffset,
		bloomStartOffset: bloomStartOffset,
		bloom:            LoadBloomFilter(bloomBytes, 3),
		MinKey:           string(minBuf),
		MaxKey:           string(maxBuf),
	}, nil
}

func (r *SSTableReader) Get(userKey []byte, targetSeqNum uint64) ([]byte, bool, bool, error) {
	if !r.bloom.MightContain(string(userKey)) {
		return nil, false, false, nil
	}

	if len(r.index) == 0 {
		return nil, false, false, nil
	}

	// 2. Binary Search
	idx := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].Key, userKey) >= 0
	})

	// FIX 2: Correct index navigation logic
	if idx > 0 && (idx == len(r.index) || bytes.Compare(r.index[idx].Key, userKey) > 0) {
		idx--
	}
	// If idx is 0, we check the first block. If idx > 0, we check the block starting at idx.

	blockStart := r.index[idx].Offset
	var blockEnd uint32
	if idx+1 < len(r.index) {
		blockEnd = r.index[idx+1].Offset
	} else {
		blockEnd = r.bloomStartOffset
	}

	bytesToRead := int(blockEnd - blockStart)
	blockBuf := make([]byte, bytesToRead)
	if _, err := r.file.ReadAt(blockBuf, int64(blockStart)); err != nil {
		return nil, false, false, err
	}

	// 3. Scanning with FIX 3: Correct Offset Advancement
	currentOffset := 0
	for currentOffset < bytesToRead {
		intKeyLen := int(binary.LittleEndian.Uint32(blockBuf[currentOffset : currentOffset+4]))
		currentOffset += 4

		internalKey := blockBuf[currentOffset : currentOffset+intKeyLen]
		currentOffset += intKeyLen

		valLen := int(binary.LittleEndian.Uint32(blockBuf[currentOffset : currentOffset+4]))
		currentOffset += 4

		// Capture the value slice before potentially skipping or returning
		valBytes := blockBuf[currentOffset : currentOffset+valLen]

		currUserKey, currSeqNum, currType := ParseInternalKey(internalKey)
		cmp := bytes.Compare(currUserKey, userKey)

		if cmp == 0 && currSeqNum <= targetSeqNum {
			if currType == TypeDelete {
				return nil, true, true, nil
			}
			return append([]byte(nil), valBytes...), false, true, nil
		}

		// ALWAYS advance the offset by valLen to keep the loop synchronized
		currentOffset += valLen

		if cmp > 0 {
			break
		}
	}

	return nil, false, false, nil
}
