package engine

import (
	"encoding/binary"
	"io"
	"os"
	"sort"
)

type SSTableReader struct {
	file             *os.File
	index            []IndexEntry
	indexStartOffset uint32
	bloomStartOffset uint32
	bloom            *BloomFilter
}

func NewSSTableReader(path string) (*SSTableReader, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(-16, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	footerBuf := make([]byte, 16)
	if _, err := io.ReadFull(f, footerBuf); err != nil {
		return nil, err
	}

	bloomStartOffset := binary.LittleEndian.Uint32(footerBuf[0:4])
	indexStartOffset := binary.LittleEndian.Uint32(footerBuf[4:8])
	//*** metaStartOffset := binary.LittleEndian.Uint32(footerBuf[8:12]) // Used for recovery/manifest building later
	magicNumber := binary.LittleEndian.Uint32(footerBuf[12:16])

	if magicNumber != 0xABCD1234 {
		f.Close()
		return nil, os.ErrInvalid
	}
	// Bloom filter loading to RAM, we want faster writes, so we need to take it to RAM
	bloomSize := indexStartOffset - bloomStartOffset
	bloomBytes := make([]byte, bloomSize)
	_, err = f.Seek(int64(bloomStartOffset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(f, bloomBytes); err != nil {
		return nil, err
	}

	// Loading the sparse index table to RAM
	_, err = f.Seek(int64(indexStartOffset), io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Get the number of entries inside the Sparse Index, we use countBuf as max number of entries possible
	countBuf := make([]byte, 4)
	// Check if there is some bad sector error in the Entries
	if _, err := io.ReadFull(f, countBuf); err != nil {
		return nil, err
	}
	// Count of entries, obviously convert little endian to int
	numEntries := binary.LittleEndian.Uint32(countBuf)

	// Create an array(obviously inside RAM) of IndexEntry, put all entries of sparse index to it
	index := make([]IndexEntry, numEntries)

	for i := 0; i < int(numEntries); i++ {
		// These error checks are necessary, there can be several errors like modified file by some superuser
		// midway, or file deleted etc ~ Protection Agreement Basically
		// read the KeyLength and store in countBuf as bits in little endian format
		if _, err := io.ReadFull(f, countBuf); err != nil {
			return nil, err
		}

		keyLen := binary.LittleEndian.Uint32(countBuf) // convert the LittleEndian format to int again

		keyBuf := make([]byte, keyLen) // read the content, basically Key
		if _, err := io.ReadFull(f, keyBuf); err != nil {
			return nil, err
		}
		// read the offset
		if _, err := io.ReadFull(f, countBuf); err != nil {
			return nil, err
		}
		offset := binary.LittleEndian.Uint32(countBuf)

		index[i] = IndexEntry{
			Key:    string(keyBuf),
			Offset: offset,
		}
	}
	// Obviously, gotta return the address of this sparse index
	return &SSTableReader{
		file:             f,
		index:            index,
		indexStartOffset: indexStartOffset,
		bloomStartOffset: bloomStartOffset,
		bloom:            LoadBloomFilter(bloomBytes, 3),
	}, nil
}

func (r *SSTableReader) Get(key string) ([]byte, bool, error) {
	// Checking the bloom filter
	if !r.bloom.MightContain(key) {
		return nil, false, nil
	}

	if len(r.index) == 0 {
		return nil, false, nil
	}
	// inbuilt binary search, also it's not sorting, the sparse index is already sorted on keys
	// It returns the
	idx := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].Key >= key
	})

	if idx < len(r.index) && r.index[idx].Key == key {
		// Exact match on the block's first key
	} else if idx > 0 {
		// one less, 5 >= 3 where (key = 4), so idx points to 5, 4 is inside block of 3, but not visible inside
		// our sparse index table
		idx--
	} else {
		return nil, false, nil
	}

	// Calculating the bounds of the block which we have to scan
	// it will be between current position offset and next index offset currOffset[.....]NextOffset
	blockStart := r.index[idx].Offset
	var blockEnd uint32
	if idx+1 < len(r.index) {
		blockEnd = r.index[idx+1].Offset
	} else {
		blockEnd = r.bloomStartOffset // the data block at the end ,ends at beginning of the bloom filter location on DISK

	}

	_, err := r.file.Seek(int64(blockStart), io.SeekStart)
	if err != nil {
		return nil, false, err
	}

	bytesToRead := int(blockEnd - blockStart) // these many bytes to read from the DISK

	blockBuf := make([]byte, bytesToRead)
	// Why single system call : Initially i tried to make 6 system calls to read data again, then repeat
	// the same thing again and again to reach each section (tombstone, keylen, key ,valuelen, value)
	// so instead , we can bring the whole block to RAM at once and touch disk only once
	//	This also solves the Garabage collector thrashing, as we will deallocate only once and not again and again

	// 1 SINGLE SYSTEM CALL: Pull the whole chunk into RAM at once.
	if _, err := io.ReadFull(r.file, blockBuf); err != nil {
		return nil, false, err
	}

	// In-memory parsing (Zero syscalls in this loop)
	currentOffset := 0
	for currentOffset < bytesToRead {
		isTombstone := blockBuf[currentOffset] == 1
		currentOffset++

		keyLen := binary.LittleEndian.Uint32(blockBuf[currentOffset : currentOffset+4])
		currentOffset += 4

		currentKey := string(blockBuf[currentOffset : currentOffset+int(keyLen)])
		currentOffset += int(keyLen)

		valLen := binary.LittleEndian.Uint32(blockBuf[currentOffset : currentOffset+4])
		currentOffset += 4

		// Did we find it?
		if currentKey == key {
			if isTombstone {
				return nil, true, nil
			}

			// MEMORY LEAK PREVENTION:(Suggested by Gemini, i was so naive)
			// We MUST copy the value into a new slice. If we just returned a slice
			// of blockBuf (e.g., blockBuf[start:end]), Go's Garbage Collector would
			// keep the entire 4KB block in memory forever just to preserve this tiny value!
			finalVal := make([]byte, valLen)
			copy(finalVal, blockBuf[currentOffset:currentOffset+int(valLen)])
			return finalVal, true, nil
		}

		currentOffset += int(valLen)

		if currentKey > key {
			break
		}
	}

	return nil, false, nil
}
