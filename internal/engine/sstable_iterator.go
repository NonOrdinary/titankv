package engine

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type IteratorKV struct {
	InternalKey []byte
	Value       []byte
}

type SSTableIterator struct {
	file   *os.File
	reader *bufio.Reader // FIX 3: Buffered reading to minimize syscalls
	endPos uint32
	curr   uint32 // FIX 1: Local counter to eliminate Seek tax

	lenBuf []byte // Pre-allocated 4-byte buffer for lengths
}

func NewSSTableIterator(path string, endPos uint32) (*SSTableIterator, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &SSTableIterator{
		file:   f,
		reader: bufio.NewReaderSize(f, 64*1024), // 64KB buffer for smooth streaming
		endPos: endPos,
		curr:   0,
		lenBuf: make([]byte, 4),
	}, nil
}

func (it *SSTableIterator) Next() (IteratorKV, error) {
	// Check against our local counter instead of Seek()
	if it.curr >= it.endPos {
		return IteratorKV{}, io.EOF
	}

	// 1. Read InternalKey Length
	if _, err := io.ReadFull(it.reader, it.lenBuf); err != nil {
		return IteratorKV{}, err
	}
	ikLen := binary.LittleEndian.Uint32(it.lenBuf)
	it.curr += 4

	// 2. FIX 2: Zero-allocation direct read.
	// We allocate the final destination once and read directly into it.
	finalIK := make([]byte, ikLen)
	if _, err := io.ReadFull(it.reader, finalIK); err != nil {
		return IteratorKV{}, fmt.Errorf("failed to read InternalKey: %w", err)
	}
	it.curr += ikLen

	// 3. Read Value Length
	if _, err := io.ReadFull(it.reader, it.lenBuf); err != nil {
		return IteratorKV{}, err
	}
	valLen := binary.LittleEndian.Uint32(it.lenBuf)
	it.curr += 4

	// 4. Read Value
	var finalVal []byte
	if valLen > 0 {
		finalVal = make([]byte, valLen)
		if _, err := io.ReadFull(it.reader, finalVal); err != nil {
			return IteratorKV{}, fmt.Errorf("failed to read Value: %w", err)
		}
		it.curr += valLen
	}

	return IteratorKV{InternalKey: finalIK, Value: finalVal}, nil
}

func (it *SSTableIterator) Close() error {
	return it.file.Close()
}
