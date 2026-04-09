package engine

import (
	"encoding/binary"
	"io"
	"os"
)

type KVPair struct {
	Key         string
	Value       []byte
	IsTombstone bool
}

// SSTableIterator streams an SSTable sequentially using its own file descriptor.
type SSTableIterator struct {
	file          *os.File
	currentOffset int64
	endOffset     int64

	tombstoneBuf []byte
	lenBuf       []byte
	keyBuf       []byte // Pre-allocated buffer
	valBuf       []byte // Pre-allocated buffer
}

func NewSSTableIterator(path string, bloomStartOffset uint32) (*SSTableIterator, error) {
	// Independent file descriptor prevents data races with live Get() queries
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &SSTableIterator{
		file:          f,
		currentOffset: 0,
		endOffset:     int64(bloomStartOffset),
		tombstoneBuf:  make([]byte, 1),
		lenBuf:        make([]byte, 4),
		keyBuf:        make([]byte, 0, 256),
		valBuf:        make([]byte, 0, 1024),
	}, nil
}

func (it *SSTableIterator) Next() (*KVPair, error) {
	if it.currentOffset >= it.endOffset {
		return nil, io.EOF
	}

	if _, err := io.ReadFull(it.file, it.tombstoneBuf); err != nil {
		return nil, err
	}
	isTombstone := it.tombstoneBuf[0] == 1
	it.currentOffset++

	if _, err := io.ReadFull(it.file, it.lenBuf); err != nil {
		return nil, err
	}
	keyLen := binary.LittleEndian.Uint32(it.lenBuf)
	it.currentOffset += 4

	// Safely grow or reuse the Key buffer
	if cap(it.keyBuf) < int(keyLen) {
		it.keyBuf = make([]byte, keyLen)
	}
	it.keyBuf = it.keyBuf[:keyLen]
	if _, err := io.ReadFull(it.file, it.keyBuf); err != nil {
		return nil, err
	}
	it.currentOffset += int64(keyLen)

	if _, err := io.ReadFull(it.file, it.lenBuf); err != nil {
		return nil, err
	}
	valLen := binary.LittleEndian.Uint32(it.lenBuf)
	it.currentOffset += 4

	// Safely grow or reuse the Value buffer
	if cap(it.valBuf) < int(valLen) {
		it.valBuf = make([]byte, valLen)
	}
	it.valBuf = it.valBuf[:valLen]
	if _, err := io.ReadFull(it.file, it.valBuf); err != nil {
		return nil, err
	}
	it.currentOffset += int64(valLen)

	return &KVPair{
		Key:         string(it.keyBuf),
		Value:       it.valBuf,
		IsTombstone: isTombstone,
	}, nil
}

func (it *SSTableIterator) Close() {
	it.file.Close()
}
