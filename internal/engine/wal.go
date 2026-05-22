package engine

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type WAL struct {
	file *os.File
	mu   sync.Mutex
	pool sync.Pool
}

func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: f,
		pool: sync.Pool{
			New: func() interface{} {
				b := make([]byte, 0, 4096)
				return &b
			},
		},
	}, nil
}

func (w *WAL) WriteRecord(internalKey []byte, value []byte) error {
	payloadSize := 4 + len(internalKey) + 4 + len(value)
	totalSize := 4 + payloadSize

	bufPtr := w.pool.Get().(*[]byte)
	buf := *bufPtr
	buf = buf[:0]

	if cap(buf) < totalSize {
		buf = make([]byte, totalSize)
	} else {
		buf = buf[:totalSize]
	}

	offset := 4

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(internalKey)))
	offset += 4

	copy(buf[offset:], internalKey)
	offset += len(internalKey)

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(value)))
	offset += 4

	copy(buf[offset:], value)

	checksum := crc32.ChecksumIEEE(buf[4:totalSize])
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	w.mu.Lock()
	_, err := w.file.Write(buf)
	w.mu.Unlock()

	*bufPtr = buf
	w.pool.Put(bufPtr)

	return err
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

func (w *WAL) Recover(mt *MemTable) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	var validOffset int64 = 0
	var maxSeqNum uint64 = 0

	err := func() error {
		reader := bufio.NewReader(w.file)
		headerBuf := make([]byte, 4)
		lenBuf := make([]byte, 4)
		scratchBuf := make([]byte, 4096)

		for {
			if _, err := io.ReadFull(reader, headerBuf); err != nil {
				return nil
			}
			expectedChecksum := binary.LittleEndian.Uint32(headerBuf)

			if _, err := io.ReadFull(reader, lenBuf); err != nil {
				break
			}
			keyLen := binary.LittleEndian.Uint32(lenBuf)

			if cap(scratchBuf) < int(keyLen) {
				scratchBuf = make([]byte, keyLen)
			}
			keyBytes := scratchBuf[:keyLen]
			if _, err := io.ReadFull(reader, keyBytes); err != nil {
				break
			}

			finalIK := make([]byte, keyLen)
			copy(finalIK, keyBytes)

			if _, err := io.ReadFull(reader, lenBuf); err != nil {
				break
			}
			valLen := binary.LittleEndian.Uint32(lenBuf)

			var finalVal []byte
			if valLen > 0 {
				if cap(scratchBuf) < int(valLen) {
					scratchBuf = make([]byte, valLen)
				}
				vBytes := scratchBuf[:valLen]
				if _, err := io.ReadFull(reader, vBytes); err != nil {
					break
				}
				finalVal = make([]byte, valLen)
				copy(finalVal, vBytes)
			}

			hasher := crc32.NewIEEE()
			binary.LittleEndian.PutUint32(lenBuf, keyLen)
			hasher.Write(lenBuf)
			hasher.Write(finalIK)
			binary.LittleEndian.PutUint32(lenBuf, valLen)
			hasher.Write(lenBuf)
			hasher.Write(finalVal)

			if hasher.Sum32() != expectedChecksum {
				break
			}

			uKey, seq, kType := ParseInternalKey(finalIK)
			if seq > maxSeqNum {
				maxSeqNum = seq
			}

			if kType == TypeDelete {
				mt.Delete(uKey, seq)
			} else {
				mt.Put(uKey, finalVal, seq)
			}

			validOffset += 4 + 4 + int64(keyLen) + 4 + int64(valLen)
		}
		return nil
	}()

	if err != nil {
		return 0, err
	}

	if err := w.file.Truncate(validOffset); err != nil {
		return 0, fmt.Errorf("windows-access-denied-fix: %w", err)
	}

	_, err = w.file.Seek(validOffset, io.SeekStart)
	return maxSeqNum, err
}
