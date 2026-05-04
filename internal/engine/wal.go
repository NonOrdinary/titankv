/*
*
* WAL : Write Ahead Log File
Please Note : write now we are directly touching the disk, we would optimise it later
* -Ensures Durability(D) of ACID
* -Can withstand any kind of failure, system or server
* -Success of a transaction to user will only be shown when the wal has been flushed to DISK(permanent) -fsync() syscall
* -This is an append only file, thus enabling almost sequential writes (we cannot actually ensure that it would be sequential)
* -There is a file descriptor at end, new data is added to offsets, which are visible immediately(this is what sequential means here)
* -On failure, complete file isn't read, rather only upto last checkpoint, as on reaching checkpoint, the file is synced to
* disk, basically on failure, start from the previos checkpoint
* -Checksum : To ensure correct writing of data and no data loss (either by error or by power outage during write)using the checksum
* sync pool : very essential, otherwise the Go garbage collector would bottleneck while clearing the space allocated to
* serialised data of key,so sync pool are auto released and actually overwritten once given back to pool buffer.
*/
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

// WAL represents the Write-Ahead Log.
type WAL struct {
	file *os.File
	mu   sync.Mutex
	pool sync.Pool
}

func NewWAL(path string) (*WAL, error) {
	// FIX 1: Must be O_RDWR so we can actually read it during recovery
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

	// Reset to start
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	var validOffset int64 = 0
	var maxSeqNum uint64 = 0

	// Use a LimitedReader to ensure bufio doesn't read past what's actually there
	// and keep the file locked.
	err := func() error {
		// On Windows, bufio can be aggressive. For recovery,
		// let's use a smaller buffer or direct reads if necessary.
		reader := bufio.NewReader(w.file)
		headerBuf := make([]byte, 4)
		lenBuf := make([]byte, 4)
		scratchBuf := make([]byte, 4096)

		for {
			if _, err := io.ReadFull(reader, headerBuf); err != nil {
				return nil // EOF
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

			// Checksum...
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

	// CRITICAL: Now that we are done reading, we must ensure the file
	// is ready for truncation.
	if err := w.file.Truncate(validOffset); err != nil {
		return 0, fmt.Errorf("windows-access-denied-fix: %w", err)
	}

	// Because we removed O_APPEND, we MUST manually seek to the end
	// so the next WriteRecord doesn't overwrite our recovered data.
	_, err = w.file.Seek(validOffset, io.SeekStart)
	return maxSeqNum, err
}
