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
	"errors"
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
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0644)
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

// WriteRecord appends an InternalKey and Value to the log.
// The Tombstone and SeqNum are already baked into the internalKey.
func (w *WAL) WriteRecord(internalKey []byte, value []byte) error {

	// Payload: 4 (InternalKeyLen) + len(internalKey) + 4 (ValueLen) + len(value)
	payloadSize := 4 + len(internalKey) + 4 + len(value)
	totalSize := 4 + payloadSize // Total includes 4 bytes for CRC32 Checksum

	bufPtr := w.pool.Get().(*[]byte)
	buf := *bufPtr
	buf = buf[:0]

	if cap(buf) < totalSize {
		buf = make([]byte, totalSize)
	} else {
		buf = buf[:totalSize]
	}

	offset := 4 // Skip the first 4 bytes for the checksum

	// Write InternalKey Length
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(internalKey)))
	offset += 4

	// Write InternalKey
	copy(buf[offset:], internalKey)
	offset += len(internalKey)

	// Write Value Length
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(value)))
	offset += 4

	// Write Value
	copy(buf[offset:], value)

	// Calculate CRC32 of the payload
	checksum := crc32.ChecksumIEEE(buf[4:totalSize])
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	// Write to disk
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

func (w *WAL) Recover(mt *MemTable) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	reader := bufio.NewReaderSize(w.file, 32*1024)

	headerBuf := make([]byte, 4)
	lenBuf := make([]byte, 4)
	scratchBuf := make([]byte, 4096)

	for {
		// 1. Read Checksum
		if _, err := io.ReadFull(reader, headerBuf); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		expectedChecksum := binary.LittleEndian.Uint32(headerBuf)

		// 2. Read InternalKey Length
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			return err
		}
		keyLen := binary.LittleEndian.Uint32(lenBuf)

		// 3. Read InternalKey
		if cap(scratchBuf) < int(keyLen) {
			scratchBuf = make([]byte, keyLen)
		}
		keyBytes := scratchBuf[:keyLen]
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return err
		}

		// MUST allocate a new slice for the InternalKey because the MemTable will hold it forever
		finalInternalKey := make([]byte, keyLen)
		copy(finalInternalKey, keyBytes)

		// 4. Read Value Length
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			return err
		}
		valLen := binary.LittleEndian.Uint32(lenBuf)

		// 5. Read Value
		var finalVal []byte
		if valLen > 0 {
			if cap(scratchBuf) < int(valLen) {
				scratchBuf = make([]byte, valLen)
			}
			valBytes := scratchBuf[:valLen]
			if _, err := io.ReadFull(reader, valBytes); err != nil {
				return err
			}
			finalVal = make([]byte, valLen)
			copy(finalVal, valBytes)
		}

		// 6. Verify Checksum
		hasher := crc32.NewIEEE()
		binary.LittleEndian.PutUint32(lenBuf, keyLen)
		hasher.Write(lenBuf)
		hasher.Write(finalInternalKey)

		binary.LittleEndian.PutUint32(lenBuf, valLen)
		hasher.Write(lenBuf)
		hasher.Write(finalVal)

		if hasher.Sum32() != expectedChecksum {
			return errors.New("WAL corruption detected: torn write")
		}

		// 7. Parse the InternalKey and restore it to the MemTable
		userKey, seqNum, keyType := ParseInternalKey(finalInternalKey)

		if keyType == TypeDelete {
			mt.Delete(userKey, seqNum)
		} else {
			mt.Put(userKey, finalVal, seqNum)
		}
	}

	_, err := w.file.Seek(0, 2)
	return err
}
