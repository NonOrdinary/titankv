/**
 * WAL : Write Ahead Log File
 * -Ensures Durability(D) of ACID
 * -Can withstand any kind of failure, system or server
 * -Success of a transaction to user will only be shown when the wal has been flushed to DISK(permanent) -fsync() syscall
 * -This is an append only file, thus enabling almost sequential writes (we cannot actually ensure that it would be sequential)
 * -There is a file descriptor at end, new data is added to offsets, which are visible immediately(this is what sequential means here)
 * -On failure, complete file isn't read, rather only upto last checkpoint, as on reaching checkpoint, the file is synced to
 * disk, basically on failure, start from the previos checkpoint
 * -Checksum : To ensure correct writing of data and no data loss (either by error or by power outage during write)using the checksum
 */
package engine

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
)

// WAL represents the Write-Ahead Log.
type WAL struct {
	file *os.File
	mu   sync.Mutex

	// pool holds reusable byte slices
	// *** we used this to ensure when system is scaled, the allocation of array could have bottleneck the performance
	// because Garbage collector would go crazy after 50000 writes to just clean up
	pool sync.Pool
}

// NewWAL opens an existing WAL file or creates a new one.
func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY|os.O_SYNC, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: f,
		pool: sync.Pool{
			New: func() interface{} {
				// Pre-allocate a 4KB buffer (default), we can grow this later if required
				b := make([]byte, 0, 4096)
				return &b // We store a pointer to the slice in the pool
			},
		},
	}, nil
}

func (w *WAL) WriteRecord(key string, value []byte, isTombstone bool) error {

	// Payload: 1 (Tombstone) + 4 (KeyLen) + len(key) + 4 (ValueLen) + len(value)
	payloadSize := 1 + 4 + len(key) + 4 + len(value)
	// Total: 4 (CRC32 Checksum) + Payload
	totalSize := 4 + payloadSize

	// 2. Grab a reusable buffer from the pool
	bufPtr := w.pool.Get().(*[]byte)
	buf := *bufPtr

	// Fast-path: Reset length to 0 but keep capacity
	buf = buf[:0]

	//Ensure the buffer is large enough for this specific record
	if cap(buf) < totalSize {
		// If the value is massive, we must allocate a larger buffer just this once
		buf = make([]byte, totalSize)
	} else {
		buf = buf[:totalSize]
	}

	// Build the payload (skipping the first 4 bytes reserved for the Checksum)
	offset := 4 // this is the index of position at which out next byte will be placed at
	if isTombstone {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset++

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(key)))
	offset += 4

	copy(buf[offset:], key)
	offset += len(key)

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(value)))
	offset += 4

	copy(buf[offset:], value)

	// Calculate the CRC32 Checksum of the PAYLOAD (everything after byte 4)
	checksum := crc32.ChecksumIEEE(buf[4:totalSize])

	// Place the checksum, on it's reserved space ofcourse
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	// Lock the file and write sequentially
	w.mu.Lock()
	_, err := w.file.Write(buf)
	w.mu.Unlock() // Unlock immediately after disk I/O

	//Return the buffer to the pool for the next goroutine to use
	*bufPtr = buf
	w.pool.Put(bufPtr)

	return err
}

// Close safely shuts down the file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
