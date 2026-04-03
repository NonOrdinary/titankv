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

	// pool holds reusable byte slices
	// *** we used this to ensure when system is scaled, the allocation of array could have bottleneck the performance
	// because Garbage collector would go crazy after 50000 writes to just clean up
	pool sync.Pool
}

// NewWAL opens an existing WAL file or creates a new one.
// Gemini Helped on this one a lot
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

	// 2. Grab a pointer of reusable buffer from the pool : slice of bytes
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

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(key))) // converting int(key) to LittleEndian byte format because CPU refuse to accept int ,it accepts bits,bytes,slice of bytes
	// Why only LittleEndian : We can practically do big Endian too, but it's industry standard as i read in blogs, so
	// that's why, otherwise it's totally upto programmers choice, also littleEndian is faster as LSB is in the lowest address
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

	// Lock the file and write sequentially(append the data)
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

// Recover reads the WAL from disk and reconstructs the MemTable efficiently.
func (w *WAL) Recover(mt *MemTable) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	//moving the pointer to 0th byte, to read the WAL
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	// Intially we used 6 syscalls , which consumes a lot of CPU cycles
	// also, we were dooming the garbage collector, this time we:
	// FIX 1: bufio.Reader. This grabs 32KB of data from the hard drive
	// in a single syscall and stores it in RAM. All our io.ReadFull calls
	// will now read from the Main Memory.
	reader := bufio.NewReaderSize(w.file, 32*1024)

	// FIX 2: Pre-allocate static buffers OUTSIDE the loop.
	// We reuse these exact same memory addresses for every single record.
	headerBuf := make([]byte, 4)
	tombstoneBuf := make([]byte, 1)
	lenBuf := make([]byte, 4)

	// scratchBuf is a dynamic buffer we will resize if a key/value is larger than 4KB
	scratchBuf := make([]byte, 4096)

	for {
		// Read Checksum - since headerBuf size is 4 byte, thus it will only pull 4 bytes from the space of 32KB
		if _, err := io.ReadFull(reader, headerBuf); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		expectedChecksum := binary.LittleEndian.Uint32(headerBuf)

		// Read Tombstone
		if _, err := io.ReadFull(reader, tombstoneBuf); err != nil {
			return err
		}
		isTombstone := tombstoneBuf[0] == 1

		// Read Key Length
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			return err
		}
		keyLen := binary.LittleEndian.Uint32(lenBuf) // Converting bytes stored to integer for lengths, as they are required

		// Read Key (Safely growing scratchBuf if the key is massive)
		if cap(scratchBuf) < int(keyLen) {
			scratchBuf = make([]byte, keyLen)
		}
		keyBytes := scratchBuf[:keyLen]
		if _, err := io.ReadFull(reader, keyBytes); err != nil {
			return err
		}
		// We convert it to a string now so it copies the underlying bytes.
		// If we didn't, reading the value into scratchBuf would overwrite our key!
		keyStr := string(keyBytes)

		// Read Value Length
		if _, err := io.ReadFull(reader, lenBuf); err != nil {
			return err
		}
		valLen := binary.LittleEndian.Uint32(lenBuf) // this is nothing more then reading bytes and converting to int from MM

		// Read Value
		if cap(scratchBuf) < int(valLen) {
			scratchBuf = make([]byte, valLen)
		}
		valBytes := scratchBuf[:valLen]
		if _, err := io.ReadFull(reader, valBytes); err != nil {
			return err
		}

		// We MUST allocate a new byte slice for the value here.
		// The MemTable will hold onto this reference forever. If we just gave it
		// valBytes, the next loop iteration would overwrite the data in the database!
		finalVal := make([]byte, valLen)
		copy(finalVal, valBytes)

		// Verify Checksum (Zero-Allocation Streaming Hashing)
		hasher := crc32.NewIEEE()
		hasher.Write(tombstoneBuf)

		binary.LittleEndian.PutUint32(lenBuf, keyLen) // reverse, int -> slice of bytes (storing in MM now,which is 0/1)
		hasher.Write(lenBuf)
		hasher.Write([]byte(keyStr))

		binary.LittleEndian.PutUint32(lenBuf, valLen)
		hasher.Write(lenBuf)
		hasher.Write(finalVal)

		if hasher.Sum32() != expectedChecksum {
			return errors.New("WAL corruption detected: torn write : Corrupted during intial write to Log File")
		}

		// Insert into MemTable
		if isTombstone {
			mt.Delete(keyStr)
		} else {
			mt.Put(keyStr, finalVal)
		}
	}

	// Move the actual OS file pointer back to the end so future Puts append correctly
	_, err := w.file.Seek(0, 2)
	return err
}
