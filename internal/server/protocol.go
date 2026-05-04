package server

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	// Operation Codes
	OpGet uint8 = 0x01
	OpPut uint8 = 0x02
	OpDel uint8 = 0x03

	// HeaderSize is 5 bytes: 4 bytes for total payload length, 1 byte for the operation code
	HeaderSize = 5
)

// Request represents a parsed network command from the client
type Request struct {
	Op    uint8
	Key   []byte
	Value []byte // Value will be empty for GET and DEL operations
}

var (
	ErrInvalidPayload  = errors.New("invalid payload structure")
	ErrPayloadTooLarge = errors.New("payload exceeds maximum allowed size")
)

// EncodeRequest serializes a Request struct into raw bytes and writes it to the TCP stream.
// Wire Format: [Payload Length (4 bytes)] [OpCode (1 byte)] [Key Length (2 bytes)] [Key] [Value]
func EncodeRequest(w io.Writer, req *Request) error {
	// Calculate the exact size of the dynamic payload
	// 2 bytes for the Key Length + length of Key + length of Value
	payloadLen := 2 + len(req.Key) + len(req.Value)

	// 1. Write the 5-byte Header
	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(payloadLen))
	header[4] = req.Op

	if _, err := w.Write(header); err != nil {
		return err
	}

	// 2. Write the 2-byte Key Length
	keyLenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(keyLenBytes, uint16(len(req.Key)))
	if _, err := w.Write(keyLenBytes); err != nil {
		return err
	}

	// 3. Write the actual Key
	if _, err := w.Write(req.Key); err != nil {
		return err
	}

	// 4. Write the Value (if it exists)
	if len(req.Value) > 0 {
		if _, err := w.Write(req.Value); err != nil {
			return err
		}
	}

	return nil
}

// DecodeRequest reads exactly one complete command from the TCP stream and parses it into a Request struct.
func DecodeRequest(r io.Reader) (*Request, error) {
	// 1. Read the 5-byte Header exactly
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	payloadLen := binary.BigEndian.Uint32(header[0:4])
	op := header[4]

	// Sanity check to prevent out-of-memory crashes from malicious or corrupted length headers
	// Limiting max request size to 10MB for this implementation
	if payloadLen > 10*1024*1024 {
		return nil, ErrPayloadTooLarge
	}

	// 2. Read the exact length of the payload specified by the header
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	// 3. Parse the payload
	if payloadLen < 2 {
		return nil, ErrInvalidPayload
	}

	keyLen := binary.BigEndian.Uint16(payload[0:2])

	// Ensure the key length doesn't logically exceed the total payload we just read
	if uint32(2+keyLen) > payloadLen {
		return nil, ErrInvalidPayload
	}

	// Slicing the byte array to extract Key and Value without copying memory
	key := payload[2 : 2+keyLen]
	value := payload[2+keyLen:]

	return &Request{
		Op:    op,
		Key:   key,
		Value: value,
	}, nil
}
