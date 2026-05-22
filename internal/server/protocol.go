package server

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	OpGet uint8 = 0x01
	OpPut uint8 = 0x02
	OpDel uint8 = 0x03

	HeaderSize = 5
)

type Request struct {
	Op    uint8
	Key   []byte
	Value []byte
}

var (
	ErrInvalidPayload  = errors.New("invalid payload structure")
	ErrPayloadTooLarge = errors.New("payload exceeds maximum allowed size")
)

func EncodeRequest(w io.Writer, req *Request) error {
	payloadLen := 2 + len(req.Key) + len(req.Value)

	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:4], uint32(payloadLen))
	header[4] = req.Op

	if _, err := w.Write(header); err != nil {
		return err
	}

	keyLenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(keyLenBytes, uint16(len(req.Key)))
	if _, err := w.Write(keyLenBytes); err != nil {
		return err
	}

	if _, err := w.Write(req.Key); err != nil {
		return err
	}

	if len(req.Value) > 0 {
		if _, err := w.Write(req.Value); err != nil {
			return err
		}
	}

	return nil
}

func DecodeRequest(r io.Reader) (*Request, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	payloadLen := binary.BigEndian.Uint32(header[0:4])
	op := header[4]

	if payloadLen > 10*1024*1024 {
		return nil, ErrPayloadTooLarge
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	if payloadLen < 2 {
		return nil, ErrInvalidPayload
	}

	keyLen := binary.BigEndian.Uint16(payload[0:2])

	if uint32(2+keyLen) > payloadLen {
		return nil, ErrInvalidPayload
	}

	key := payload[2 : 2+keyLen]
	value := payload[2+keyLen:]

	return &Request{
		Op:    op,
		Key:   key,
		Value: value,
	}, nil
}
