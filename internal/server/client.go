package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

var (
	ErrServer = errors.New("internal server error")
)

// Client provides a thread-safe SDK that perfectly mirrors the engine.DB interface.
type Client struct {
	addr string
	conn net.Conn

	// Enforces atomic network writes to prevent byte-interleaving on concurrent calls.
	mu sync.Mutex
}

// NewClient connects to the TitanKV server with a strict timeout.
func NewClient(addr string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to TitanKV at %s: %w", addr, err)
	}

	return &Client{
		addr: addr,
		conn: conn,
	}, nil
}

// Close gracefully terminates the TCP connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Get perfectly mirrors db.Get(key string) ([]byte, bool, error)
func (c *Client) Get(key string) ([]byte, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &Request{Op: OpGet, Key: []byte(key)}
	if err := EncodeRequest(c.conn, req); err != nil {
		return nil, false, fmt.Errorf("failed to send GET: %w", err)
	}

	status, value, err := c.readResponse()
	if err != nil {
		return nil, false, err
	}

	if status == StatusNotFound {
		return nil, false, nil // Successfully queried, but key doesn't exist
	}
	if status == StatusError {
		return nil, false, fmt.Errorf("%w: %s", ErrServer, string(value))
	}

	return value, true, nil
}

// Put mirrors db.Put(key string, value []byte) error
func (c *Client) Put(key string, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &Request{Op: OpPut, Key: []byte(key), Value: value}
	if err := EncodeRequest(c.conn, req); err != nil {
		return fmt.Errorf("failed to send PUT: %w", err)
	}

	status, respVal, err := c.readResponse()
	if err != nil {
		return err
	}

	if status == StatusError {
		return fmt.Errorf("%w: %s", ErrServer, string(respVal))
	}

	return nil
}

// Delete mirrors db.Delete(key string) error
func (c *Client) Delete(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	req := &Request{Op: OpDel, Key: []byte(key)}
	if err := EncodeRequest(c.conn, req); err != nil {
		return fmt.Errorf("failed to send DEL: %w", err)
	}

	status, respVal, err := c.readResponse()
	if err != nil {
		return err
	}

	if status == StatusError {
		return fmt.Errorf("%w: %s", ErrServer, string(respVal))
	}

	return nil
}

// readResponse decodes the server's reply.
func (c *Client) readResponse() (uint8, []byte, error) {
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer c.conn.SetReadDeadline(time.Time{})

	header := make([]byte, 5)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return 0, nil, fmt.Errorf("failed to read response header: %w", err)
	}

	payloadLen := binary.BigEndian.Uint32(header[0:4])
	status := header[4]

	if payloadLen < 1 {
		return 0, nil, errors.New("invalid response payload length")
	}

	valueLen := payloadLen - 1
	var value []byte

	if valueLen > 0 {
		value = make([]byte, valueLen)
		if _, err := io.ReadFull(c.conn, value); err != nil {
			return 0, nil, fmt.Errorf("failed to read response value: %w", err)
		}
	}

	return status, value, nil
}
