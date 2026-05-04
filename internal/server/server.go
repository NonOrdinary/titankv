package server

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// KVStore defines the exact contract required from the storage engine.
// This perfectly mirrors the public methods of your DB struct in db.go.
type KVStore interface {
	Get(key string) ([]byte, bool, error)
	Put(key string, value []byte) error
	Delete(key string) error
}

// Response Status Codes
const (
	StatusOk       uint8 = 0x00
	StatusNotFound uint8 = 0x01
	StatusError    uint8 = 0x02
)

// Server represents our high-performance TCP multiplexer.
type Server struct {
	addr     string
	store    KVStore
	listener net.Listener

	// Concurrency controls for graceful shutdown
	quit chan struct{}
	wg   sync.WaitGroup
}

// NewServer initializes the network server with the provided storage engine.
func NewServer(addr string, store KVStore) *Server {
	return &Server{
		addr:  addr,
		store: store,
		quit:  make(chan struct{}),
	}
}

// Start binds to the TCP port and begins accepting connections.
func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	log.Printf("TitanKV TCP Server listening on %s", s.addr)

	// Accept loop runs in its own goroutine so Start() doesn't block
	go s.acceptLoop()

	return nil
}

// Stop initiates a graceful shutdown.
func (s *Server) Stop() {
	log.Println("Initiating graceful network shutdown...")
	close(s.quit)      // Signal the accept loop to stop
	s.listener.Close() // Break the blocking Accept() call
	s.wg.Wait()        // Wait for all active client goroutines to finish
	log.Println("Network server successfully stopped.")
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return // Graceful shutdown requested
			default:
				log.Printf("Failed to accept connection: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()

	for {
		// Prevent dead clients from holding connections open forever
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

		req, err := DecodeRequest(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return // Client disconnected cleanly
			}
			log.Printf("Connection error with %s: %v", remoteAddr, err)
			return
		}

		s.dispatch(conn, req)
	}
}

func (s *Server) dispatch(conn net.Conn, req *Request) {
	// Set a deadline for the server to reply
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	// Cast the byte slice to a string to match the db.go API
	keyStr := string(req.Key)

	switch req.Op {
	case OpGet:
		val, exists, err := s.store.Get(keyStr)
		if err != nil {
			writeResponse(conn, StatusError, []byte(err.Error()))
		} else if !exists {
			writeResponse(conn, StatusNotFound, nil)
		} else {
			writeResponse(conn, StatusOk, val)
		}

	case OpPut:
		err := s.store.Put(keyStr, req.Value)
		if err != nil {
			writeResponse(conn, StatusError, []byte(err.Error()))
		} else {
			writeResponse(conn, StatusOk, nil)
		}

	case OpDel:
		err := s.store.Delete(keyStr)
		if err != nil {
			writeResponse(conn, StatusError, []byte(err.Error()))
		} else {
			writeResponse(conn, StatusOk, nil)
		}

	default:
		writeResponse(conn, StatusError, []byte("unknown operation code"))
	}
}

// writeResponse serializes the server's reply back to the client.
func writeResponse(w io.Writer, status uint8, value []byte) error {
	payloadLen := 1 + len(value)

	header := make([]byte, 5)
	binary.BigEndian.PutUint32(header[0:4], uint32(payloadLen))
	header[4] = status

	if _, err := w.Write(header); err != nil {
		return err
	}

	if len(value) > 0 {
		if _, err := w.Write(value); err != nil {
			return err
		}
	}

	return nil
}
