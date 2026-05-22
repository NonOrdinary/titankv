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

type KVStore interface {
	Get(key string) ([]byte, bool, error)
	Put(key string, value []byte) error
	Delete(key string) error
}

const (
	StatusOk       uint8 = 0x00
	StatusNotFound uint8 = 0x01
	StatusError    uint8 = 0x02
)

type Server struct {
	addr     string
	store    KVStore
	listener net.Listener

	quit chan struct{}
	wg   sync.WaitGroup
}

func NewServer(addr string, store KVStore) *Server {
	return &Server{
		addr:  addr,
		store: store,
		quit:  make(chan struct{}),
	}
}

func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	log.Printf("TitanKV TCP Server listening on %s", s.addr)

	go s.acceptLoop()

	return nil
}

func (s *Server) Stop() {
	log.Println("Initiating graceful network shutdown...")
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
	log.Println("Network server successfully stopped.")
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return
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
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

		req, err := DecodeRequest(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			log.Printf("Connection error with %s: %v", remoteAddr, err)
			return
		}

		s.dispatch(conn, req)
	}
}

func (s *Server) dispatch(conn net.Conn, req *Request) {
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

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
