package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/NonOrdinary/titankv/internal/server"
)

// Router acts as a Layer 7 TCP Proxy with an integrated Connection Pool.
type Router struct {
	addr string
	ring *HashRing
	pool sync.Map // map[string]chan net.Conn
}

// NewRouter initializes the gateway.
func NewRouter(addr string, ring *HashRing) *Router {
	return &Router{
		addr: addr,
		ring: ring,
	}
}

// Start binds the router to a public-facing port.
func (r *Router) Start() error {
	listener, err := net.Listen("tcp", r.addr)
	if err != nil {
		return fmt.Errorf("router failed to bind: %w", err)
	}

	log.Printf("TitanKV Router (Gateway) listening on %s", r.addr)

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			log.Printf("Router accept error: %v", err)
			continue
		}

		go r.handleClient(clientConn)
	}
}

func (r *Router) handleClient(clientConn net.Conn) {
	defer clientConn.Close()
	remoteAddr := clientConn.RemoteAddr().String()

	for {
		clientConn.SetReadDeadline(time.Now().Add(5 * time.Minute))

		req, err := server.DecodeRequest(clientConn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			log.Printf("Router decode error from %s: %v", remoteAddr, err)
			return
		}

		targetNodeAddr := r.ring.GetNode(string(req.Key))
		if targetNodeAddr == "" {
			return
		}

		err = r.forwardRequest(clientConn, targetNodeAddr, req)
		if err != nil {
			log.Printf("Failed to forward request to %s: %v", targetNodeAddr, err)
		}
	}
}

// --- CONNECTION POOL LOGIC ---

func (r *Router) getConn(targetAddr string) (net.Conn, error) {
	// 1. Get or create a channel that holds up to 100 idle connections per shard
	poolInt, _ := r.pool.LoadOrStore(targetAddr, make(chan net.Conn, 100))
	pool := poolInt.(chan net.Conn)

	select {
	case conn := <-pool:
		// 2. We found a reusable connection!
		return conn, nil
	default:
		// 3. Pool is empty, open a new one
		return net.DialTimeout("tcp", targetAddr, 2*time.Second)
	}
}

func (r *Router) releaseConn(targetAddr string, conn net.Conn) {
	poolInt, ok := r.pool.Load(targetAddr)
	if !ok {
		conn.Close()
		return
	}
	pool := poolInt.(chan net.Conn)

	select {
	case pool <- conn:
		// 4. Put the connection back in the pool for the next request
	default:
		// 5. Pool is full, safe to drop and close
		conn.Close()
	}
}

// forwardRequest proxies data using the reusable connection pool.
func (r *Router) forwardRequest(clientConn net.Conn, targetAddr string, req *server.Request) error {
	// Get a pooled connection instead of dialing a fresh one
	targetConn, err := r.getConn(targetAddr)
	if err != nil {
		return fmt.Errorf("shard %s unreachable: %w", targetAddr, err)
	}

	// Ensure the connection goes back to the pool when this function finishes
	defer r.releaseConn(targetAddr, targetConn)

	if err := server.EncodeRequest(targetConn, req); err != nil {
		return err
	}

	targetConn.SetReadDeadline(time.Now().Add(5 * time.Second))

	header := make([]byte, 5)
	if _, err := io.ReadFull(targetConn, header); err != nil {
		return err
	}

	if _, err := clientConn.Write(header); err != nil {
		return err
	}

	payloadLen := binary.BigEndian.Uint32(header[0:4])
	if payloadLen > 1 {
		valLen := payloadLen - 1
		val := make([]byte, valLen)
		if _, err := io.ReadFull(targetConn, val); err != nil {
			return err
		}

		if _, err := clientConn.Write(val); err != nil {
			return err
		}
	}

	return nil
}
