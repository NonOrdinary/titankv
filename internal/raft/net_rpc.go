package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
)

// NetworkTransport manages the dedicated TCP listener for internal cluster communication.
// IMPORTANT: This port MUST be different from the Phase 4 Client TCP port.
type NetworkTransport struct {
	node     *Node
	listener net.Listener
	server   *rpc.Server
	port     string
}

// NewNetworkTransport sets up the RPC listener for node-to-node communication.
func NewNetworkTransport(node *Node, port string) *NetworkTransport {
	return &NetworkTransport{
		node:   node,
		server: rpc.NewServer(),
		port:   port,
	}
}

// Start binds the internal RPC server to the network port.
func (t *NetworkTransport) Start() error {
	// Register the Node itself so its RequestVote and AppendEntries methods
	// (written in election.go and replication.go) are exposed via RPC.
	if err := t.server.RegisterName("RaftNode", t.node); err != nil {
		return fmt.Errorf("failed to register Raft RPCs: %w", err)
	}

	var err error
	t.listener, err = net.Listen("tcp", t.port)
	if err != nil {
		return fmt.Errorf("failed to bind Raft internal port %s: %w", t.port, err)
	}

	log.Printf("Raft Node [%s] Internal RPC listening on %s", t.node.id, t.port)

	// Accept incoming internal network connections
	go func() {
		for {
			conn, err := t.listener.Accept()
			if err != nil {
				select {
				case <-t.node.quit:
					return // Graceful shutdown triggered
				default:
					log.Printf("Raft RPC Accept error: %v", err)
					continue
				}
			}
			// Let Go's standard library handle the struct serialization
			go t.server.ServeConn(conn)
		}
	}()

	return nil
}

// Stop gracefully closes the internal listener.
func (t *NetworkTransport) Stop() {
	if t.listener != nil {
		t.listener.Close()
	}
}

// ---------------------------------------------------------
// RPC Client Callers (Replacing the Phase 5 Placeholders)
// ---------------------------------------------------------

// sendRequestVote executes the network call to campaign for a vote.
func (n *Node) sendRequestVote(peerAddr string, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// Strict Timeout: If a peer is dead, we cannot wait forever, otherwise
	// the election Goroutine hangs and the cluster deadlocks.
	conn, err := net.DialTimeout("tcp", peerAddr, 500*time.Millisecond)
	if err != nil {
		return false // Network timeout or connection refused
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	defer client.Close()

	// Synchronous call. The goroutine in election.go blocks until this returns.
	err = client.Call("RaftNode.RequestVote", args, reply)
	if err != nil {
		log.Printf("RPC RequestVote to [%s] failed: %v", peerAddr, err)
		return false
	}

	return true
}

// sendAppendEntries executes the network call for log replication and heartbeats.
func (n *Node) sendAppendEntries(peerAddr string, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// Heartbeats are fired constantly. A fast 500ms timeout prevents backpressure.
	conn, err := net.DialTimeout("tcp", peerAddr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	defer client.Close()

	err = client.Call("RaftNode.AppendEntries", args, reply)
	if err != nil {
		// Silent fail is intended here. If a Follower is down, heartbeats will fail.
		// We don't want to spam the Leader's terminal with thousands of timeout logs.
		return false
	}

	return true
}
