package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
)

type NetworkTransport struct {
	node     *Node
	listener net.Listener
	server   *rpc.Server
	port     string
}

func NewNetworkTransport(node *Node, port string) *NetworkTransport {
	return &NetworkTransport{
		node:   node,
		server: rpc.NewServer(),
		port:   port,
	}
}

func (t *NetworkTransport) Start() error {
	if err := t.server.RegisterName("RaftNode", t.node); err != nil {
		return fmt.Errorf("failed to register Raft RPCs: %w", err)
	}

	var err error
	t.listener, err = net.Listen("tcp", t.port)
	if err != nil {
		return fmt.Errorf("failed to bind Raft internal port %s: %w", t.port, err)
	}

	log.Printf("Raft Node [%s] Internal RPC listening on %s", t.node.id, t.port)

	go func() {
		for {
			conn, err := t.listener.Accept()
			if err != nil {
				select {
				case <-t.node.quit:
					return
				default:
					log.Printf("Raft RPC Accept error: %v", err)
					continue
				}
			}
			go t.server.ServeConn(conn)
		}
	}()

	return nil
}

func (t *NetworkTransport) Stop() {
	if t.listener != nil {
		t.listener.Close()
	}
}

func (n *Node) sendRequestVote(peerAddr string, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	conn, err := net.DialTimeout("tcp", peerAddr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	defer client.Close()

	err = client.Call("RaftNode.RequestVote", args, reply)
	if err != nil {
		log.Printf("RPC RequestVote to [%s] failed: %v", peerAddr, err)
		return false
	}

	return true
}

func (n *Node) sendAppendEntries(peerAddr string, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	conn, err := net.DialTimeout("tcp", peerAddr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	defer client.Close()

	err = client.Call("RaftNode.AppendEntries", args, reply)
	if err != nil {
		return false
	}

	return true
}
