package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Node struct {
	mu sync.RWMutex

	id    string
	peers []string

	currentTerm uint64
	votedFor    string
	log         *Log

	commitIndex uint64
	lastApplied uint64
	state       State

	nextIndex map[string]uint64

	matchIndex map[string]uint64

	electionResetEvent time.Time

	quit chan struct{}
	wg   sync.WaitGroup
}

func NewNode(id string, peers []string, raftLog *Log) *Node {
	return &Node{
		id:          id,
		peers:       peers,
		currentTerm: 0,
		votedFor:    "",
		log:         raftLog,
		state:       Follower,
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		quit:        make(chan struct{}),
	}
}

func (n *Node) Start() {
	n.mu.Lock()
	n.electionResetEvent = time.Now()
	n.mu.Unlock()

	n.wg.Add(1)
	go n.runElectionTimer()

	log.Printf("Raft Node [%s] started as FOLLOWER.", n.id)
}

func (n *Node) Stop() {
	close(n.quit)
	n.wg.Wait()
	log.Printf("Raft Node [%s] shut down.", n.id)
}

func (n *Node) runElectionTimer() {
	defer n.wg.Done()

	timeoutDuration := n.randomElectionTimeout()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.quit:
			return
		case <-ticker.C:
			n.mu.RLock()
			state := n.state
			lastEvent := n.electionResetEvent
			n.mu.RUnlock()

			if state != Leader {
				if time.Since(lastEvent) >= timeoutDuration {
					n.startElection()
					timeoutDuration = n.randomElectionTimeout()
				}
			}
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.electionResetEvent = time.Now()

	savedTerm := n.currentTerm
	n.mu.Unlock()

	log.Printf("Raft Node [%s] election timeout! Starting election for Term %d.", n.id, savedTerm)
	go n.broadcastRequestVote()

}

func (n *Node) randomElectionTimeout() time.Duration {
	ms := 150 + rand.Intn(150)
	return time.Duration(ms) * time.Millisecond
}
