package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// State represents the three possible roles a Raft node can occupy.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// Node represents a single server participating in the Raft consensus cluster.
type Node struct {
	mu sync.RWMutex

	// --- Core Identification ---
	id    string   // This node's unique ID (e.g., "node_1")
	peers []string // Network addresses of all other nodes in the cluster

	// --- Persistent State on all servers ---
	// (Must be saved to disk before responding to RPCs, though kept in RAM for now)
	currentTerm uint64
	votedFor    string // ID of the candidate that received vote in current term (or "" if none)
	log         *Log   // The replicated state machine log we just built

	// --- Volatile State on all servers ---
	commitIndex uint64 // Index of highest log entry known to be committed
	lastApplied uint64 // Index of highest log entry applied to the local TitanKV engine
	state       State

	// --- Volatile State on Leaders (Reinitialized after election) ---
	// nextIndex tracks what log entry to send to each Follower next.
	nextIndex map[string]uint64
	// matchIndex tracks the highest log entry known to be replicated on each Follower.
	matchIndex map[string]uint64

	// --- Concurrency & Timing ---
	// Randomized election timeout prevents split-brain (all nodes waking up to vote at once)
	electionResetEvent time.Time

	quit chan struct{}
	wg   sync.WaitGroup
}

// NewNode initializes a Raft node in the default Follower state.
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

// Start boots the Raft consensus engine and begins the randomized election timer.
func (n *Node) Start() {
	n.mu.Lock()
	n.electionResetEvent = time.Now()
	n.mu.Unlock()

	n.wg.Add(1)
	go n.runElectionTimer()

	log.Printf("Raft Node [%s] started as FOLLOWER.", n.id)
}

// Stop safely shuts down the Raft background threads.
func (n *Node) Stop() {
	close(n.quit)
	n.wg.Wait()
	log.Printf("Raft Node [%s] shut down.", n.id)
}

// runElectionTimer continuously checks if the Leader has died.
// If the timer expires without a heartbeat, the Follower triggers an election.
func (n *Node) runElectionTimer() {
	defer n.wg.Done()

	// Raft paper recommends election timeouts between 150ms and 300ms
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

			// Leaders do not run election timers
			if state != Leader {
				// If the time since the last heartbeat is greater than our random timeout, panic!
				if time.Since(lastEvent) >= timeoutDuration {
					n.startElection()
					// Reset the timeout duration for the next cycle
					timeoutDuration = n.randomElectionTimeout()
				}
			}
		}
	}
}

// startElection transitions the node to Candidate and requests votes.
func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id // Vote for self
	n.electionResetEvent = time.Now()

	savedTerm := n.currentTerm
	n.mu.Unlock()

	log.Printf("Raft Node [%s] election timeout! Starting election for Term %d.", n.id, savedTerm)
	go n.broadcastRequestVote()

}

// randomElectionTimeout generates a jittered timeout between 150ms and 300ms.
func (n *Node) randomElectionTimeout() time.Duration {
	// Formula: 150 + rand(150)
	ms := 150 + rand.Intn(150)
	return time.Duration(ms) * time.Millisecond
}
