package raft

import (
	"log"
	"sync/atomic"
	"time"
)

// broadcastRequestVote fires off vote requests to all peers concurrently.
func (n *Node) broadcastRequestVote() {
	n.mu.RLock()
	savedTerm := n.currentTerm
	lastIndex, lastTerm := n.log.LastInfo()
	n.mu.RUnlock()

	args := RequestVoteArgs{
		Term:         savedTerm,
		CandidateId:  n.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	// We start with 1 vote (we always vote for ourselves)
	var votesReceived int32 = 1

	// A Quorum is the absolute majority required to win.
	// For 3 nodes: (3/2)+1 = 2. For 5 nodes: (5/2)+1 = 3.
	quorum := (len(n.peers)+1)/2 + 1

	log.Printf("Node [%s] campaigning for Term %d. Needs %d votes for Quorum.", n.id, savedTerm, quorum)

	// Blast requests to all peers concurrently
	for _, peerId := range n.peers {
		go func(peer string) {
			reply := RequestVoteReply{}

			if ok := n.sendRequestVote(peer, &args, &reply); !ok {
				return // Network error or timeout talking to this peer
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// If our state changed while waiting for the network, abort the campaign.
			// This happens if someone else won the election before we finished getting our votes.
			if n.state != Candidate || n.currentTerm != savedTerm {
				return
			}

			// If the peer replied with a higher term, our term is obsolete.
			// We immediately step down in shame and become a Follower.
			if reply.Term > savedTerm {
				log.Printf("Node [%s] discovered higher term %d. Stepping down.", n.id, reply.Term)
				n.becomeFollower(reply.Term)
				return
			}

			if reply.VoteGranted {
				votes := atomic.AddInt32(&votesReceived, 1)
				if int(votes) == quorum {
					log.Printf("Node [%s] WON THE ELECTION for Term %d!", n.id, savedTerm)
					n.becomeLeader()
				}
			}
		}(peerId)
	}
}

// RequestVote is the RPC handler. This is what a Follower runs when a Candidate asks it for a vote.
func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 1. If Candidate's term is older than ours, reject immediately.
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.VoteGranted = false
		return nil
	}

	// 2. If Candidate's term is newer, we step down, update our term, and clear who we voted for.
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	reply.Term = n.currentTerm
	reply.VoteGranted = false

	// 3. Have we already voted for someone else in this term?
	if n.votedFor != "" && n.votedFor != args.CandidateId {
		return nil // Already cast our ballot elsewhere
	}

	// 4. The Election Restriction Safety Check (Log Matching)
	lastIndex, lastTerm := n.log.LastInfo()

	// A candidate's log is "up-to-date" if its last term is greater than ours.
	// If the terms are the same, its index must be equal to or greater than ours.
	isUpToDate := false
	if args.LastLogTerm > lastTerm {
		isUpToDate = true
	} else if args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex {
		isUpToDate = true
	}

	if isUpToDate {
		reply.VoteGranted = true
		n.votedFor = args.CandidateId
		n.electionResetEvent = time.Now() // Reset timer so we don't start an election while voting
		log.Printf("Node [%s] granted vote to [%s] for Term %d", n.id, args.CandidateId, args.Term)
	}

	return nil
}

// becomeFollower resets the node to a passive state.
func (n *Node) becomeFollower(term uint64) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.electionResetEvent = time.Now()
}

// becomeLeader transitions the node to Dictator state and initializes replication trackers.
func (n *Node) becomeLeader() {
	n.state = Leader

	// Reset the volatile leader state to track where each follower is currently at
	lastLogIndex, _ := n.log.LastInfo()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastLogIndex + 1
		n.matchIndex[peer] = 0
	}

	go n.startHeartbeatTicker()
}

// startHeartbeatTicker continuously fires empty logs to suppress follower elections.
func (n *Node) startHeartbeatTicker() {
	// 1. Fire an immediate heartbeat to establish authority instantly
	n.broadcastHeartbeats()

	// 2. Set up a ticker to fire every 50 milliseconds
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.quit:
			return
		case <-ticker.C:
			n.mu.RLock()
			state := n.state
			n.mu.RUnlock()

			// If we get deposed and are no longer Leader, kill this heartbeat loop
			if state != Leader {
				return
			}
			n.broadcastHeartbeats()
		}
	}
}
