package raft

import (
	"log"
	"sync/atomic"
	"time"
)

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

	var votesReceived int32 = 1

	quorum := (len(n.peers)+1)/2 + 1

	log.Printf("Node [%s] campaigning for Term %d. Needs %d votes for Quorum.", n.id, savedTerm, quorum)

	for _, peerId := range n.peers {
		go func(peer string) {
			reply := RequestVoteReply{}

			if ok := n.sendRequestVote(peer, &args, &reply); !ok {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Candidate || n.currentTerm != savedTerm {
				return
			}

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

func (n *Node) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	reply.Term = n.currentTerm
	reply.VoteGranted = false

	if n.votedFor != "" && n.votedFor != args.CandidateId {
		return nil
	}

	lastIndex, lastTerm := n.log.LastInfo()

	isUpToDate := false
	if args.LastLogTerm > lastTerm {
		isUpToDate = true
	} else if args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex {
		isUpToDate = true
	}

	if isUpToDate {
		reply.VoteGranted = true
		n.votedFor = args.CandidateId
		n.electionResetEvent = time.Now()
		log.Printf("Node [%s] granted vote to [%s] for Term %d", n.id, args.CandidateId, args.Term)
	}

	return nil
}

func (n *Node) becomeFollower(term uint64) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.electionResetEvent = time.Now()
}

func (n *Node) becomeLeader() {
	n.state = Leader

	lastLogIndex, _ := n.log.LastInfo()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastLogIndex + 1
		n.matchIndex[peer] = 0
	}

	go n.startHeartbeatTicker()
}

func (n *Node) startHeartbeatTicker() {
	n.broadcastHeartbeats()

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

			if state != Leader {
				return
			}
			n.broadcastHeartbeats()
		}
	}
}
