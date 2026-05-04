package raft

import (
	"log"
	"time"
)

// broadcastHeartbeats sends empty AppendEntries RPCs to all followers.
// Leaders run this in a continuous loop to suppress new elections.
func (n *Node) broadcastHeartbeats() {
	n.mu.RLock()
	savedTerm := n.currentTerm
	leaderId := n.id
	n.mu.RUnlock()

	for _, peerId := range n.peers {
		go func(peer string) {
			n.mu.RLock()
			// Fetch the index we expect this specific follower to need next
			nextIdx := n.nextIndex[peer]

			// Get the information about the entry immediately BEFORE nextIdx
			prevLogIndex := nextIdx - 1
			prevLogTerm, _ := n.log.TermAtIndex(prevLogIndex)

			// For a pure heartbeat, we send NO new entries
			entries := make([]LogEntry, 0)

			leaderCommit := n.commitIndex
			n.mu.RUnlock()

			args := AppendEntriesArgs{
				Term:         savedTerm,
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			}

			reply := AppendEntriesReply{}

			if ok := n.sendAppendEntries(peer, &args, &reply); !ok {
				return // Network timeout
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// If our state changed while waiting for the network, abort.
			if n.state != Leader || n.currentTerm != savedTerm {
				return
			}

			// If the follower replied with a higher term, we are a deposed dictator.
			if reply.Term > savedTerm {
				log.Printf("Leader [%s] discovered higher term %d. Stepping down.", n.id, reply.Term)
				n.becomeFollower(reply.Term)
				return
			}

			// Phase 5 Todo: Handle reply.Success logic when we actually send data logs.
			// For heartbeats, we just care that we successfully reset their timer.
		}(peerId)
	}
}

// AppendEntries is the RPC handler run by the FOLLOWER when the Leader sends logs/heartbeats.
func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 1. Reject instantly if the Leader's term is older than ours (False Leader)
	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.Success = false
		return nil
	}

	// 2. We recognize this Leader. Reset our state.
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	// Acknowledge the heartbeat to prevent us from starting an election
	n.electionResetEvent = time.Now()
	n.state = Follower
	n.currentTerm = args.Term
	reply.Term = n.currentTerm

	// 3. THE LOG MATCHING PROPERTY CHECK
	// Does the follower have an entry at PrevLogIndex?
	lastLogIndex, _ := n.log.LastInfo()
	if args.PrevLogIndex > lastLogIndex {
		// We are missing data! We can't accept the new entries yet.
		reply.Success = false
		return nil
	}

	// If we have an entry at PrevLogIndex, does its term match the Leader's PrevLogTerm?
	myPrevLogTerm, _ := n.log.TermAtIndex(args.PrevLogIndex)
	if myPrevLogTerm != args.PrevLogTerm {
		// Data corruption/Split-Brain detected!
		// Forcefully delete the conflicting entry and all that follow it.
		n.log.TruncateSuffix(args.PrevLogIndex)
		reply.Success = false
		return nil
	}

	// 4. If we passed the matching check, append any new entries we don't already have.
	if len(args.Entries) > 0 {
		for i, entry := range args.Entries {
			// Check if we already have this entry (e.g., duplicated network packet)
			existingTerm, err := n.log.TermAtIndex(entry.Index)

			if err == nil && existingTerm == entry.Term {
				continue // Already have it, perfectly safe
			}

			if err == nil && existingTerm != entry.Term {
				// Existing entry conflicts with the leader's new entry. Truncate!
				n.log.TruncateSuffix(entry.Index)
			}

			// Append the missing entries
			n.log.Append(args.Entries[i:]...)
			break // Once we append the rest of the slice, we are done
		}
	}

	// 5. Update Commit Index
	// If the Leader has safely committed data, the Follower can commit it too.
	if args.LeaderCommit > n.commitIndex {
		lastNewEntryIndex, _ := n.log.LastInfo()

		// Set our commit index to the min(LeaderCommit, Index of last new entry)
		if args.LeaderCommit < lastNewEntryIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNewEntryIndex
		}

		// In a full system, you would trigger a goroutine here to "Apply"
		// these newly committed logs to your TitanKV engine.
	}

	reply.Success = true
	return nil
}
