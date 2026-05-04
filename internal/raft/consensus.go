package raft

import (
	"errors"
	"log"
	"sort"
)

var (
	ErrNotLeader = errors.New("node is not the leader")
)

// Propose is the entry point for external client data.
// It takes a serialized Phase 4 network command (like a PUT or DEL),
// appends it to the Raft log, and initiates the replication process.
func (n *Node) Propose(command []byte) (uint64, uint64, error) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return 0, 0, ErrNotLeader
	}

	// 1. Create the new log entry
	lastIndex, _ := n.log.LastInfo()
	newIndex := lastIndex + 1
	term := n.currentTerm

	entry := LogEntry{
		Term:    term,
		Index:   newIndex,
		Command: command,
	}

	// 2. The Leader immediately appends it to its own log.
	// (Note: It is NOT committed yet. It is not in TitanKV).
	n.log.Append(entry)

	// 3. Update the leader's own match and next indexes for itself
	n.matchIndex[n.id] = newIndex
	n.nextIndex[n.id] = newIndex + 1

	n.mu.Unlock()

	log.Printf("Leader [%s] proposing new entry at Index %d, Term %d", n.id, newIndex, term)

	// 4. Force an immediate replication cycle instead of waiting for the next heartbeat timer
	n.replicateToFollowers()

	return newIndex, term, nil
}

// replicateToFollowers is the aggressive data-sending version of broadcastHeartbeats.
func (n *Node) replicateToFollowers() {
	n.mu.RLock()
	savedTerm := n.currentTerm
	leaderId := n.id
	n.mu.RUnlock()

	for _, peerId := range n.peers {
		if peerId == n.id {
			continue // Don't send RPCs to ourselves
		}

		go func(peer string) {
			n.mu.RLock()
			nextIdx := n.nextIndex[peer]
			prevLogIndex := nextIdx - 1
			prevLogTerm, _ := n.log.TermAtIndex(prevLogIndex)

			// Grab all new entries that this specific follower doesn't have yet
			entries := n.log.EntriesAfter(prevLogIndex)
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

			// Note: Phase 6 Network call placeholder
			if ok := n.sendAppendEntries(peer, &args, &reply); !ok {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Leader || n.currentTerm != savedTerm {
				return // State changed mid-flight
			}

			if reply.Term > savedTerm {
				n.becomeFollower(reply.Term)
				return
			}

			if reply.Success {
				// The follower successfully appended the data!
				// Update our trackers to reflect their new state.
				if len(entries) > 0 {
					lastAppendedIndex := entries[len(entries)-1].Index
					n.nextIndex[peer] = lastAppendedIndex + 1
					n.matchIndex[peer] = lastAppendedIndex

					// Evaluate if we have hit a Quorum and can commit!
					n.advanceCommitIndex()
				}
			} else {
				// The Log Matching Check FAILED on the follower.
				// This means their log is corrupted or out of date.
				// We decrement nextIndex and try again on the next cycle,
				// forcing them to overwrite bad data.
				if n.nextIndex[peer] > 1 {
					n.nextIndex[peer]--
				}
			}
		}(peerId)
	}
}

// advanceCommitIndex calculates if a majority of nodes have replicated a specific log index.
// If so, the Leader marks it as "Committed".
func (n *Node) advanceCommitIndex() {
	// 1. Gather all the matchIndexes (including the leader's own)
	var matchIndexes []int
	for _, idx := range n.matchIndex {
		matchIndexes = append(matchIndexes, int(idx))
	}

	// 2. Sort them in ascending order
	sort.Ints(matchIndexes)

	// 3. Find the median. Because we sorted them, the middle value represents
	// the highest index that a mathematical MAJORITY of nodes possess.
	// For 3 nodes, index 1 is the median. For 5 nodes, index 2.
	majorityIndex := matchIndexes[len(matchIndexes)/2]

	// 4. Raft Safety Rule: A leader can only advance the commit index
	// for entries written in its CURRENT term.
	majorityTerm, err := n.log.TermAtIndex(uint64(majorityIndex))

	if err == nil && uint64(majorityIndex) > n.commitIndex && majorityTerm == n.currentTerm {
		log.Printf("Leader [%s] reached Quorum! Advancing Commit Index from %d to %d",
			n.id, n.commitIndex, majorityIndex)

		n.commitIndex = uint64(majorityIndex)

		// Phase 5 Todo: Trigger a channel here to wake up the State Machine
		// (TitanKV) to actually execute the committed logs!
		go n.applyCommittedLogs()
	}
}

// applyCommittedLogs safely hands off committed data from Raft to your LSM-Tree.
func (n *Node) applyCommittedLogs() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry, err := n.log.Get(n.lastApplied)
		if err != nil {
			log.Printf("Critcal Error getting log for application: %v", err)
			continue
		}

		// In reality, this would send the entry.Command down a Go Channel
		// to your Phase 4 TCP Server, which would then call db.Put()
		log.Printf("--> APPLYING INDEX %d TO TITANKV ENGINE <--", entry.Index)
	}
}
