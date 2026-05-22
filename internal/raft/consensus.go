package raft

import (
	"errors"
	"log"
	"sort"
)

var (
	ErrNotLeader = errors.New("node is not the leader")
)

func (n *Node) Propose(command []byte) (uint64, uint64, error) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return 0, 0, ErrNotLeader
	}

	lastIndex, _ := n.log.LastInfo()
	newIndex := lastIndex + 1
	term := n.currentTerm

	entry := LogEntry{
		Term:    term,
		Index:   newIndex,
		Command: command,
	}

	n.log.Append(entry)

	n.matchIndex[n.id] = newIndex
	n.nextIndex[n.id] = newIndex + 1

	n.mu.Unlock()

	log.Printf("Leader [%s] proposing new entry at Index %d, Term %d", n.id, newIndex, term)

	n.replicateToFollowers()

	return newIndex, term, nil
}

func (n *Node) replicateToFollowers() {
	n.mu.RLock()
	savedTerm := n.currentTerm
	leaderId := n.id
	n.mu.RUnlock()

	for _, peerId := range n.peers {
		if peerId == n.id {
			continue
		}

		go func(peer string) {
			n.mu.RLock()
			nextIdx := n.nextIndex[peer]
			prevLogIndex := nextIdx - 1
			prevLogTerm, _ := n.log.TermAtIndex(prevLogIndex)

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

			if ok := n.sendAppendEntries(peer, &args, &reply); !ok {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Leader || n.currentTerm != savedTerm {
				return
			}

			if reply.Term > savedTerm {
				n.becomeFollower(reply.Term)
				return
			}

			if reply.Success {
				if len(entries) > 0 {
					lastAppendedIndex := entries[len(entries)-1].Index
					n.nextIndex[peer] = lastAppendedIndex + 1
					n.matchIndex[peer] = lastAppendedIndex

					n.advanceCommitIndex()
				}
			} else {
				if n.nextIndex[peer] > 1 {
					n.nextIndex[peer]--
				}
			}
		}(peerId)
	}
}

func (n *Node) advanceCommitIndex() {
	var matchIndexes []int
	for _, idx := range n.matchIndex {
		matchIndexes = append(matchIndexes, int(idx))
	}

	sort.Ints(matchIndexes)

	majorityIndex := matchIndexes[len(matchIndexes)/2]

	majorityTerm, err := n.log.TermAtIndex(uint64(majorityIndex))

	if err == nil && uint64(majorityIndex) > n.commitIndex && majorityTerm == n.currentTerm {
		log.Printf("Leader [%s] reached Quorum! Advancing Commit Index from %d to %d",
			n.id, n.commitIndex, majorityIndex)

		n.commitIndex = uint64(majorityIndex)

		go n.applyCommittedLogs()
	}
}

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

		log.Printf("--> APPLYING INDEX %d TO TITANKV ENGINE <--", entry.Index)
	}
}
