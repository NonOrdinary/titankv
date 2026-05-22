package raft

import (
	"log"
	"time"
)

func (n *Node) broadcastHeartbeats() {
	n.mu.RLock()
	savedTerm := n.currentTerm
	leaderId := n.id
	n.mu.RUnlock()

	for _, peerId := range n.peers {
		go func(peer string) {
			n.mu.RLock()
			nextIdx := n.nextIndex[peer]

			prevLogIndex := nextIdx - 1
			prevLogTerm, _ := n.log.TermAtIndex(prevLogIndex)

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
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Leader || n.currentTerm != savedTerm {
				return
			}

			if reply.Term > savedTerm {
				log.Printf("Leader [%s] discovered higher term %d. Stepping down.", n.id, reply.Term)
				n.becomeFollower(reply.Term)
				return
			}
		}(peerId)
	}
}

func (n *Node) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.Success = false
		return nil
	}

	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}

	n.electionResetEvent = time.Now()
	n.state = Follower
	n.currentTerm = args.Term
	reply.Term = n.currentTerm

	lastLogIndex, _ := n.log.LastInfo()
	if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		return nil
	}

	myPrevLogTerm, _ := n.log.TermAtIndex(args.PrevLogIndex)
	if myPrevLogTerm != args.PrevLogTerm {
		n.log.TruncateSuffix(args.PrevLogIndex)
		reply.Success = false
		return nil
	}

	if len(args.Entries) > 0 {
		for i, entry := range args.Entries {
			existingTerm, err := n.log.TermAtIndex(entry.Index)

			if err == nil && existingTerm == entry.Term {
				continue
			}

			if err == nil && existingTerm != entry.Term {
				n.log.TruncateSuffix(entry.Index)
			}

			n.log.Append(args.Entries[i:]...)
			break
		}
	}

	if args.LeaderCommit > n.commitIndex {
		lastNewEntryIndex, _ := n.log.LastInfo()

		if args.LeaderCommit < lastNewEntryIndex {
			n.commitIndex = args.LeaderCommit
		} else {
			n.commitIndex = lastNewEntryIndex
		}
	}

	reply.Success = true
	return nil
}
