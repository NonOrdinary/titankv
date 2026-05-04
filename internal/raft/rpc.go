package raft

// ---------------------------------------------------------
// 1. RequestVote RPC
// ---------------------------------------------------------

// RequestVoteArgs represents the payload a Candidate sends to peers to request their vote.
type RequestVoteArgs struct {
	Term        uint64 // Candidate's current term
	CandidateId string // Candidate requesting vote (e.g., "node_1")

	// The next two fields are critical for the "Election Restriction" safety property.
	// A node will REJECT a vote if its own log is more up-to-date than the Candidate's log.
	LastLogIndex uint64 // Index of candidate's last log entry
	LastLogTerm  uint64 // Term of candidate's last log entry
}

// RequestVoteReply represents the Follower's response to a Candidate.
type RequestVoteReply struct {
	Term        uint64 // The Follower's current term (allows Candidate to update itself if it is behind)
	VoteGranted bool   // True means the Candidate received the vote
}

// ---------------------------------------------------------
// 2. AppendEntries RPC
// ---------------------------------------------------------

// AppendEntriesArgs serves a dual purpose:
// 1. Transmitting new log entries from the Leader to the Followers.
// 2. Acting as a Heartbeat (when Entries array is empty) to maintain Leader authority.
type AppendEntriesArgs struct {
	Term     uint64 // Leader's current term
	LeaderId string // So followers can redirect external clients to the current Leader

	// The "Log Matching Property" check. The Leader sends the index and term of the
	// entry immediately preceding the new ones. If the Follower doesn't have it, it rejects the append.
	PrevLogIndex uint64
	PrevLogTerm  uint64

	Entries []LogEntry // New log entries to store (empty for heartbeat)

	// Tells the Follower what data is safe to actually apply to the TitanKV LSM-tree
	LeaderCommit uint64
}

// AppendEntriesReply represents the Follower's response to the Leader.
type AppendEntriesReply struct {
	Term    uint64 // Follower's current term, for the Leader to step down if it is obsolete
	Success bool   // True if the Follower contained an entry matching PrevLogIndex and PrevLogTerm
}
