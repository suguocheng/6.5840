package raft

import (
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// AppendEntries args,reply
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		DPrintf("Follower %d sees higher term from Candidate %d: Term %d", rf.me, args.CandidateId, args.Term)

		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = "Follower"
		rf.persist()
		resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)
	}

	if args.Term == rf.currentTerm {
		if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true
			rf.state = "Follower"
			rf.persist()
			resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)

			DPrintf("Follower %d vote for Candidate %d", rf.me, args.CandidateId)
		} else {
			reply.VoteGranted = false
			DPrintf("Follower %d don't vote for Candidate %d", rf.me, args.CandidateId)
		}
	} else {
		reply.VoteGranted = false
		DPrintf("Follower %d don't vote for Candidate %d", rf.me, args.CandidateId)
	}

	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// heartbeat
// func (rf *Raft) Heartbeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	if args.Term >= rf.currentTerm {
// 		rf.currentTerm = args.Term
// 		rf.voteFor = -1
// 		rf.state = "Follower"

// 		if args.LeaderCommit > rf.commitIndex {
// 			rf.commitIndex = Min(args.LeaderCommit, len(rf.logs)-1)
// 			DPrintf("Follower %d successfully appended logs. New commitIndex=%d", rf.me, rf.commitIndex)
// 		}

// 		resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)
// 		reply.Success = true
// 	} else {
// 		reply.Success = false
// 	}
// }

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing AppendEntries,  AppendEntriesArgs %v and AppendEntriesReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)

	// Reply false if term < currentTerm(§5.1)
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	// indicate the peer is the leader
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.voteFor = args.Term, -1
		rf.persist()
	}

	rf.currentTerm = args.Term
	rf.voteFor = -1
	rf.state = "Follower"
	rf.persist()
	resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm(§5.3)
	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// check the log is matched, if not, return the conflict index and term
	// if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it(§5.3)
	if !rf.isLogMatched(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastLogIndex := rf.getLastLog().Index
		// find the first index of the conflicting term
		if lastLogIndex < args.PrevLogIndex {
			// the last log index is smaller than the prevLogIndex, then the conflict index is the last log index
			reply.ConflictIndex, reply.ConflictTerm = lastLogIndex+1, -1
		} else {
			firstLogIndex := rf.getFirstLog().Index
			// find the first index of the conflicting term
			index := args.PrevLogIndex
			for index >= firstLogIndex && rf.logs[index-firstLogIndex].Term == args.PrevLogTerm {
				index--
			}
			reply.ConflictIndex, reply.ConflictTerm = index+1, args.PrevLogTerm
		}
		return
	}
	// append any new entries not already in the log
	firstLogIndex := rf.getFirstLog().Index
	for index, entry := range args.Entries {
		// find the junction of the existing log and the appended log.
		if entry.Index-firstLogIndex >= len(rf.logs) || rf.logs[entry.Index-firstLogIndex].Term != entry.Term {
			rf.logs = shrinkEntries(append(rf.logs[:entry.Index-firstLogIndex], args.Entries[index:]...))
			rf.persist()
			break
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) (paper)
	newCommitIndex := Min(args.LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		DPrintf("{Node %v} advances commitIndex from %v to %v with leaderCommit %v in term %v", rf.me, rf.commitIndex, newCommitIndex, args.LeaderCommit, rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing InstallSnapshot,  InstallSnapshotArgs %v and InstallSnapshotReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)

	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = "Follower"
		rf.persist()
		resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)

		// check the snapshot is more up-to-date than the current log
		if args.LastIncludedIndex <= rf.commitIndex {
			return
		}

		go func() {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      args.Data,
				SnapshotTerm:  args.LastIncludedTerm,
				SnapshotIndex: args.LastIncludedIndex,
			}
		}()

	} else {
		reply.Term = rf.currentTerm
	}
}
