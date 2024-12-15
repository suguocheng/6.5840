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
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A, 3B).
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = "Follower"
		resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)

		reply.Term = rf.currentTerm
		if rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
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
func (rf *Raft) Heartbeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = "Follower"
		resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)
		reply.Success = true
	} else {
		reply.Success = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Follower %d received AppendEntries: PrevLogIndex=%d, PrevLogTerm=%d, Entries=%v",
		rf.me, args.PrevLogIndex, args.PrevLogTerm, args.Entries)

	if args.Term >= rf.currentTerm {
		DPrintf("Follower %d term %d is outdated, switching to follower", rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = "Follower"

		// 比较日志
		if args.PrevLogIndex >= len(rf.logs) || (args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
			DPrintf("Follower %d log mismatch: PrevLogIndex=%d, PrevLogTerm=%d", rf.me, args.PrevLogIndex, args.PrevLogTerm)
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}

		// 复制日志
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		args.Entries = args.Entries[args.PrevLogIndex+1:]
		rf.logs = append(rf.logs, args.Entries...)

		// 更新commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, len(rf.logs)+1)
		}

		DPrintf("Follower %d successfully appended logs. New commitIndex=%d", rf.me, rf.commitIndex)

		reply.Term = rf.currentTerm
		reply.Success = true
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}
