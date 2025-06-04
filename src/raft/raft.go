package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm    int
	voteFor        int
	logs           []LogEntry
	commitIndex    int
	lastApplied    int
	state          string
	nextIndex      []int
	matchIndex     []int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg
	applyCond      *sync.Cond
	replicatorCond []*sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == "Leader"
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		fmt.Printf("error")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = logs
		rf.lastApplied, rf.commitIndex = rf.getFirstLog().Index, rf.getFirstLog().Index
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex || index > rf.getLastLog().Index {
		return
	}

	// remove log entries up to index
	newLogs := make([]LogEntry, len(rf.logs[index-snapshotIndex:]))
	copy(newLogs, rf.logs[index-snapshotIndex:])
	rf.logs = newLogs

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)

	DPrintf("Server %d Generate snapshot before index %d", rf.me, index)
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// outdated snapshot
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	// need dummy entry at index 0
	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logs = make([]LogEntry, 1)
	} else {
		rf.logs = rf.logs[lastIncludedIndex-rf.getFirstLog().Index:]
		rf.logs[0].Command = nil
	}

	rf.logs[0].Term, rf.logs[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.commitIndex, rf.lastApplied = lastIncludedIndex, lastIncludedIndex

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)

	return true
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.getLastLog().Index + 1
	isLeader := rf.state == "Leader"
	DPrintf("server %d, Term %d, Starting command: %v", rf.me, rf.currentTerm, command)

	// Your code here (3B).

	if isLeader {
		rf.logs = append(rf.logs, LogEntry{
			Command: command,
			Term:    rf.currentTerm,
			Index:   index,
		})
		rf.persist()
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		DPrintf("Leader %d, Term %d, Appended log: Index=%d, Command=%v", rf.me, rf.currentTerm, index, command)

		go rf.BroadcastHeartbeat(false)
	} else {
		DPrintf("Not a leader, returning")
	}

	return index, rf.currentTerm, isLeader
}

func (rf *Raft) BroadcastHeartbeat(isHeartbeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartbeat {
			// should send heartbeat to all peers immediately
			go rf.broadcastAppendEntries(true, peer)
		} else {
			// just need to signal replicator to send log entries to peer
			rf.replicatorCond[peer].Signal()
		}
	}
}
func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check the logs of peer is behind the leader
	return rf.state == "Leader" && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// send log entries to peer
		rf.broadcastAppendEntries(false, peer)
	}
}

func (rf *Raft) broadcastAppendEntries(isHeartbeat bool, i int) {
	DPrintf("Leader %d broadcasting AppendEntries, Term %d, isHeartbeat %v", rf.me, rf.currentTerm, isHeartbeat)
	term := rf.currentTerm

	rf.mu.Lock()
	if rf.state != "Leader" || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[i] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		firstLog := rf.getFirstLog()
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: firstLog.Index,
			LastIncludedTerm:  firstLog.Term,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()

		reply := new(InstallSnapshotReply)

		if rf.sendInstallSnapshot(i, args, reply) {
			rf.mu.Lock()
			if rf.state == "Leader" && rf.currentTerm == args.Term {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = "Follower"
					rf.voteFor = -1
					rf.persist()
					resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)
					rf.heartbeatTimer.Stop()

				} else {
					rf.nextIndex[i] = args.LastIncludedIndex + 1
					rf.matchIndex[i] = args.LastIncludedIndex
				}
			}
			rf.mu.Unlock()
			DPrintf("{Node %v} sends InstallSnapshotArgs %v to {Node %v} and receives InstallSnapshotReply %v", rf.me, args, i, reply)
		}
	} else {
		prevLogTerm := rf.logs[prevLogIndex-rf.getFirstLog().Index].Term
		var entries []LogEntry
		if !isHeartbeat {
			entries = make([]LogEntry, len(rf.logs[prevLogIndex-rf.getFirstLog().Index+1:]))
			copy(entries, rf.logs[prevLogIndex-rf.getFirstLog().Index+1:])
		}

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		DPrintf("Leader %d sending AppendEntries to Follower %d: PrevLogIndex=%d, Entries=%v LeaderCommit=%d",
			rf.me, i, args.PrevLogIndex, args.Entries, args.LeaderCommit)

		reply := AppendEntriesReply{}

		if rf.sendAppendEntries(i, &args, &reply) {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				DPrintf("Leader %d sees higher term from Follower %d: Term %d", rf.me, i, reply.Term)

				// rf.logs = rf.logs[:len(rf.logs)-1]
				rf.currentTerm = reply.Term
				rf.state = "Follower"
				rf.voteFor = -1
				rf.persist()
				resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)
				rf.heartbeatTimer.Stop()

				rf.mu.Unlock()
				return
			} else {
				if reply.Success {
					match := args.PrevLogIndex + len(args.Entries)
					rf.matchIndex[i] = match
					rf.nextIndex[i] = match + 1
					DPrintf("Follower %d successfully replicated log, nextIndex=%d", i, rf.nextIndex[i])
					rf.updateCommitIndex()
					rf.mu.Unlock()
					return
				} else {
					// if rf.nextIndex[i] > 1 {
					// 	rf.nextIndex[i]--
					// }
					// DPrintf("Follower %d failed to replicate log, retrying with PrevLogIndex=%d", i, rf.nextIndex[i])
					if reply.XLen == 0 && reply.XTerm == 0 {
						rf.mu.Unlock()
						return
					}

					if reply.XTerm == -1 {
						// Follower 日志过短
						rf.nextIndex[i] = reply.XLen
					} else {
						rf.nextIndex[i] = reply.XIndex
					}

					rf.nextIndex[i] = Max(rf.nextIndex[i], 1)
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	for i := rf.getLastLog().Index; i > rf.commitIndex; i-- {
		if rf.logs[i-rf.getFirstLog().Index].Term != rf.currentTerm {
			continue // 提前跳过非当前 Term 的日志
		}
		count := 1 // 包括自己
		for j := range rf.peers {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= i {
				// DPrintf("ID=%d, matchIndex=%d", j, rf.matchIndex[j])
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = i
			rf.applyCond.Signal()
			DPrintf("Leader %d successfully update commitIndex. New commitIndex=%d", rf.me, rf.commitIndex)
			break
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied-rf.getFirstLog().Index+1:commitIndex-rf.getFirstLog().Index+1])
		rf.mu.Unlock()

		DPrintf("server %d applier: Applying logs from index %d to %d", rf.me, lastApplied+1, commitIndex)

		for _, entry := range entries {
			DPrintf("server %d applier: Applying log: Index=%d, Command=%v", rf.me, entry.Index, entry.Command)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		DPrintf("server %d applier: Finished applying logs up to commitIndex=%d", rf.me, rf.commitIndex)
		rf.mu.Unlock()
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			rf.startElection()
			if rf.state != "Leader" {
				resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)
			}
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == "Leader" {
				rf.BroadcastHeartbeat(true)
				resetTimer(rf.heartbeatTimer, time.Duration(125)*time.Millisecond)
			}
			rf.mu.Unlock()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
	}
}

func (rf *Raft) startElection() {
	DPrintf("Server %d start election", rf.me)

	rf.mu.Lock()
	rf.currentTerm++
	rf.state = "Candidate"
	rf.voteFor = rf.me
	rf.persist()

	voteCount := 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
	rf.mu.Unlock()
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(i int) {

			rf.mu.Lock()
			if rf.state != "Candidate" {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			reply := RequestVoteReply{}
			if rf.sendRequestVote(i, &args, &reply) {
				rf.mu.Lock()

				if reply.VoteGranted {
					voteCount++
					if voteCount > len(rf.peers)/2 && rf.state == "Candidate" {
						rf.state = "Leader"
						DPrintf("server %d become Leader", rf.me)
						rf.BroadcastHeartbeat(true)
						rf.electionTimer.Stop()
						resetTimer(rf.heartbeatTimer, time.Duration(125)*time.Millisecond)

						// 初始化nextIndex和matchIndex
						for index := range rf.nextIndex {
							if index == rf.me {
								continue
							}
							rf.nextIndex[index] = len(rf.logs)
							rf.matchIndex[index] = 0
						}
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = "Follower"
						rf.voteFor = -1
						rf.persist()
						resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)
					}
				}
				rf.mu.Unlock()
			}
		}(index)
	}
	// if voteCount <= len(rf.peers)/2 {
	// 	rf.currentTerm--
	// 	rf.state = "Follower"
	// 	rf.voteFor = -1
	// }
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.state = "Follower"
	rf.electionTimer = time.NewTimer(time.Duration(randomInRange(1000, 2000)) * time.Millisecond)
	rf.heartbeatTimer = time.NewTimer(time.Duration(125) * time.Millisecond)
	rf.heartbeatTimer.Stop()

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.replicatorCond = make([]*sync.Cond, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for peer := range peers {
		rf.matchIndex[peer], rf.nextIndex[peer] = 0, rf.getLastLog().Index+1
		if peer != rf.me {
			rf.replicatorCond[peer] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to send log entries to peer
			go rf.replicator(peer)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
