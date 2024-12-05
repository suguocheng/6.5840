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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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

// 使用Raft的服务（例如k/v服务器）想要开始对下一个要追加到Raft日志的命令达成一致。
// 如果该服务器不是领导者，返回false。否则，立即开始达成一致并返回。
// 无法保证此命令将被提交到Raft日志，因为领导者可能会失败或输掉选举。即使Raft实例已被终止，该函数也应优雅地返回。

// 第一个返回值是命令将显示的索引，如果它被提交过。第二个返回值是当前的任期。第三个返回值如果这个服务器认为它是领导者，则为真。

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := rf.commitIndex + 1
	term, isLeader := rf.GetState()

	// Your code here (3B).

	if isLeader {
		log := LogEntry{}
		log = LogEntry{
			Command: command,
			Term:    term,
			Index:   index,
		}
		rf.logs = append(rf.logs, log)

	}

	return index, term, isLeader
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
		if rf.state != "Leader" {
			rf.heartbeatTimer.Stop()
		}

		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.startElection()
			if rf.state != "Leader" {
				resetTimer(rf.electionTimer, time.Duration(randomInRange(500, 1000))*time.Millisecond)
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == "Leader" {
				rf.broadcastHeartbeat()
				resetTimer(rf.heartbeatTimer, time.Duration(200)*time.Millisecond)
			}
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.state = "Candidate"
	rf.voteFor = rf.me
	voteCount := 1
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.logs[len(rf.logs)-1].Index,
			LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
		}
		go func(i int) {
			if rf.state != "Candidate" {
				// 如果当前节点状态不再是候选者或任期已更新，则停止操作
				return
			}
			reply := RequestVoteReply{}
			rf.sendRequestVote(i, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.VoteGranted {
				voteCount++
				if voteCount > len(rf.peers)/2 {
					rf.state = "Leader"
					rf.broadcastHeartbeat()
					rf.electionTimer.Stop()
					resetTimer(rf.heartbeatTimer, time.Duration(200)*time.Millisecond)
				}
			} else {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = "Follower"
					rf.voteFor = -1
				}
			}
		}(index)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	go func() {
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
		}
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			reply := AppendEntriesReply{}
			rf.peers[index].Call("Raft.Heartbeat", &args, &reply)
		}
	}()
}

func randomInRange(min, max int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(max-min) + min
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		// 如果Stop返回false，计时器可能已经过期但没有被读，清理通道
		select {
		case <-t.C:
			// 消费过期的信号防止通道堵塞
		default:
			// 通道里没有信号，不做任何处理
		}
	}
	t.Reset(d)
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
	rf.electionTimer = time.NewTimer(time.Duration(randomInRange(500, 1000)) * time.Millisecond)
	rf.heartbeatTimer = time.NewTimer(time.Duration(200) * time.Millisecond)
	// rf.heartbeatTimer.Stop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
