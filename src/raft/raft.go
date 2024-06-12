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

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// Server state
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.

// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A log entry.
type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state   State // server state
	applych chan ApplyMsg

	electionTimer  time.Time
	heartBeatTimer time.Time

	// Persisent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state
	commitIndex int
	lastApplied int

	// Volatile state for Leader
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	rf.DPrintf("GetState: term = %d, isleader = %t\n", term, isleader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
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
	// Your code here (2C).
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
	// Your code here (2D).

}

// AppendEntries RPC arguments
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if out-of-date
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.DPrintf("AppendEntries: args.term [%d] < rf.currentTerm [%d]\n", args.Term, rf.currentTerm)
		return
	}

	// reset
	rf.resetTimer(ElectionTime)
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1
	// rf.DPrintf("AppendEntries: reset election timer\n")

	reply.Term = rf.currentTerm
	reply.Success = true

	// if conflict
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.DPrintf("AppendEntries: conflict. args.PrevLogIndex[%d], len(rf.log)[%d]\n", args.PrevLogIndex, len(rf.log))
		reply.Success = false
		return
	}

	// append new logs
	if args.Entries != nil {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		rf.DPrintf("AppendEntries: append logs N[%d], Logs[%d]\n", len(args.Entries), len(rf.log))
	}

	// 同步提交
	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.log[rf.lastApplied-1].Command,
		}
		rf.applych <- applyMsg
		rf.commitIndex = rf.lastApplied
		//fmt.Printf("[	AppendEntries func-rf(%v)	] commitLog  \n", rf.me)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	if args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.DPrintf("RequestVote: term %d < currentTerm %d\n", args.Term, rf.currentTerm)
		return
	}

	// 自己过时，就重制
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// 避免刚投完票就进入选举状态
	rf.resetTimer(ElectionTime)

	if rf.votedFor == -1 {
		// Reply true if candidate's log is at least as up-to-date as receiver's log
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.DPrintf("RequestVote: grant vote candidate[%d]\n", args.CandidateId)
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			rf.DPrintf("RequestVote: candidate[%d]'s log is not up-to-date\n", args.CandidateId)
		}
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.DPrintf("RequestVote: had voted to candidate[%d]\n", args.CandidateId)
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
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).

	if rf.killed() {
		return index, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, isLeader
	}

	isLeader = true

	// just append a new log
	term = rf.currentTerm
	index = len(rf.log)
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.DPrintf("Start: append log[%d] = %v\n", index, command)

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

func (rf *Raft) resetTimer(ot Overtime_t) {
	if ot == ElectionTime {
		rf.electionTimer = time.Now()
	} else {
		rf.heartBeatTimer = time.Now()
	}
	rf.DPrintf("resetTimer: %d\n", ot)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

	var vote_count int32 = 1

	// reset time at election begin
	rf.resetTimer(ElectionTime)

	// Send RequestVote RPCs to all other servers.
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(server, args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Candidate {
				rf.DPrintf("Not a candidate anymore\n")
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.resetTimer(ElectionTime)
				rf.DPrintf("Becoming a follower again\n")
				return
			}

			if reply.VoteGranted {
				// Increment vote count
				atomic.AddInt32(&vote_count, 1)

				if (atomic.LoadInt32(&vote_count)+1) > int32(len(rf.peers)/2) && rf.state == Candidate {
					atomic.StoreInt32(&vote_count, 0) // once

					rf.state = Leader
					rf.DPrintf("Becoming a leader, nextIndex = %d\n", len(rf.log))

					// init nextIndex and matchIndex
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}

					// reset peers timer
					rf.beatHeart()
				}
			}
		}(i)

	}
}

func (rf *Raft) beatHeart() {
	rf.resetTimer(HeartBeatTime)
	var success_count int32 = 0

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			for {
				rf.mu.Lock()

				rf.DPrintf("Beat heart to server[%d]\n", server)
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[server] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
					Entries:      rf.log[rf.nextIndex[server]:],
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				reply := &AppendEntriesReply{}
				for !rf.killed() && !rf.sendAppendEntries(server, args, reply) { // repeat
				}

				rf.mu.Lock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.resetTimer(ElectionTime)
					rf.DPrintf("Becoming a follower again\n")
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					rf.nextIndex[server] = len(rf.log)
					rf.matchIndex[server] = len(rf.log) - 1

					if reply.Term == rf.currentTerm {
						atomic.AddInt32(&success_count, 1)
						if atomic.LoadInt32(&success_count)+1 > int32(len(rf.peers)/2) {
							atomic.StoreInt32(&success_count, 0) // once

							// commit log
							for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
								count := 0
								for i := range rf.peers {
									if rf.matchIndex[i] >= N {
										count++
									}
								}
								if count > len(rf.peers)/2 {
									rf.commitIndex = N
									break
								}
							}

							// apply log
							for rf.lastApplied < rf.commitIndex {
								rf.lastApplied++
								applyMsg := ApplyMsg{
									CommandValid: true,
									Command:      rf.log[rf.lastApplied-1].Command,
									CommandIndex: rf.lastApplied,
								}
								rf.applych <- applyMsg
							}
						}
					}

					rf.mu.Unlock()
					return
				} else {
					rf.nextIndex[server]-- // try again
				}
			}
		}(i)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.Lock()

		if rf.state == Follower || rf.state == Candidate {
			lapse := time.Now().Sub(rf.electionTimer)
			if lapse > time.Duration(RandDuration(ElectionTime))*time.Millisecond {
				rf.DPrintf("Start election\n")
				rf.mu.Unlock()
				rf.startElection()
			} else {
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
		} else {
			lapse := time.Now().Sub(rf.heartBeatTimer)
			if lapse > time.Duration(RandDuration(HeartBeatTime))*time.Millisecond {
				rf.DPrintf("Beat heart\n")
				rf.mu.Unlock()
				rf.beatHeart()
			} else {
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.applych = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.electionTimer = time.Now()
	rf.heartBeatTimer = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
