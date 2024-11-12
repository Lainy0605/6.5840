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
	"6.5840/labgob"
	"bytes"
	"fmt"
	"log"

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
	currentTerm int
	votedFor    int
	log         []LogEntry
	state       State

	commitIndex int //index of highest log entry known to be committed
	lastApplied int //index of highest log entry applied to state machine

	applyCh    chan ApplyMsg
	nextIndex  []int //for each server, index of the next log entry to send to that server
	matchIndex []int //for each server, index of highest log entry known to be replicated on server

	// leader election
	voteCount      int
	electionTimer  *time.Timer
	heartBeatTimer *time.Timer

	//Lab2D
	snapShot          []byte //the latest snapshot
	lastIncludedIndex int    //the snapshot replaces all entries up through and including this index
	lastIncludedTerm  int    //term of lastIncludedIndex
}

const HEARTBEAT_INTERVAL = 100 * time.Millisecond

type State int

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	Term    int
	Command interface{}
}

const DEBUG_MODE = false

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.state == LEADER

	return term, isLeader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.log) != nil ||
		e.Encode(rf.lastIncludedIndex) != nil ||
		e.Encode(rf.lastIncludedTerm) != nil {
		log.Fatal("Encode error")
	}

	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var Log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&Log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = Log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	if DEBUG_MODE {
		fmt.Printf("peer %d starts to snapshot\n", rf.me)
	}

	if rf.lastApplied < index || index <= rf.lastIncludedIndex {
		return
	}

	relativeIndex := rf.getRelativeLogIndex(index)
	oldLength := len(rf.log)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[relativeIndex].Term
	rf.log = rf.log[relativeIndex:]
	rf.snapShot = snapshot
	if DEBUG_MODE {
		fmt.Printf("peer %d snapshots log from length %d to %d\n", rf.me, oldLength, len(rf.log))
	}
	rf.persist()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	//Send the entire snapshot in a single InstallSnapshot RPC. Don't implement Figure 13's offset mechanism for splitting up the snapshot.
	Offset int  //not use
	Done   bool //not use
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.state = FOLLOWER
	rf.electionTimer.Reset(rf.randomElectionTimeout())

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Term = rf.currentTerm
		return
	}

	//If existing log entry has same index and term as snapshot’s
	//last included entry, retain log entries following it and reply
	relativeLogIndex := rf.getRelativeLogIndex(args.LastIncludedIndex)
	if relativeLogIndex < len(rf.log) && rf.log[relativeLogIndex].Term == args.LastIncludedTerm {
		rf.log = rf.log[relativeLogIndex:]
	} else {
		rf.log = append([]LogEntry{{
			args.LastIncludedTerm,
			nil,
		}})
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapShot = args.Data

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.applyCh <- ApplyMsg{
		false,
		nil,
		0,
		true,
		args.Data,
		args.LastIncludedTerm,
		args.LastIncludedIndex,
	}

	rf.persist()

	if DEBUG_MODE {
		fmt.Printf("peer %d gets installSnapshot from peer %d with lastIncludedIndex %d\n", rf.me, args.LeaderId, args.LastIncludedIndex)
	}
	return
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

	if DEBUG_MODE {
		fmt.Printf("peer %d gets vote request from CANDIDATE %d with term %d\n", rf.me, args.CandidateId, rf.currentTerm)
	}

	if args.Term < rf.currentTerm { //candidate's term is smaller than current peer's term, refuse to vote
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		if DEBUG_MODE {
			fmt.Printf("peer %d refuses to vote for CANDIDATE %d with term %d\n", rf.me, args.CandidateId, rf.currentTerm)
		}
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()

		if DEBUG_MODE {
			fmt.Printf("peer %d becomes FOLLOWER with term %d\n", rf.me, rf.currentTerm)
		}
	}

	vote := false
	reply.VoteGranted = false

	if args.LastLogTerm > rf.getLastLogTerm() { //candidate's log entry is up-to-date
		vote = true
	} else if args.LastLogTerm == rf.getLastLogTerm() {
		if args.LastLogIndex >= rf.getLastLogIndex() {
			vote = true
		}
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && vote {
		rf.state = FOLLOWER
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()

		if DEBUG_MODE {
			fmt.Printf("peer %d votes for %d with term %d\n", rf.me, rf.votedFor, rf.currentTerm)
		}
	}

	return
}

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
	XTerm   int //which term that leader's appendLog should begin from
	XIndex  int //the index of leader's appendLog should begin from
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	} else {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.persist()
	}

	rf.electionTimer.Reset(rf.randomElectionTimeout())
	if DEBUG_MODE {
		fmt.Printf("peer %d gets heartbeat from LEADER %d with term %d\n", rf.me, args.LeaderId, rf.currentTerm)
	}

	//Reply false if log doesn't contain an entry at prevLogIndex,whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.XIndex = rf.getLastLogIndex() + 1
		reply.XTerm = rf.getLastLogTerm()
		return
	} else if rf.log[rf.getRelativeLogIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[rf.getRelativeLogIndex(args.PrevLogIndex)].Term
		reply.XIndex = rf.findFirstIndexOfTerm(reply.XTerm)

		//if reply.XIndex == 0 {
		//	rf.log = []LogEntry{{0, nil}}
		//} else {
		//	rf.log = rf.log[:rf.getRelativeLogIndex(reply.XIndex)]
		//}
		rf.persist()
		return
	}

	reply.Success = true
	rf.log = rf.log[:rf.getRelativeLogIndex(args.PrevLogIndex+1)] //[:index)
	rf.persist()
	if args.Entries != nil {
		for _, entry := range args.Entries {
			rf.log = append(rf.log, entry)
			rf.persist()
		}
	}

	//TODO: what is index of last new entry?
	if args.LeaderCommit > rf.commitIndex {
		// rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		if args.LeaderCommit < rf.getLastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.getLastLogIndex()
		}
	}

	for rf.lastApplied < rf.commitIndex && rf.commitIndex <= rf.getLastLogIndex() {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{
			true,
			rf.log[rf.getRelativeLogIndex(rf.lastApplied)].Command,
			rf.lastApplied,
			false,
			nil,
			0,
			0,
		}
	}

	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
		} else {
			rf.nextIndex[server] = args.LastIncludedIndex + 1
			rf.matchIndex[server] = args.LastIncludedIndex
		}
	}

	return ok
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if reply.Term > rf.currentTerm { //not the latest, return to FOLLOWER
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
		} else if rf.state == CANDIDATE { //maybe it has become FOLLOWER or LEADER, do not accept remaining votes
			if reply.VoteGranted && args.Term == rf.currentTerm { //maybe get in next term
				if DEBUG_MODE {
					fmt.Printf("Candidate %d gets vote from %d\n", rf.me, server)
				}
				rf.voteCount++
				if rf.voteCount > len(rf.peers)/2 {
					rf.state = LEADER
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))

					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
						rf.matchIndex[i] = rf.getLastLogIndex()
					}

					if DEBUG_MODE {
						fmt.Printf("Candidate %d becomes LEADER\n", rf.me)
					}

					go rf.heartBeat()
				}
			}
		}
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	//for !ok { //If followers crash or run slowly, or if network packets are lost, the leader retries AppendEntries RPCs indefinitely
	//	if rf.killed() || rf.state != LEADER {
	//		return false
	//}
	//	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//}
	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		return ok
	} else if reply.Term != args.Term || args.Term != rf.currentTerm || rf.state != LEADER {
		_, _ = DPrintf("old AppendEntries RPC")
		return ok
	}

	if !reply.Success {
		rf.matchIndex[server] = reply.XIndex - 1
		rf.nextIndex[server] = reply.XIndex
	} else {
		if args.Entries == nil || len(args.Entries) == 0 {
			rf.matchIndex[server] = args.PrevLogIndex
		} else if args.Entries != nil && len(args.Entries) != 0 {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}

		for N := rf.commitIndex + 1; N <= rf.getLastLogIndex(); N++ {
			count := 0
			for i := 0; i < len(rf.peers); i++ {
				if rf.matchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 && rf.log[rf.getRelativeLogIndex(N)].Term == rf.currentTerm { //over half of servers commit
				rf.commitIndex = N
			}
		}

		for rf.lastApplied < rf.commitIndex && rf.commitIndex <= rf.getLastLogIndex() {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{
				true,
				rf.log[rf.getRelativeLogIndex(rf.lastApplied)].Command,
				rf.lastApplied,
				false,
				nil,
				0,
				0,
			}
		}
	}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (2B).
	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		rf.matchIndex[rf.me]++
		rf.nextIndex[rf.me]++

		if DEBUG_MODE {
			fmt.Printf("LEADER %d gets command %d\n", rf.me, command)
		}
		rf.log = append(rf.log, LogEntry{
			rf.currentTerm,
			command,
		})
		rf.persist()
		//The leader
		//appends the command to its log as a new entry, then issues
		//AppendEntries RPCs in parallel to each of the other
		//servers to replicate the entry.
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
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			rf.startElection()
		case <-rf.heartBeatTimer.C:
			rf.heartBeat()
			rf.heartBeatTimer.Reset(HEARTBEAT_INTERVAL)
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//TODO: check if this peer is LEADER?
	if rf.state == LEADER {
		return
	}

	if DEBUG_MODE {
		fmt.Printf("peer %d starts election\n", rf.me)
	}

	rf.electionTimer.Reset(rf.randomElectionTimeout())
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()

	for i := range rf.peers { // send RequestRPC to all other peers
		if i != rf.me {
			args := RequestVoteArgs{
				rf.currentTerm,
				rf.me,
				rf.getLastLogIndex(),
				rf.getLastLogTerm(),
			}
			reply := RequestVoteReply{}
			if DEBUG_MODE {
				fmt.Printf("Candidate %d requests peer %d's vote with term %d\n", rf.me, i, rf.currentTerm)
			}
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}

func (rf *Raft) heartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return
	}
	rf.electionTimer.Reset(rf.randomElectionTimeout())

	for i := range rf.peers {
		if i != rf.me {
			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				args := InstallSnapshotArgs{
					rf.currentTerm,
					rf.me,
					rf.lastIncludedIndex,
					rf.lastIncludedTerm,
					rf.snapShot,
					0,
					false,
				}
				reply := InstallSnapshotReply{}
				if DEBUG_MODE {
					fmt.Printf("LEADER %d send InstallSnapshot to peer %d with term %d\n", rf.me, i, rf.currentTerm)
				}
				go rf.sendInstallSnapshot(i, &args, &reply)
			} else {
				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := rf.log[rf.getRelativeLogIndex(prevLogIndex)].Term
				args := AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					prevLogIndex,
					prevLogTerm,
					nil,
					rf.commitIndex,
				}
				if rf.getLastLogIndex() >= rf.nextIndex[i] {
					args.Entries = rf.log[rf.getRelativeLogIndex(rf.nextIndex[i]):]
				}

				reply := AppendEntriesReply{}
				if DEBUG_MODE {
					fmt.Printf("LEADER %d send heartbeat to peer %d with term %d\n", rf.me, i, rf.currentTerm)
				}
				go rf.sendAppendEntries(i, &args, &reply)
			}
		}
	}
}

// rf.log stores entry from index = 1, and rf.log[0] is null(doesn't store any entry)
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[rf.getRelativeLogIndex(rf.getLastLogIndex())].Term
}

func (rf *Raft) getAbsoluteLogIndex(index int) int {
	return index + rf.lastIncludedIndex
}

func (rf *Raft) getRelativeLogIndex(index int) int {
	return index - rf.lastIncludedIndex
}

// find the index of the first log entry of given term
func (rf *Raft) findFirstIndexOfTerm(term int) int {
	for i := 1; i < len(rf.log); i++ {
		if rf.log[i].Term == term {
			return rf.getAbsoluteLogIndex(i)
		}
	}

	//the first log entry doesn't exists in this log, maybe in snapshot
	return 0
}

func (rf *Raft) randomElectionTimeout() time.Duration {
	ms := 500 + (rand.Int63() % 500)
	return time.Duration(ms) * time.Millisecond
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
	// 2A
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.electionTimer = time.NewTimer(rf.randomElectionTimeout())
	rf.heartBeatTimer = time.NewTimer(HEARTBEAT_INTERVAL)

	//2B
	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{
		0,
		nil,
	})

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	//2D
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.snapShot = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapShot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
