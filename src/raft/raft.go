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
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
}

type LogEntry struct {
	Command any
	Term    int
}

type ServerRole int

const (
	Follower ServerRole = iota
	Candidate
	Leader
)

const ElectionTimeout = 500 * time.Millisecond
const HeartBeatTimeout = 150 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	applyCond *sync.Cond
	peers     []*labrpc.ClientEnd //  // FollowerRPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int
	votedFor    int // -1 for when votedFor = null
	log         []LogEntry
	// Volatile state on all servers:
	commitIndex   int
	lastApplied   int
	currentLeader int

	prevRole        ServerRole
	currentRole     ServerRole // 0: Follower; 1: Candidate; 2: Leader
	electionResetTs time.Time

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int
}

func (rf *Raft) updateRole(newRole ServerRole) {
	rf.prevRole = rf.currentRole
	rf.currentRole = newRole
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// (2A)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.currentLeader == rf.me
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).interface
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // Candidate's term
	CandidateId  int
	LastLogIndex int // idx of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry

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

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.updateRole(Follower)
	}

	if args.Term >= rf.currentTerm && rf.votedFor != -1 {
		var isCandidateAsUpToDate bool
		if args.LastLogTerm > rf.currentTerm {
			isCandidateAsUpToDate = true
		} else if args.LastLogTerm == rf.currentTerm && args.LastLogIndex >= len(rf.log) {
			isCandidateAsUpToDate = true
		}

		if isCandidateAsUpToDate {
			reply.VoteGranted = true
			rf.electionResetTs = time.Now()
		}
	}

}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int // index of log entry immediatly preceding new ones
	PrevLogTerm  int // term of PrevLogIndex
	Entries      []LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	rf.electionResetTs = time.Now()

	if args.Term < rf.currentTerm || len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	if rf.currentRole != Leader || (rf.currentRole == Leader && args.Term > rf.currentTerm) {
		rf.updateRole(Follower)
	}

	// Overwrite conflicting indexes (if any) to match leader entries:
	nextReadIdx := 0
	for nextWriteIdx := args.PrevLogIndex + 1; nextWriteIdx < len(rf.log); nextWriteIdx++ {
		rf.log[nextWriteIdx] = args.Entries[nextReadIdx]
		nextReadIdx++
	}

	// Append extra entries (if any)
	rf.log = append(rf.log, args.Entries[nextReadIdx:]...)

	// Update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.applyCond.Broadcast() // kick off applyier goroutine
	}

	reply.Success = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller shointerfaceuld
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Function that implements the operations related to applying commited messages.
// Should be launched as a separate goroutine from the launchOps function()
func (rf *Raft) applyOp() {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()

	for {
		if rf.killed() {
			fmt.Printf("[server_%d::applyOp()] Server killed. Exiting goroutine.\n", rf.me)
			return
		}

		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				rf.lastApplied++
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i,
				}
			}
		} else {
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) followerBehavior() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if time.Since(rf.electionResetTs) >= ElectionTimeout+time.Duration(rand.IntN(100)) {
		rf.updateRole(Candidate)
	}
}

func (rf *Raft) runElection() {
	// rf.mu.Lock() (already aquired by caller)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.electionResetTs = time.Now()

	var replies []*RequestVoteReply
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		args := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
			// LastLogIndex: len(rf.log) - 1,
			// LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		if len(rf.log) > 0 {
			args.LastLogIndex = len(rf.log) - 1
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
		} else {
			args.LastLogIndex = -1
			args.LastLogTerm = -1
		}

		reply := RequestVoteReply{Term: -1}
		replies = append(replies, &reply)
		go rf.sendRequestVote(i, &args, &reply)
	}
	rf.mu.Unlock()

	hasMajorityGranted := false
	hasMajorityRejected := false
	var votesGranted, votesRejected int
	for !hasMajorityGranted || !hasMajorityRejected {
		votesGranted = 1 // Start couting own vote
		votesRejected = 0
		for _, reply := range replies {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.updateRole(Candidate)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if reply.Term > -1 { // if reply received
				if reply.VoteGranted {
					votesGranted++
				} else {
					votesRejected++
				}
			}
		}

		if votesGranted > len(rf.peers)/2 {
			hasMajorityGranted = true
		} else if votesRejected > len(rf.peers)/2 {
			hasMajorityRejected = true
		}

		rf.mu.Lock()
		if time.Since(rf.electionResetTs) >= ElectionTimeout {
			fmt.Printf("[server_%d::runElection()] Election timeout.\n", rf.me)

			rf.mu.Unlock()
			return
		}
	}

	fmt.Printf(`[server_%d::runElection()] Election finished. 
				Votes Granted: %d, Votes Rejected: %d, Current Term: %d\n`,
		rf.me, votesGranted, votesRejected, rf.currentTerm)

	if hasMajorityGranted {
		rf.updateRole(Leader)
		rf.currentLeader = rf.me
		fmt.Printf("[server_%d::runElection()] Elected as leader for term %d.\n", rf.me, rf.currentTerm)
	}
	rf.mu.Unlock()
}

func (rf *Raft) candidateBehavior() {
	rf.mu.Lock()
	if rf.prevRole == Follower ||
		time.Since(rf.electionResetTs) >= ElectionTimeout+time.Duration(rand.IntN(100)) { // Run election if has just updated or timed out

		rf.prevRole = Candidate // reset transition trigger
		rf.runElection()
	}
	rf.mu.Unlock()
}

func (rf *Raft) leaderBehavior() {
	rf.mu.Lock()
	if rf.prevRole != Leader ||
		time.Since(rf.electionResetTs) >= HeartBeatTimeout {
		rf.prevRole = Leader
		rf.mu.Unlock()

		// send heartbeat
		for serverIdx := range rf.peers {
			if serverIdx == rf.me {
				continue
			}

			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				// PrevLogIndex: len(rf.log) - 1,
				// PrevLogTerm:  rf.log[len(rf.log)-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			if len(rf.log) > 0 {
				args.PrevLogIndex = len(rf.log) - 1
				args.PrevLogTerm = rf.log[len(rf.log)-1].Term
			} else {
				args.PrevLogIndex = -1
				args.PrevLogTerm = -1
			}
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(serverIdx, &args, &reply)
		}

		rf.mu.Lock()
		rf.electionResetTs = time.Now()
	}

	rf.mu.Unlock()
}

// Launches goroutines that perform the server operations
func (rf *Raft) launch() {
	fmt.Printf("[server_%d::launch()] Launching...\n", rf.me)

	// Actions for all server states:
	go rf.applyOp()

	// Actions for specific server states
	go func() {
		for {
			if rf.killed() {
				fmt.Printf("[server_%d::launchOps()] Server killed. Exiting goroutine.\n", rf.me)
				return
			}

			switch rf.currentRole {
			case Follower:
				rf.followerBehavior()
			case Candidate:
				rf.candidateBehavior()
			case Leader:
				rf.leaderBehavior()
			}
		}
	}()
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
	isLeader := true

	// Your code here (2B).

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
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.currentLeader = -1
	rf.commitIndex = -1
	rf.lastApplied = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.launch()

	return rf
}
