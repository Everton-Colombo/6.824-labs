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
	"io"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"mc714/src/labgob"
	"mc714/src/labrpc"
)

var debugLog = log.New(io.Discard, "", 0)

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

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
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
const RandTimeoutRange = 200
const HeartBeatTimeout = 150 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	applyCond *sync.Cond
	peers     []labrpc.RPCClient //  // FollowerRPC end points of all peers
	persister *Persister         // Object to hold this peer's persisted state
	me        int                // this peer's index into peers[]
	dead      int32              // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int
	votedFor    int // -1 for when votedFor = null
	log         []LogEntry
	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	prevRole        ServerRole
	currentRole     ServerRole // 0: Follower; 1: Candidate; 2: Leader
	leaderID        int        // last known leader, -1 if unknown
	electionResetTs time.Time

	// Volatile state on leaders:
	nextIndex  []int // for each server, idx of the next log entry to send to that server
	matchIndex []int // for each server, idx of the highest log entry known to be replicated on server

	trimmedEntriesCount int
	pendingSnapshot     *ApplyMsg
}

func (rf *Raft) updateRole(newRole ServerRole) {
	rf.prevRole = rf.currentRole
	rf.currentRole = newRole
}

func (rf *Raft) resetElectionTimer() {
	jitter := time.Duration(rand.Intn(RandTimeoutRange)) * time.Millisecond
	rf.electionResetTs = time.Now().Add(jitter)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// (2A)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.currentRole == Leader
}

// Status is a point-in-time view of a peer, for monitoring and experiments.
type Status struct {
	Term           int
	IsLeader       bool
	LeaderID       int
	CommitIndex    int
	LastLogIndex   int
	LastApplied    int
	RaftStateBytes int
	SnapshotBytes  int
}

func (rf *Raft) Status() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return Status{
		Term:           rf.currentTerm,
		IsLeader:       rf.currentRole == Leader,
		LeaderID:       rf.leaderID,
		CommitIndex:    rf.commitIndex,
		LastLogIndex:   rf.lastLogIndex(),
		LastApplied:    rf.lastApplied,
		RaftStateBytes: rf.persister.RaftStateSize(),
		SnapshotBytes:  rf.persister.SnapshotSize(),
	}
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.trimmedEntriesCount)
	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeState())
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var trimmedEntriesCount int
	if d.Decode(&currentTerm) == nil &&
		d.Decode(&votedFor) == nil &&
		d.Decode(&log) == nil &&
		d.Decode(&trimmedEntriesCount) == nil {

		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.trimmedEntriesCount = trimmedEntriesCount
		rf.mu.Unlock()
	}
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
	debugLog.Printf("[%d::RequestVote()] Received RequestVote: Term=%d, CandidateId=%d, LastLogIndex=%d, LastLogTerm=%d\n", rf.me, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("[%d::RequestVote()] Passed lock!\n", rf.me)

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderID = -1
		rf.persist()
		rf.updateRole(Follower)
		debugLog.Printf("[%d::RequestVote()] Updated term to %d. Converted to follower.\n", rf.me, args.Term)
	}

	if args.Term >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		myLastLogTerm := -1
		if len(rf.log) > 0 {
			myLastLogTerm = rf.log[len(rf.log)-1].Term
		}
		myLastLogIndex := rf.lastLogIndex()

		candidateUpToDate := args.LastLogTerm > myLastLogTerm ||
			(args.LastLogTerm == myLastLogTerm && args.LastLogIndex >= myLastLogIndex)

		if candidateUpToDate {
			reply.VoteGranted = true
			rf.resetElectionTimer()
			rf.votedFor = args.CandidateId
			rf.persist()
			debugLog.Printf("[%d::RequestVote()] Granted vote for %d\n", rf.me, args.CandidateId)
			return
		}
	}

	debugLog.Printf("[%d::RequestVote()] Denied vote for %d\n", rf.me, args.CandidateId)
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
	PeerTerm int
	Success  bool
	XTerm    int // term of the conflicting entry (if any)
	XIndex   int // index of the first entry with that term (if any)
	XLen     int // length of the follower's log
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

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// Rejected due to stale term
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderID = -1
		rf.persist()
		rf.updateRole(Follower)
	}

	rf.leaderID = args.LeaderId
	rf.resetElectionTimer()

	if rf.pendingSnapshot == nil || args.LastIncludedIndex > rf.pendingSnapshot.SnapshotIndex {
		rf.pendingSnapshot = &ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}
	}
	rf.applyCond.Broadcast()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	debugLog.Printf("[%d::AppendEntries()] Received AppendEntries RPC from leader %d for term %d.\n", rf.me, args.LeaderId, args.Term)
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = rf.lastLogIndex() + 1

	reply.PeerTerm = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderID = -1
		rf.persist()

		if rf.currentRole != Follower {
			rf.updateRole(Follower)
			debugLog.Printf("[%d::AppendEntries()] Updated term to %d. Converted to follower.\n", rf.me, args.Term)
		}
	}

	if args.Term < rf.currentTerm {
		// Rejected due to stale term
		debugLog.Printf("[%d::AppendEntries()] Rejected AE.\n", rf.me)
		reply.Success = false
		return
	}

	rf.leaderID = args.LeaderId
	rf.resetElectionTimer()

	prevIndex := rf.toLogIdx(args.PrevLogIndex)
	missingPrev := args.PrevLogIndex > rf.lastLogIndex() || (args.PrevLogIndex >= 0 && prevIndex < 0)
	if missingPrev || (args.PrevLogIndex >= 0 && rf.log[prevIndex].Term != args.PrevLogTerm) {
		// Rejected due to log incosistency.

		debugLog.Printf("[%d::AppendEntries()] Rejected AE.\n", rf.me)
		reply.Success = false

		if missingPrev {
			reply.XTerm = -1
			reply.XIndex = -1
		} else {
			reply.XTerm = rf.log[prevIndex].Term
			for idx, entry := range rf.log {
				if entry.Term == reply.XTerm {
					reply.XIndex = idx + rf.trimmedEntriesCount
					break
				}
			}
		}
		reply.XLen = rf.lastLogIndex() + 1

		return
	}

	// Append entries from prevlogindex
	if len(args.Entries) != 0 {
		var i, aeIdx int
		for i, aeIdx = args.PrevLogIndex+1, 0; i <= rf.lastLogIndex() && aeIdx < len(args.Entries); i, aeIdx = i+1, aeIdx+1 {
			if rf.log[rf.toLogIdx(i)] != args.Entries[aeIdx] {
				rf.log = rf.log[:rf.toLogIdx(i)]
				break
			}
		}
		args.Entries = args.Entries[aeIdx:]
	}

	rf.log = append(rf.log, args.Entries...)
	rf.persist()

	// Update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
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
	debugLog.Printf("[%d::sendRequestVote()] Sending RequestVote RPC to server %d for term %d.\n", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	debugLog.Printf("[%d::sendAppendEntries()] Sending AppendEntries RPC to server %d for term %d.\n", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	debugLog.Printf("[%d::sendInstallSnapshot()] Sending InstallSnapshot RPC to server %d for term %d.\n", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// Function that implements the operations related to applying commited messages.
// Should be launched as a separate goroutine from the launchOps function()
func (rf *Raft) applyOp() {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()

	for {
		if rf.killed() {
			debugLog.Printf("[%d::applyOp()] Server killed. Exiting goroutine.\n", rf.me)
			return
		}

		// Collect the messages to send under the lock, then send without
		// holding rf.mu so the service can call back into raft (Snapshot,
		// CondInstallSnapshot) without deadlocking. Do not touch rf.log or
		// rf.commitIndex again after releasing the lock: a concurrent
		// AppendEntries may truncate the log.
		var msgs []ApplyMsg
		if rf.pendingSnapshot != nil {
			msgs = append(msgs, *rf.pendingSnapshot)
			rf.pendingSnapshot = nil
		} else if rf.commitIndex > rf.lastApplied {
			debugLog.Printf("[%d::applyOp()] Applying log entries from index %d to %d.\n", rf.me, rf.lastApplied+1, rf.commitIndex)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msgs = append(msgs, ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.toLogIdx(i)].Command,
					CommandIndex: i,
				})
			}
			rf.lastApplied = rf.commitIndex
		}

		if len(msgs) == 0 {
			rf.applyCond.Wait()
			continue
		}

		rf.applyCond.L.Unlock()
		for _, m := range msgs {
			rf.applyCh <- m
		}
		rf.applyCond.L.Lock()
	}
}

func (rf *Raft) followerBehavior() {
	rf.mu.Lock()
	if rf.electionResetTs.IsZero() {
		debugLog.Printf("[%d::followerBehavior()] Election reset timestamp is zero. Setting to current time.\n", rf.me)
		rf.resetElectionTimer()
	}

	if time.Since(rf.electionResetTs) >= ElectionTimeout {
		debugLog.Printf("[%d::followerBehavior()] Election timeout. Transitioning to candidate role.\n", rf.me)
		rf.updateRole(Candidate)
	}
	rf.mu.Unlock()

	time.Sleep(50 * time.Millisecond)
}

func (rf *Raft) runElection() {
	rf.mu.Lock()
	rf.currentTerm++
	electionTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.leaderID = -1
	rf.persist()
	rf.resetElectionTimer()
	rf.mu.Unlock()

	replyChan := make(chan *RequestVoteReply, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		args := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
			// LastLogIndex: len(rf.log) - 1,
			// LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		args.LastLogIndex = rf.lastLogIndex()
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
		rf.mu.Unlock()

		go func(serverIdx int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(serverIdx, &args, &reply)
			if ok {
				replyChan <- &reply
			}
		}(i)
	}

	hasMajorityGranted := false
	hasMajorityRejected := false
	votesGranted := 1 // Start counting own vote
	votesRejected := 0
	for !hasMajorityGranted && !hasMajorityRejected {
		select {
		case reply := <-replyChan:
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				rf.updateRole(Follower)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if reply.VoteGranted {
				votesGranted++
			} else {
				votesRejected++
			}
		default:
			rf.mu.Lock()
			if rf.currentRole != Candidate || rf.currentTerm != electionTerm {
				debugLog.Printf("[%d::runElection()] No longer a candidate or term changed. Exiting election.\n", rf.me)
				rf.mu.Unlock()
				return
			}
			if votesGranted > len(rf.peers)/2 {
				hasMajorityGranted = true
			} else if votesRejected > len(rf.peers)/2 {
				hasMajorityRejected = true
			}
			if time.Since(rf.electionResetTs) >= ElectionTimeout {
				debugLog.Printf("[%d::runElection()] Election timeout.\n", rf.me)
				rf.resetElectionTimer()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			time.Sleep(50 * time.Millisecond)
		}
	}

	debugLog.Printf("[%d::runElection()] Election finished. Votes Granted: %d, Votes Rejected: %d, Current Term: %d\n",
		rf.me, votesGranted, votesRejected, electionTerm)

	rf.mu.Lock()
	rf.resetElectionTimer()
	if hasMajorityGranted && rf.currentRole == Candidate && rf.currentTerm == electionTerm {
		rf.updateRole(Leader)
		rf.leaderID = rf.me
		// Reinitialize leader state
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.nextIndex[i] = rf.lastLogIndex() + 1
			rf.matchIndex[i] = -1
		}

		// Commit no-op
		// rf.log = append(rf.log, LogEntry{
		// 	Term:    rf.currentTerm,
		// 	Command: nil,
		// })
		// rf.commitIndex = len(rf.log) - 1
		rf.applyCond.Broadcast()

		debugLog.Printf("[%d::runElection()] Elected as leader for term %d.\n", rf.me, rf.currentTerm)
	}
	rf.mu.Unlock()
}

func (rf *Raft) candidateBehavior() {
	rf.mu.Lock()
	if rf.prevRole == Follower ||
		time.Since(rf.electionResetTs) >= ElectionTimeout {
		rf.prevRole = Candidate // reset transition trigger
		// Run election if has just updated or timed out
		rf.mu.Unlock()
		debugLog.Printf("[%d::candidateBehavior()] Starting election for term %d.\n", rf.me, rf.currentTerm+1)
		rf.runElection()
	} else {
		rf.mu.Unlock()
	}

}

func (rf *Raft) leaderBehavior() {
	rf.mu.Lock()
	if rf.prevRole != Leader ||
		time.Since(rf.electionResetTs) >= HeartBeatTimeout {

		debugLog.Printf("[%d::leaderBehavior()] Sending heartbeat for term %d.\n", rf.me, rf.currentTerm)

		rf.prevRole = Leader

		// send heartbeat
		for serverIdx := range rf.peers {
			if serverIdx == rf.me {
				continue
			}
			go rf.replicateTo(serverIdx)
		}
		rf.resetElectionTimer()
	}

	rf.mu.Unlock()
}

// Launches goroutines that perform the server operations
func (rf *Raft) launch() {
	debugLog.Printf("[%d::launch()] Launching...\n", rf.me)

	// Actions for all server states:
	go rf.applyOp()

	// Actions for specific server states
	go func() {
		for {
			if rf.killed() {
				debugLog.Printf("[%d::launchOps()] Server killed. Exiting goroutine.\n", rf.me)
				return
			}

			rf.mu.Lock()
			currentRole := rf.currentRole
			rf.mu.Unlock()

			switch currentRole {
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

func (rf *Raft) handleAeRejection(reply AppendEntriesReply, peerIdx int) {
	if reply.PeerTerm > rf.currentTerm {
		// AE rejected due to stale term. Update term and convert to follower.
		rf.currentTerm = reply.PeerTerm
		rf.votedFor = -1
		rf.persist()
		rf.updateRole(Follower)
		debugLog.Printf("[%d::handleAeRejection()] Updated term to %d. Converted to follower.\n", rf.me, reply.PeerTerm)
	} else {
		// AE rejected due to log inconsistency. Adjust nextIndex based on reply.
		if reply.XTerm != -1 {
			// Inconsistency was conflicting entry at PrevLogIdx
			lastLeaderIdxOfXTerm := -1
			for idx, entry := range rf.log {
				if entry.Term == reply.XTerm {
					lastLeaderIdxOfXTerm = idx + rf.trimmedEntriesCount
				}
			}

			if lastLeaderIdxOfXTerm != -1 {
				// Leader has an entry with term XTerm
				rf.nextIndex[peerIdx] = lastLeaderIdxOfXTerm
			} else {
				rf.nextIndex[peerIdx] = reply.XIndex
			}

		} else {
			// Inconsistency was a shorter client log
			rf.nextIndex[peerIdx] = reply.XLen
		}

		// Never rewind nextIndex below the trimmed prefix; the leader no
		// longer holds those entries and must send a snapshot instead.
		if rf.nextIndex[peerIdx] <= rf.trimmedEntriesCount {
			rf.nextIndex[peerIdx] = rf.trimmedEntriesCount
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	for logIdx := rf.lastLogIndex(); logIdx > rf.commitIndex; logIdx-- {
		if rf.log[rf.toLogIdx(logIdx)].Term != rf.currentTerm {
			// Leaders may not commit entries with older terms (figure 8 safety violation)
			break
		}

		matchCount := 1
		for peerIdx := range rf.peers {
			if peerIdx == rf.me {
				continue
			}

			if rf.matchIndex[peerIdx] >= logIdx {
				matchCount++
			}
		}

		if matchCount > len(rf.peers)/2 {
			rf.commitIndex = logIdx
			rf.applyCond.Broadcast()
			break
		}
	}
}

func (rf *Raft) replicateTo(peerIdx int) {
	rf.mu.Lock()
	if rf.currentRole != Leader {
		debugLog.Printf("[%d::replicateTo()] No longer leader. Exiting goroutine.\n", rf.me)
		rf.mu.Unlock()
		return
	}

	nextIdx := rf.nextIndex[peerIdx]
	term := rf.currentTerm

	// The entries the follower needs have been trimmed: send a snapshot.
	if nextIdx <= rf.trimmedEntriesCount && rf.trimmedEntriesCount > 0 {
		args := InstallSnapshotArgs{
			Term:              term,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.trimmedEntriesCount,
			LastIncludedTerm:  rf.log[0].Term,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()

		reply := InstallSnapshotReply{}
		if !rf.sendInstallSnapshot(peerIdx, &args, &reply) {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentRole != Leader || rf.currentTerm != args.Term {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
			rf.updateRole(Follower)
			return
		}

		rf.matchIndex[peerIdx] = args.LastIncludedIndex
		rf.nextIndex[peerIdx] = args.LastIncludedIndex + 1
		rf.updateCommitIndex()
		return
	}

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	if nextIdx > rf.trimmedEntriesCount {
		args.PrevLogIndex = nextIdx - 1
		args.PrevLogTerm = rf.log[rf.toLogIdx(args.PrevLogIndex)].Term
		args.Entries = append([]LogEntry{}, rf.log[rf.toLogIdx(nextIdx):]...)
	} else {
		// nextIdx == trimmedEntriesCount == 0: follower log is empty.
		args.PrevLogIndex = -1
		args.PrevLogTerm = -1
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	if !rf.sendAppendEntries(peerIdx, &args, &reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentRole != Leader || rf.currentTerm != args.Term {
		return
	}
	if reply.Success {
		rf.matchIndex[peerIdx] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peerIdx] = rf.matchIndex[peerIdx] + 1
	} else {
		rf.handleAeRejection(reply, peerIdx)
	}
	rf.updateCommitIndex()
}

func (rf *Raft) dispatchAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		rf.resetElectionTimer()
		rf.mu.Unlock()

		go rf.replicateTo(i)
	}
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.currentRole == Leader
	if !isLeader {
		return -1, -1, isLeader
	}
	index := rf.lastLogIndex() + 1
	term := rf.currentTerm

	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    term,
	})
	rf.persist()

	go rf.dispatchAppendEntries()

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
func Make(peers []labrpc.RPCClient, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).
	rf.currentRole = Follower
	rf.votedFor = -1
	rf.leaderID = -1
	rf.log = []LogEntry{{Term: 0}} // Initialize with a dummy log entry at index 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.trimmedEntriesCount
	rf.lastApplied = rf.trimmedEntriesCount

	rf.launch()

	return rf
}

func (rf *Raft) toLogIdx(idx int) int {
	return idx - rf.trimmedEntriesCount
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1 + rf.trimmedEntriesCount
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.trimmedEntriesCount || index > rf.lastLogIndex() ||
		index > rf.commitIndex {
		return
	}

	rf.log = append([]LogEntry(nil), rf.log[rf.toLogIdx(index):]...)
	rf.trimmedEntriesCount = index
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

// CondInstallSnapshot is called by the service after Raft pushed a snapshot
// it received from the leader onto applyCh. It returns true only if this
// snapshot is still ahead of the state Raft has already applied (which can
// happen if the service snapshotted or applied past it in the meantime).
func (rf *Raft) CondInstallSnapshot(lastIncludedIndex, lastIncludedTerm int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.trimmedEntriesCount || lastIncludedIndex <= rf.commitIndex {
		return false
	}

	// Keep the suffix past lastIncludedIndex if it agrees with the snapshot;
	// otherwise discard the whole log and adopt the snapshot as the new base.
	li := rf.toLogIdx(lastIncludedIndex)
	if li < len(rf.log) && rf.log[li].Term == lastIncludedTerm {
		rf.log = append([]LogEntry(nil), rf.log[li:]...)
	} else {
		rf.log = []LogEntry{{Term: lastIncludedTerm}}
	}
	rf.trimmedEntriesCount = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	return true
}

func (rf *Raft) RaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.RaftStateSize()
}
