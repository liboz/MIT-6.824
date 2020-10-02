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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

const (
	Follower  = iota // 0
	Candidate = iota // 1
	Leader    = iota // 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term         int
	IsSnapshot   bool
	Seen         map[int64]int
	StateSize    int
}

type LogEntry struct {
	Term int
	Data interface{}
}

type AppendEntriesResponse struct {
	Request     AppendEntriesArgs
	Response    AppendEntriesReply
	ServerIndex int
}

type InstallSnapshotResponse struct {
	Request     InstallSnapshotArgs
	Response    InstallSnapshotReply
	ServerIndex int
}

type Snapshot struct {
	LastIncludedTerm  int
	Data              map[string]string
	LastIncludedIndex int
	Seen              map[int64]int
}

type AppendEntriesOrInstallSnapshot struct {
	AppendEntries    map[int]*AppendEntriesArgs
	InstallSnapshots map[int]*InstallSnapshotArgs
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu          sync.RWMutex        // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	currentTerm int                 // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int                 // int of candidate in peers that received vote in currentterm (or -1 if none)
	state       int                 // one of Follower, Candidate, or Leader
	log         []LogEntry          //log entries; each entry contains commandfor state machine, and term when entry was received by leader
	commitIndex int                 // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int                 //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex   []int               //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int               // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	applyCh                   chan ApplyMsg
	appendEntriesResponseCh   chan AppendEntriesResponse
	installSnapshotResponseCh chan InstallSnapshotResponse

	electionTimeout time.Duration // time before timingout election
	lastHeartbeat   time.Time     // Time of last heartbeat
	offset          int
	snapshot        Snapshot
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) encodeSnapshot(snapshot Snapshot) []byte {
	snapshotBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(snapshotBuffer)

	encoder.Encode(snapshot)
	encodedSnapshot := snapshotBuffer.Bytes()
	return encodedSnapshot
}

func (rf *Raft) encodeData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	data := rf.encodeData()
	DPrintf("persisting on server %d; term %d; votedFor %d; log: %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshotData []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	} else {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		var currentTerm int
		var votedFor int
		var savedLog []LogEntry
		snapshotR := bytes.NewBuffer(snapshotData)
		snapshotD := labgob.NewDecoder(snapshotR)
		var snapshot Snapshot
		snapshotD.Decode(&snapshot) // ignore error when snapshot doesn't exist

		if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&savedLog) != nil {
			DPrint("error reading persisted")
		} else {
			DPrint("persistedLog on server ", rf.me, " is:", currentTerm, votedFor, savedLog, snapshot)
			rf.currentTerm = currentTerm
			rf.votedFor = votedFor
			rf.log = savedLog
			rf.snapshot = snapshot
			rf.offset = snapshot.LastIncludedIndex // 0 when snapshot is undefined
		}
	}
}

func (rf *Raft) persistSnapshot(snapshot Snapshot) {
	data := rf.encodeData()
	encodedSnapshot := rf.encodeSnapshot(snapshot)
	rf.persister.SaveStateAndSnapshot(data, encodedSnapshot)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // index in peers of candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // index in peers of candidate requesting vote
	PrevLogIndex int        // index of log entry immediately preceding new ones, 1 indexed
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm for leader to update itself
	Success bool // true if follower contained entry matchingprevLogIndex and prevLogTerm
	XTerm   int
	XIndex  int
	XLength int
}

type InstallSnapshotArgs struct {
	Term     int      // leader's term
	LeaderId int      // index in peers of candidate requesting vote
	Snapshot Snapshot // snapshot data
}

type InstallSnapshotReply struct {
	Term int // currentTerm for leader to update itself
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term < rf.currentTerm {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}

		lastLogIndex := rf.effectiveLogLength()
		lastLogEntryTerm := 0
		if lastLogIndex > 0 {
			logEntry, snapshot, isLogEntry := rf.getLogEntryOrSnapshot(lastLogIndex - 1)
			if isLogEntry {
				lastLogEntryTerm = logEntry.Term
			} else {
				lastLogEntryTerm = snapshot.LastIncludedTerm
			}
		}
		if args.Term > rf.currentTerm {
			// update current term and remove votedFor and convert to follower
			DPrint("Server ", rf.me, ": follower and updating term from ", args.Term, " compared to current term of ", rf.currentTerm)
			rf.stepDown(args.Term)
		}

		log.Print("i am server ", rf.me, " and I got a request from ", args.CandidateId, " for term ", args.Term, ". ", args.LastLogTerm, lastLogEntryTerm, args.LastLogIndex, lastLogIndex)
		isRequestedLogHigherTerm := args.LastLogTerm > lastLogEntryTerm
		isRequestedLogSameTermAndLonger := args.LastLogTerm == lastLogEntryTerm && args.LastLogIndex >= lastLogIndex
		if (isRequestedLogHigherTerm || isRequestedLogSameTermAndLonger) &&
			(rf.votedFor == -1 || args.CandidateId == rf.votedFor) {
			rf.votedFor = args.CandidateId
			rf.lastHeartbeat = time.Now()
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			log.Print("Granting server ", args.CandidateId, " the vote from server ", rf.me)
			rf.persist()
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
		return
	}
}

func (rf *Raft) convertToFollower() {
	if rf.state != Follower {
		DPrint("Server id ", rf.me, " got converted from ", rf.state, " to Follower")
	}
	rf.state = Follower
}

func (rf *Raft) makeXLengthResponse(reply *AppendEntriesReply, xLength int) {
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLength = xLength
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term < rf.currentTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}

		if len(args.Entries) > 0 {
			log.Print("AppendEntries received by server ", rf.me, " from server ", args.LeaderId, ". ", args.PrevLogIndex, rf.log, args.Entries)
		}
		rf.lastHeartbeat = time.Now()
		if args.PrevLogIndex != 0 {
			// longer than current log
			effectiveLength := rf.effectiveLogLength()
			if args.PrevLogIndex > effectiveLength {
				rf.makeXLengthResponse(reply, effectiveLength)
				return
			} else if rf.snapshot.Data != nil && args.PrevLogIndex < rf.snapshot.LastIncludedIndex {
				rf.makeXLengthResponse(reply, rf.snapshot.LastIncludedIndex)
				return
			}

			// term we have stored at the previous log index doesn't match the one from recevied request
			logEntry, snapshot, isLogEntry := rf.getLogEntryOrSnapshot(args.PrevLogIndex - 1)
			var termStoredAtPrevLogIndex int
			if isLogEntry {
				termStoredAtPrevLogIndex = logEntry.Term
			} else {
				termStoredAtPrevLogIndex = snapshot.LastIncludedTerm
			}

			if termStoredAtPrevLogIndex != args.PrevLogTerm {
				reply.Term = rf.currentTerm
				reply.Success = false
				reply.XTerm = termStoredAtPrevLogIndex
				if isLogEntry {
					reply.XIndex = rf.findFirstInLog(logEntry.Term)
				} else {
					reply.XIndex = snapshot.LastIncludedIndex
				}
				reply.XLength = -1

				return
			}
		}

		// consider the case when leader has not received a single response from follower, but follower has already snapshotted
		if rf.offset > 0 && args.PrevLogIndex < rf.offset {
			rf.makeXLengthResponse(reply, rf.snapshot.LastIncludedIndex)
			return
		}

		if args.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.currentTerm = args.Term
		}

		if len(args.Entries) != 0 {
			slice := rf.log[:args.PrevLogIndex-rf.offset]
			rf.log = append(slice, args.Entries...)
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.effectiveLogLength())
		}
		rf.persist()

		rf.convertToFollower()

		reply.Term = args.Term
		reply.Success = true
		//DPrint("server ", rf.me, " responded with a success message to server ", args.LeaderId)

		return
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		log.Printf("%d: received install snapshot request from %d for term %d. current term is %d; offset is %d; loglength is %d; lastIncludedIndex is %d; current log is %v; snapshot data is %v",
			rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.offset, len(rf.log), args.Snapshot.LastIncludedIndex, rf.log, args.Snapshot)

		if args.Term < rf.currentTerm {
			reply.Term = rf.currentTerm
			return
		}
		lastIncludedIndex := args.Snapshot.LastIncludedIndex

		rf.lastHeartbeat = time.Now()
		if lastIncludedIndex > rf.snapshot.LastIncludedIndex && lastIncludedIndex < rf.effectiveLogLength() {
			rf.log = rf.log[lastIncludedIndex-rf.offset:]
		} else {
			rf.log = []LogEntry{}
		}
		rf.snapshot = args.Snapshot
		rf.offset = rf.snapshot.LastIncludedIndex

		rf.commitIndex = max(rf.commitIndex, rf.snapshot.LastIncludedIndex)
		rf.currentTerm = args.Term

		rf.persistSnapshot(args.Snapshot)
		rf.convertToFollower()

		reply.Term = args.Term
		log.Printf("%d: handled install snapshot request from %d for term %d. current term is %d; offset is %d; loglength is %d; current log is %v",
			rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.offset, len(rf.log), rf.log)
		return
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrint("Sending request for vote from id ", rf.me, " to id ", server, " for term ", args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		//DPrint("Some odd crap happened and the request for vote failed", args, reply)
	} else {
		DPrint("Received a reply to request for vote from ", server, " as ", rf.me, ": ", reply)
	}
	return ok
}

func (rf *Raft) becomeCandidate() {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	rf.currentTerm += 1
	DPrint("Server ", rf.me, " becoming candidate for term ", rf.currentTerm)
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.electionTimeout = generateElectionTimeOut()
	rf.lastHeartbeat = time.Now()
	currentTerm := rf.currentTerm
	lastLogTerm := 0
	lastLogIndex := 0
	effectiveLogLength := rf.effectiveLogLength()
	if effectiveLogLength > 0 {
		logEntry, snapshot, isLogEntry := rf.getLogEntryOrSnapshot(effectiveLogLength - 1)
		if isLogEntry {
			lastLogTerm = logEntry.Term
		} else {
			lastLogTerm = snapshot.LastIncludedTerm
		}
		lastLogIndex = effectiveLogLength
	}
	rf.persist()
	rf.mu.Unlock()
	resultChannel := make(chan *RequestVoteReply)
	for serverIndex := range rf.peers {
		if serverIndex != rf.me {
			go func(serverIndex int) {
				args := &RequestVoteArgs{}
				args.Term = currentTerm
				args.CandidateId = rf.me
				args.LastLogTerm = lastLogTerm
				args.LastLogIndex = lastLogIndex
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(serverIndex, args, reply)
				if ok {
					resultChannel <- reply
				}
			}(serverIndex)
		}
	}

	votesRequired := (len(rf.peers) - 1) / 2

	for {
		select {
		case reply := <-resultChannel:
			rf.mu.RLock()
			if rf.state != Candidate {
				rf.mu.RUnlock()
				// abort if we've converted to something else already (i.e. follower)
				return
			}
			currentTerm := rf.currentTerm
			rf.mu.RUnlock()
			if reply.VoteGranted {
				votesRequired -= 1
			} else if reply.Term > currentTerm {
				// abort trying to be leader
				rf.mu.Lock()
				rf.stepDown(reply.Term)
				rf.mu.Unlock()
				DPrint("Another server has a higher term index ", reply.Term, "than so server id ", rf.me, " is aborting")
				return
			}
			if (votesRequired) == 0 {
				rf.mu.Lock()
				rf.state = Leader
				rf.initializeMatchAndNextIndex()
				requests := rf.makeAppendEntriesOrInstallSnapshotRequests()
				log.Printf("Enough servers have voted for index %d. Becoming leader for term %d", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				rf.sendAppendEntriesOrInstallSnapshotToAll(requests)
				return
			}
		case <-time.After(rf.electionTimeout):
			rf.mu.RLock()
			if rf.state == Candidate {
				rf.mu.RUnlock()
				rf.becomeCandidate()
				return
			} else {
				rf.mu.RUnlock()
				return
			}
		}
	}
}

func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = Follower
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//DPrint("Sending appendEntries request from id ", rf.me, " to id ", server, " for term ", args.Term)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		//DPrint("Some odd crap happened and the appendEntries failed", args, reply)
	}

	//DPrint("Received appendEntries response to message from id ", rf.me, " to id ", server, " for term ", args.Term, ". status: ", ok)

	return ok
}

func (rf *Raft) makeSnapshotArgs(currentTerm int) *InstallSnapshotArgs {
	r := bytes.NewBuffer(rf.persister.ReadSnapshot())
	d := labgob.NewDecoder(r)
	var snapshotCopy Snapshot
	d.Decode(&snapshotCopy)
	installSnapshotArgs := &InstallSnapshotArgs{}
	installSnapshotArgs.Term = currentTerm
	installSnapshotArgs.LeaderId = rf.me
	installSnapshotArgs.Snapshot = snapshotCopy
	return installSnapshotArgs
}

func (rf *Raft) makeAppendEntriesOrInstallSnapshotRequests() AppendEntriesOrInstallSnapshot {
	currentTerm := rf.currentTerm
	leaderCommit := rf.commitIndex
	requests := AppendEntriesOrInstallSnapshot{}
	requests.AppendEntries = make(map[int]*AppendEntriesArgs)
	requests.InstallSnapshots = make(map[int]*InstallSnapshotArgs)

	for serverIndex := range rf.peers {
		if serverIndex != rf.me {
			args := &AppendEntriesArgs{}
			args.Term = currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[serverIndex] - 1
			DPrintf("%d: PrevLogIndex sent to %d was %d, snapshot last index %d", rf.me, serverIndex, args.PrevLogIndex, rf.snapshot.LastIncludedIndex)
			args.Term = currentTerm
			if args.PrevLogIndex < rf.snapshot.LastIncludedIndex {
				installSnapshotArgs := rf.makeSnapshotArgs(currentTerm)
				requests.InstallSnapshots[serverIndex] = installSnapshotArgs
				continue
			}

			if args.PrevLogIndex == 0 {
				args.PrevLogTerm = -1
			} else {
				logEntry, snapshot, isLogEntry := rf.getLogEntryOrSnapshot(args.PrevLogIndex - 1)
				if isLogEntry {
					args.PrevLogTerm = logEntry.Term
				} else {
					args.PrevLogTerm = snapshot.LastIncludedTerm
				}
			}

			if args.PrevLogIndex < rf.effectiveLogLength() {
				entries := make([]LogEntry, len(rf.log[args.PrevLogIndex-rf.offset:]))
				copy(entries, rf.log[args.PrevLogIndex-rf.offset:])
				args.Entries = entries
				//log.Printf("%d: sending to %d the entries %v", rf.me, serverIndex, args.Entries)
			}

			args.LeaderCommit = leaderCommit
			requests.AppendEntries[serverIndex] = args
		}
	}
	return requests
}

func (rf *Raft) sendAppendEntriesOrInstallSnapshotToAll(requests AppendEntriesOrInstallSnapshot) {
	for serverIndex, request := range requests.AppendEntries {
		go func(serverIndex int, request *AppendEntriesArgs) {
			reply := &AppendEntriesReply{}
			fullResponse := AppendEntriesResponse{}
			fullResponse.ServerIndex = serverIndex
			fullResponse.Request = *request
			ok := rf.sendAppendEntries(serverIndex, request, reply)

			fullResponse.Response = *reply
			if ok {
				rf.appendEntriesResponseCh <- fullResponse
			}
		}(serverIndex, request)
	}
	for serverIndex, request := range requests.InstallSnapshots {
		go func(serverIndex int, request *InstallSnapshotArgs) {
			reply := &InstallSnapshotReply{}
			fullResponse := InstallSnapshotResponse{}
			fullResponse.ServerIndex = serverIndex
			fullResponse.Request = *request
			ok := rf.peers[serverIndex].Call("Raft.InstallSnapshot", request, reply)

			fullResponse.Response = *reply
			if ok {
				rf.installSnapshotResponseCh <- fullResponse
			}
		}(serverIndex, request)
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrint("Server ", rf.me, " received command ", command)
	index := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if !isLeader {
		return index, term, isLeader
	}
	DPrint("Starting consensus for ", command, " on server ", rf.me)

	logEntry := LogEntry{}
	logEntry.Data = command
	logEntry.Term = term

	rf.log = append(rf.log, logEntry)
	index = rf.effectiveLogLength()
	rf.matchIndex[rf.me] = index
	rf.persist()

	return index, term, isLeader
}

// Snapshots only contain entries that are already committed
// lastIncludedIndex is 1 indexed
func (rf *Raft) SaveSnapshot(snapshot map[string]string, lastIncludedIndex int, lastIncludedTerm int, seen map[int64]int) {
	if !rf.killed() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		log.Printf("Server %d received with raftsize %d; command with offset %d; snapshot %v; log with length %d: %v", rf.me, rf.persister.RaftStateSize(), rf.offset, rf.snapshot, len(rf.log), rf.log)

		if lastIncludedIndex < rf.snapshot.LastIncludedIndex {
			// out of order transmission
			return
		}
		storedSnapshot := Snapshot{}
		storedSnapshot.LastIncludedIndex = lastIncludedIndex
		storedSnapshot.LastIncludedTerm = lastIncludedTerm
		storedSnapshot.Data = snapshot
		storedSnapshot.Seen = seen

		log.Printf("%d: lastIncluded: %d; log length %d; offset %d; effectiveLogLength: %d", rf.me, lastIncludedIndex, len(rf.log), rf.offset, rf.effectiveLogLength())
		if lastIncludedIndex < rf.effectiveLogLength() {
			rf.log = rf.log[lastIncludedIndex-rf.offset:]
		} else {
			rf.log = []LogEntry{}
		}
		rf.snapshot = storedSnapshot
		rf.offset = lastIncludedIndex

		rf.persistSnapshot(storedSnapshot)

		log.Printf("Server %d now has raftsize %d; offset %d; snapshot %v; log with length %d: %v", rf.me, rf.persister.RaftStateSize(), rf.offset, rf.snapshot, len(rf.log), rf.log)
	}
}

func (rf *Raft) effectiveLogLength() int {
	return len(rf.log) + rf.offset
}

// bool represents if it is a LogEntry
func (rf *Raft) getLogEntryOrSnapshot(index int) (LogEntry, Snapshot, bool) {
	// it must be contained in the snapshot
	//log.Printf("%d: index %d, offset %d, loglen: %d, snapshot last %d", rf.me, index, rf.offset, len(rf.log), rf.snapshot.LastIncludedIndex)
	if index < rf.offset {
		return LogEntry{}, rf.snapshot, false
	} else {
		return rf.log[index-rf.offset], Snapshot{}, true
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrint("server id ", rf.me, " has been killed")
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) loopMaybeBecomeCandidate() {
	for !rf.killed() {
		rf.mu.RLock()
		nextElectionTime := rf.lastHeartbeat.Add(rf.electionTimeout)
		if rf.state == Follower && time.Now().After(nextElectionTime) {
			rf.mu.RUnlock()
			rf.becomeCandidate()
		} else {
			rf.mu.RUnlock()
		}
		time.Sleep(100 * time.Millisecond)

	}
}

func (rf *Raft) loopAppendEntries() {
	for !rf.killed() {
		rf.mu.RLock()
		if rf.state == Leader {
			requests := rf.makeAppendEntriesOrInstallSnapshotRequests()
			rf.mu.RUnlock()
			rf.sendAppendEntriesOrInstallSnapshotToAll(requests)
		} else {
			rf.mu.RUnlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendToApplyCh() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			for rf.commitIndex > rf.lastApplied {
				msg := ApplyMsg{}
				logEntry, snapshot, isLogEntry := rf.getLogEntryOrSnapshot(rf.lastApplied)
				if isLogEntry {
					msg.Command = logEntry.Data
					msg.Term = logEntry.Term
					msg.IsSnapshot = false
					msg.CommandIndex = rf.lastApplied + 1
				} else {
					msg.Command = snapshot.Data
					msg.Term = snapshot.LastIncludedTerm
					msg.IsSnapshot = true
					msg.CommandIndex = snapshot.LastIncludedIndex
					msg.Seen = snapshot.Seen
				}
				DPrintf("sending from server %d; entry : %v; commitIndex: %d; lastAppliedIndex: %d", rf.me, msg.Command, rf.commitIndex, rf.lastApplied+1)
				msg.CommandValid = true
				msg.StateSize = rf.persister.RaftStateSize()
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
				rf.lastApplied = msg.CommandIndex
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) listenToAppendEntriesResponseCh() {
	for !rf.killed() {
		fullResponse := <-rf.appendEntriesResponseCh
		rf.mu.Lock()
		//DPrint("server ", rf.me, "is leader?", rf.state == Leader)
		if rf.state == Leader {
			DPrint("Received appendEntries response to request from id ", rf.me, " to id ", fullResponse.ServerIndex, " for term ",
				fullResponse.Request.Term, " response term is ", fullResponse.Response.Term, ". ", fullResponse)

			if !fullResponse.Response.Success {
				if fullResponse.Response.Term > rf.currentTerm {
					// convert to follower
					log.Print("Server ", rf.me, ": stepping down as AppendEntries Response had term ", fullResponse.Response.Term, " compared to current term of ", rf.currentTerm)
					rf.stepDown(fullResponse.Response.Term)
					rf.mu.Unlock()
					continue
				} else if rf.nextIndex[fullResponse.ServerIndex] != 1 { // follower's log is out of sync with the leader
					DPrint("updating nextIndex for server ", fullResponse.ServerIndex, " from ", rf.nextIndex[fullResponse.ServerIndex], " as we got the response ", fullResponse.Response)
					lastIndex := rf.findLastInLog(fullResponse.Response.XTerm)
					if fullResponse.Response.XLength != -1 {
						rf.nextIndex[fullResponse.ServerIndex] = max(1, fullResponse.Response.XLength)
					} else if lastIndex != -1 {
						rf.nextIndex[fullResponse.ServerIndex] = lastIndex // min value of 1
					} else {
						rf.nextIndex[fullResponse.ServerIndex] = fullResponse.Response.XIndex // min value of 1
					}
					DPrint("nextIndex for server ", fullResponse.ServerIndex, " was updated to ", rf.nextIndex[fullResponse.ServerIndex], fullResponse.Response)

				}
			} else {
				newLengthFromRequest := len(fullResponse.Request.Entries) + fullResponse.Request.PrevLogIndex
				DPrint("updating matchIndex ", rf.matchIndex[fullResponse.ServerIndex], " and nextIndex ", rf.nextIndex[fullResponse.ServerIndex], " for server ", fullResponse.ServerIndex, " to ", newLengthFromRequest)
				rf.matchIndex[fullResponse.ServerIndex] = max(rf.matchIndex[fullResponse.ServerIndex], newLengthFromRequest)
				rf.nextIndex[fullResponse.ServerIndex] = max(rf.nextIndex[fullResponse.ServerIndex], newLengthFromRequest+1)
			}
			rf.maybeUpdateCommitIndex()
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) listenToInstallSnapshotResponseCh() {
	for !rf.killed() {
		fullResponse := <-rf.installSnapshotResponseCh
		rf.mu.Lock()
		if rf.state == Leader {
			log.Printf("%d: Received installSnapshot response from id %d; original term %d; response term %d; response %v",
				rf.me, fullResponse.ServerIndex, fullResponse.Request.Term, fullResponse.Response.Term, fullResponse)

			if fullResponse.Response.Term > rf.currentTerm {
				// convert to follower
				log.Printf("%d: stepping down as InstallSnapshot Response had term %d compared to current term of %d", rf.me, fullResponse.Response.Term, rf.currentTerm)
				rf.stepDown(fullResponse.Response.Term)
				rf.mu.Unlock()
				continue

			} else {
				// success!
				newLengthFromRequest := fullResponse.Request.Snapshot.LastIncludedIndex
				log.Printf("%d: updating matchIndex %d and nextIndex %d for server %d to %d", rf.me, rf.matchIndex[fullResponse.ServerIndex], rf.nextIndex[fullResponse.ServerIndex], fullResponse.ServerIndex, newLengthFromRequest)
				rf.matchIndex[fullResponse.ServerIndex] = max(rf.matchIndex[fullResponse.ServerIndex], newLengthFromRequest)
				rf.nextIndex[fullResponse.ServerIndex] = max(rf.nextIndex[fullResponse.ServerIndex], newLengthFromRequest+1)
			}
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) maybeUpdateCommitIndex() {
	originalCommitIndex := rf.commitIndex
	DPrint("updating commitIndex maybe", originalCommitIndex, rf.matchIndex, ". log is ", rf.log)
	for N := originalCommitIndex + 1; N <= rf.effectiveLogLength(); N++ {
		logEntry, snapshot, isLogEntry := rf.getLogEntryOrSnapshot(N - 1)
		var term int
		if isLogEntry {
			term = logEntry.Term
		} else {
			term = snapshot.LastIncludedTerm
		}

		if term != rf.currentTerm {
			continue
		}
		votesRequired := len(rf.peers)/2 + 1
		DPrint("N: ", N, ". term at log is ", term, ". current term is", rf.currentTerm, rf.matchIndex)
		for serverIndex := range rf.peers {
			if rf.matchIndex[serverIndex] >= N {
				votesRequired -= 1
			}
			if votesRequired == 0 {
				DPrint("commit index updated to ", N)
				rf.commitIndex = N
				break
			}
		}
	}
}

func (rf *Raft) findFirstInLog(targetTerm int) int {
	if rf.snapshot.Data != nil && targetTerm == rf.snapshot.LastIncludedTerm {
		return rf.snapshot.LastIncludedIndex
	}
	for i, logEntry := range rf.log {
		if logEntry.Term == targetTerm {
			return i + rf.offset + 1 // 1 based indexing
		}
	}
	panic(fmt.Sprintf("found nothing with term %d in log %v and snapshot %v", targetTerm, rf.log, rf.snapshot))
}

func (rf *Raft) findLastInLog(targetTerm int) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == targetTerm {
			return i + rf.offset + 1 // 1 based indexing
		}
	}
	if rf.snapshot.Data != nil && targetTerm <= rf.snapshot.LastIncludedTerm {
		return rf.snapshot.LastIncludedIndex
	} else {
		return -1
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func generateElectionTimeOut() time.Duration {
	return time.Duration(rand.Intn(1500-500)+500) * time.Millisecond
}

func (rf *Raft) initializeMatchAndNextIndex() {
	for i := range rf.peers {
		rf.nextIndex[i] = rf.effectiveLogLength() + 1
		rf.matchIndex[i] = 0
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.electionTimeout = generateElectionTimeOut()
	rf.log = []LogEntry{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.offset = 0
	rf.initializeMatchAndNextIndex()
	rf.appendEntriesResponseCh = make(chan AppendEntriesResponse)
	rf.installSnapshotResponseCh = make(chan InstallSnapshotResponse)
	//DPrint("Initialize server id ", rf.me, " with electionTimeout ", rf.electionTimeout)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// Your initialization code here (2A, 2B, 2C).
	go func() {
		rf.loopMaybeBecomeCandidate()
	}()

	go func() {
		rf.loopAppendEntries()
	}()

	go func() {
		rf.sendToApplyCh()
	}()

	go func() {
		rf.listenToAppendEntriesResponseCh()
	}()

	go func() {
		rf.listenToInstallSnapshotResponseCh()
	}()

	return rf
}
