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

	applyCh                 chan ApplyMsg
	appendEntriesResponseCh chan AppendEntriesResponse

	electionTimeout time.Duration // time before timingout election
	lastHeartbeat   time.Time     // Time of last heartbeat
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	DPrintf("persisting on server %d; term %d; votedFor %d; log: %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	} else {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		var currentTerm int
		var votedFor int
		var savedLog []LogEntry
		if d.Decode(&currentTerm) != nil ||
			d.Decode(&votedFor) != nil || d.Decode(&savedLog) != nil {
			DPrint("error reading persisted")
		} else {
			DPrint("persistedLog on server ", rf.me, " is:", currentTerm, votedFor, savedLog)
			rf.currentTerm = currentTerm
			rf.votedFor = votedFor
			rf.log = savedLog
		}
	}
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
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm for leader to update itself
	Success bool // true if follower contained entry matchingprevLogIndex and prevLogTerm
	XTerm   int
	XIndex  int
	XLength int
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

		lastLogIndex := len(rf.log)
		lastLogEntryTerm := 0
		if lastLogIndex > 0 {
			lastLogEntryTerm = rf.log[len(rf.log)-1].Term
		}
		if args.Term > rf.currentTerm {
			// update current term and remove votedFor and convert to follower
			DPrint("Server ", rf.me, ": follower and updating term from ", args.Term, " compared to current term of ", rf.currentTerm)
			rf.stepDown(args.Term)
		}

		DPrint("i am server ", rf.me, " and I got a request from ", args.CandidateId, " for term ", args.Term, ". ", args.LastLogTerm, lastLogEntryTerm, args.LastLogIndex, lastLogIndex)
		isRequestedLogHigherTerm := args.LastLogTerm > lastLogEntryTerm
		isRequestedLogSameTermAndLonger := args.LastLogTerm == lastLogEntryTerm && args.LastLogIndex >= lastLogIndex
		if (isRequestedLogHigherTerm || isRequestedLogSameTermAndLonger) &&
			(rf.votedFor == -1 || args.CandidateId == rf.votedFor) {
			rf.votedFor = args.CandidateId
			rf.lastHeartbeat = time.Now()
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			DPrint("Granting server ", args.CandidateId, " the vote from server ", rf.me)
			rf.persist()
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
		return
	}
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
			DPrint("AppendEntries received by server ", rf.me, " from server ", args.LeaderId, ". ", args.PrevLogIndex, rf.log, args.Entries,
				args.PrevLogIndex != 0 && (args.PrevLogIndex > len(rf.log) || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm))
		}
		rf.lastHeartbeat = time.Now()
		if args.PrevLogIndex != 0 && (args.PrevLogIndex > len(rf.log) || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
			reply.Term = rf.currentTerm
			reply.Success = false
			if args.PrevLogIndex > len(rf.log) {
				reply.XTerm = -1
				reply.XIndex = -1
				reply.XLength = len(rf.log)
			} else {
				reply.XTerm = rf.log[args.PrevLogIndex-1].Term
				reply.XIndex = rf.findFirstInLog(rf.log[args.PrevLogIndex-1].Term)
				reply.XLength = -1
			}

			return
		}

		if args.Term > rf.currentTerm {
			rf.votedFor = -1
			rf.currentTerm = args.Term
		}

		slice := rf.log[0:args.PrevLogIndex]
		rf.log = append(slice, args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		}
		rf.persist()

		if rf.state != Follower {
			DPrint("Server id ", rf.me, " got converted from ", rf.state, " to Follower")
		}
		rf.state = Follower

		reply.Term = args.Term
		reply.Success = true
		//DPrint("server ", rf.me, " responded with a success message to server ", args.LeaderId)

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
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
		lastLogIndex = len(rf.log)
	}
	rf.persist()
	rf.mu.Unlock()
	resultChannel := make(chan *RequestVoteReply)
	for serverIndex, _ := range rf.peers {
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
				rf.mu.Unlock()
				log.Printf("Enough servers have voted for index %d. Becoming leader for term %d", rf.me, rf.currentTerm)
				rf.sendAppendEntriesToAll()
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

func (rf *Raft) makeAppendEntriesRequests() []*AppendEntriesArgs {
	currentTerm := rf.currentTerm
	leaderCommit := rf.commitIndex
	var requests []*AppendEntriesArgs

	for serverIndex, _ := range rf.peers {
		if serverIndex != rf.me {
			args := &AppendEntriesArgs{}
			args.Term = currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[serverIndex] - 1
			if args.PrevLogIndex == 0 {
				args.PrevLogTerm = -1
			} else {
				args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
			}

			if args.PrevLogIndex < len(rf.log) {
				entries := make([]LogEntry, len(rf.log[args.PrevLogIndex:]))
				copy(entries, rf.log[args.PrevLogIndex:])
				args.Entries = entries
			}

			args.LeaderCommit = leaderCommit
			requests = append(requests, args)
		} else {
			requests = append(requests, nil)
		}
	}
	return requests
}

func (rf *Raft) sendAppendEntriesToAll() {
	rf.mu.RLock()
	requests := rf.makeAppendEntriesRequests()
	rf.mu.RUnlock()
	for serverIndex, _ := range rf.peers {
		if serverIndex != rf.me {
			go func(serverIndex int) {
				reply := &AppendEntriesReply{}
				fullResponse := AppendEntriesResponse{}
				fullResponse.ServerIndex = serverIndex
				fullResponse.Request = *requests[serverIndex]
				ok := rf.sendAppendEntries(serverIndex, requests[serverIndex], reply)

				fullResponse.Response = *reply
				if ok {
					rf.appendEntriesResponseCh <- fullResponse
				}
			}(serverIndex)
		}
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
	index = len(rf.log)
	rf.matchIndex[rf.me] = index
	rf.persist()

	return index, term, isLeader
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
			rf.mu.RUnlock()
			rf.sendAppendEntriesToAll()
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
				DPrintf("sending from server %d; entry : %v; commitIndex: %d; lastAppliedIndex: %d", rf.me, rf.log[rf.lastApplied], rf.commitIndex, rf.lastApplied+1)
				msg := ApplyMsg{}
				msg.Command = rf.log[rf.lastApplied].Data
				msg.CommandIndex = rf.lastApplied + 1
				msg.CommandValid = true
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
				rf.lastApplied += 1
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

			if fullResponse.Response.Term > rf.currentTerm {
				// convert to follower
				DPrint("Server ", rf.me, ": stepping down as AppendEntries Response had term ", fullResponse.Response.Term, " compared to current term of ", rf.currentTerm)
				rf.stepDown(fullResponse.Response.Term)
				rf.mu.Unlock()
				continue
			} else if !fullResponse.Response.Success && rf.nextIndex[fullResponse.ServerIndex] != 1 {
				DPrint("updating nextIndex for server ", fullResponse.ServerIndex, " from ", rf.nextIndex[fullResponse.ServerIndex], " as we got the response ", fullResponse.Response)
				lastIndex := rf.findLastInLog(fullResponse.Response.XTerm)
				if fullResponse.Response.XLength != -1 {
					rf.nextIndex[fullResponse.ServerIndex] = max(1, fullResponse.Response.XLength)
				} else if lastIndex != -1 {
					rf.nextIndex[fullResponse.ServerIndex] = lastIndex
				} else {
					rf.nextIndex[fullResponse.ServerIndex] = fullResponse.Response.XIndex
				}
				DPrint("nextIndex for server ", fullResponse.ServerIndex, " was updated to ", rf.nextIndex[fullResponse.ServerIndex], fullResponse.Response)
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

func (rf *Raft) maybeUpdateCommitIndex() {
	originalCommitIndex := rf.commitIndex
	DPrint("updating commitIndex maybe", originalCommitIndex, rf.matchIndex, ". log is ", rf.log)
	for N := originalCommitIndex + 1; N <= len(rf.log); N++ {
		if rf.log[N-1].Term != rf.currentTerm {
			continue
		}
		votesRequired := len(rf.peers)/2 + 1
		DPrint("N: ", N, ". term at log is ", rf.log[N-1].Term, ". current term is", rf.currentTerm, rf.matchIndex)
		for serverIndex, _ := range rf.peers {
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
	for i, logEntry := range rf.log {
		if logEntry.Term == targetTerm {
			return i + 1 // 1 based indexing
		}
	}
	return -1 // should never happen
}

func (rf *Raft) findLastInLog(targetTerm int) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == targetTerm {
			return i + 1 // 1 based indexing
		}
	}
	return -1
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
		rf.nextIndex[i] = len(rf.log) + 1
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
	rf.initializeMatchAndNextIndex()
	rf.appendEntriesResponseCh = make(chan AppendEntriesResponse)
	//DPrint("Initialize server id ", rf.me, " with electionTimeout ", rf.electionTimeout)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

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

	return rf
}
