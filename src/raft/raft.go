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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "../labgob"

type Role int

const (
	Follower   Role = 0
	Candidate  Role = 1
	Leader     Role = 2
)

const (
	ElectionTimeout  = time.Millisecond * 300
	HeartBeatTimeout = time.Millisecond * 100
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
	Term 	int
	Command interface{}
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int
	votedFor	  int
	log[]		  []LogEntry
	role 		  Role

	electionTimer *time.Timer
	pingTimer 	  *time.Timer
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
	isleader = (rf.role == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 	        int
	CandidateId    int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term		int
	VoteGranted bool
}


type AppendEntriesArgs struct {
	Term 		int
	LeaderId	int
}

type AppendEntriesReply struct {
	Term		int
	Success 	bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	// ??????????????????????????????requestVote??????
	/*
		1. ???????????????????????????????????????????????????????????????candidate?????????follower???
		2. ????????????????????????follower???????????????????????????
		3. ?????????????????????????????????
	*/
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			rf.becomeFollower(args.Term)
		} else {
			rf.electionTimer.Reset(getRandElectTimeout())
		}
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term >= rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Success = true
	} else {
		reply.Success = false
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:			peers,
		persister:		persister,
		me:				me,
		role:			Follower,
		currentTerm:	0,
		votedFor:		-1,
		electionTimer:  time.NewTimer(getRandElectTimeout()),
		pingTimer:		time.NewTimer(HeartBeatTimeout),
	}

	// Your initialization code here (2A, 2B, 2C).
	go rf.electionLoop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	return rf
}


// user defined function
func getRandElectTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeout
	return r + ElectionTimeout
}

func (rf *Raft) becomeFollower(term int) {
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.electionTimer.Reset(getRandElectTimeout())
	DPrintf("Raft %d becomes follower with term %d\n", rf.me, term)
}

func (rf *Raft) becomeCandidate() {
	rf.currentTerm += 1
	rf.role = Candidate
	rf.votedFor = rf.me
	DPrintf("Raft %d becomes candidate with term %d\n", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	DPrintf("Raft %d becomes leader with term %d\n", rf.me, rf.currentTerm)
	go rf.pingLoop()
}

func (rf *Raft) electionLoop() {
	for !rf.killed() {
		<- rf.electionTimer.C
		rf.mu.Lock()
		rf.electionTimer.Reset(getRandElectTimeout())
		rf.mu.Unlock()
		rf.startElection()
	}
}

func (rf *Raft) pingLoop() {
	rf.pingTimer.Reset(HeartBeatTimeout)
	for {
		_, isLeader := rf.GetState()
		if !isLeader {
			return
		}
		<- rf.pingTimer.C
		rf.startHeartBeat()
		rf.mu.Lock()
		rf.pingTimer.Reset(HeartBeatTimeout)
		rf.mu.Unlock()
		DPrintf("Raft %d start next ping round\n", rf.me)
	}
}

func (rf *Raft) startHeartBeat() {
	rf.mu.Lock()
	term := rf.currentTerm
	DPrintf("Raft %d start heartbeat in term %d\n", rf.me, term)
	args := AppendEntriesArgs{
		Term:     term,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			go func (i int)  {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				rf.mu.Lock()
				if ok {
					if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
					} else {
						rf.electionTimer.Reset(getRandElectTimeout())
					}
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}

func (rf *Raft) startElection() {
	DPrintf("Raft %d election timeout\n", rf.me)
	_, isLeader := rf.GetState()
	if isLeader {
		DPrintf("Raft %d is already the leader, skip elction\n", rf.me)
		return
	}
	rf.mu.Lock()
	rf.becomeCandidate()
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()
	votesCount := 1
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(i int) {
				reply := RequestVoteReply{}
				DPrintf("Raft %d send RequestVote to %d with term %d\n", rf.me, i, args.Term)
				ok := rf.sendRequestVote(i, &args, &reply)
				rf.mu.Lock()
				// ???????????????Term??????
				if ok && rf.currentTerm == args.Term && rf.role == Candidate{
					if reply.VoteGranted {
						votesCount++
						if votesCount > len(rf.peers)/2 {
							DPrintf("Raft %d receives majority votes in term %d\n", rf.me, rf.currentTerm)
							rf.becomeLeader()
						}
					} else if reply.Term > rf.currentTerm {
						// ??????????????????????????????????????????Term--leader??????????????????
						DPrintf("Raft %d finds a leader %d in term %d", rf.me, i, rf.currentTerm)
						rf.becomeFollower(reply.Term)
					}
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}