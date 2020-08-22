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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

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
	Command interface{}
	Term    int
}

type RaftState int

const (
	Leader RaftState = iota
	Candidate
	Follower
)

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
	votedFor    int
	state       RaftState
	currentTerm int
	voteTimeout time.Time
	leaderID    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
}

func getNewVoteTimeout() time.Time {
	n := 500 + rand.Intn(500)
	newTime := time.Now().Add(time.Millisecond * time.Duration(n))
	//fmt.Printf("%v  : New Timeout: %v\n", time.Now().Format("15:04:05.000000"), newTime.Format("15:04:05.000000"))
	return newTime
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
	isleader = (rf.state == Leader)

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log []LogEntry
	var currentTerm int
	var votedFor int
	if d.Decode(&log) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil {
		fmt.Println("Couldnt restore persist")
	} else {
		rf.log = log
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.persist()
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	//LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Printf("%v S %d : Got vote request Candidate: %d Candidate term: %d \n", time.Now().Format("15:04:05.000000"), rf.me, args.CandidateID, args.Term)
	//fmt.Printf("%v S %d : Voted for: %d Current term: %d \n", time.Now().Format("15:04:05.000000"), rf.me, rf.votedFor, rf.currentTerm)

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.voteTimeout = getNewVoteTimeout()
		rf.votedFor = -1
		rf.persist()
	}

	if rf.state == Leader {
		return
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if (rf.votedFor < 0) || (rf.votedFor == args.CandidateID) {
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term
		requestMoreUpToDate := false

		if args.LastLogTerm != lastLogTerm { // compare terms
			if args.LastLogTerm > lastLogTerm {
				requestMoreUpToDate = true
			}
		} else {
			if args.LastLogIndex >= lastLogIndex {
				requestMoreUpToDate = true
			}
		}

		if requestMoreUpToDate {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			reply.Term = rf.currentTerm
			rf.voteTimeout = getNewVoteTimeout()
			rf.persist()
		}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	if rf.state == Leader {
		fmt.Printf("%v S %d : I'm a leader already! T: %d CT: %d\n", time.Now().Format("15:04:05.000000"), rf.me, args.Term, rf.currentTerm)
		return
	}
	rf.voteTimeout = getNewVoteTimeout()
	//fmt.Printf("%v S %d : Append T: %d CT: %d PLI: %d PLT: %d\n", time.Now().Format("15:04:05.000000"), rf.me, args.Term, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		if len(args.Entries) <= 0 { //heartbeat
			reply.Success = true
			reply.Term = rf.currentTerm
			rf.state = Follower
			//fmt.Println("heartbeat ", len(rf.log), args.LeaderCommit)
			if len(rf.log) > args.LeaderCommit {
				for args.LeaderCommit > rf.commitIndex {
					rf.commitIndex = rf.commitIndex + 1
					fmt.Printf("%v S %d : New commit: %d\n", time.Now().Format("15:04:05.000000"), rf.me, rf.commitIndex)
					msg := ApplyMsg{Command: rf.log[rf.commitIndex].Command, CommandIndex: rf.commitIndex, CommandValid: true}
					rf.applyCh <- msg
				}
			}
		} else { // actual entries
			if len(rf.log) <= args.PrevLogIndex {
				reply.Success = false
				reply.ConflictIndex = len(rf.log) - 1
			} else {
				if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
					fmt.Printf("%v S %d : Appending %d entries to the log, PLI: %d\n", time.Now().Format("15:04:05.000000"), rf.me, len(args.Entries), args.PrevLogIndex)
					newEntries := make([]LogEntry, 0)
					for _, e := range args.Entries {
						newEntries = append(newEntries, LogEntry{Command: e, Term: args.Term})
					}

					rf.log = append(rf.log[:args.PrevLogIndex+1], newEntries...)
					reply.Success = true
					reply.Term = rf.currentTerm
					rf.persist()
				} else {
					reply.Success = false
					reply.ConflictIndex = args.PrevLogIndex
				}
			}
		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) canCommit(index int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	count := 0
	//fmt.Println("Can commit ", index)
	if rf.commitIndex >= index {
		return false
	}

	for _, v := range rf.matchIndex {
		//fmt.Println("Check ", i, v)
		if v >= index {
			count = count + 1
		}
	}

	return count > len(rf.peers)/2
}

func (rf *Raft) findNewIndex(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ret := -1
	lastLogIndex := len(rf.log) - 1
	//fmt.Println("lastLogIndex ", lastLogIndex, " match index ", rf.matchIndex[server])
	if lastLogIndex > rf.matchIndex[server] {
		ret = lastLogIndex
	}

	return ret
}

func (rf *Raft) sendIndex(server int, index int) {
	go func(server int) {
		k := 0
		for {
			if rf.state != Leader {
				return
			}
			args := AppendEntriesArgs{}
			reply := AppendEntriesReply{}
			rf.mu.Lock()
			args.Term = rf.currentTerm
			//args.LeaderID = rf.me
			args.LeaderCommit = rf.commitIndex
			fmt.Printf("%v S %d : sendIndex %d index %d\n", time.Now().Format("15:04:05.000000"), rf.me, server, index)
			for _, e := range rf.log[rf.matchIndex[server]+1 : index+1] {
				//fmt.Printf("%v S %d : Writing command %d\n", time.Now().Format("15:04:05.000000"), rf.me, e)
				args.Entries = append(args.Entries, e.Command)
			}
			args.PrevLogIndex = rf.matchIndex[server]
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			//fmt.Printf("%v S %d : Sending entries to %d PLI: %d LL: %d \n", time.Now().Format("15:04:05.000000"), rf.me, server, args.PrevLogIndex, len(args.Entries))
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(server, &args, &reply)
			if !reply.Success && ok && reply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.voteTimeout = getNewVoteTimeout()
				rf.persist()
				rf.mu.Unlock()
				return
			} else if reply.Success && ok {
				rf.mu.Lock()
				fmt.Printf("%v S %d : Start reply Server: %v Success: %v \n", time.Now().Format("15:04:05.000000"), rf.me, server, reply.Success)
				if rf.matchIndex[server] < args.PrevLogIndex+len(args.Entries) {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				}
				rf.mu.Unlock()
				for i := range args.Entries {
					tryIndex := args.PrevLogIndex + 1 + i
					if rf.canCommit(tryIndex) {
						fmt.Printf("%v S %d : Committing entry: %v \n", time.Now().Format("15:04:05.000000"), rf.me, tryIndex)
						rf.commitIndex = tryIndex
						msg := ApplyMsg{Command: rf.log[rf.commitIndex].Command, CommandIndex: rf.commitIndex, CommandValid: true}
						rf.applyCh <- msg
					}
				}

				return
			} else if !reply.Success && ok {
				rf.mu.Lock()
				k = k + 1
				newIndex := max(int(0), (reply.ConflictIndex - (4 * k)))
				newIndex = min(newIndex, len(rf.log)-1)
				fmt.Printf("%v S %d : New index: %v, k: %v \n", time.Now().Format("15:04:05.000000"), rf.me, newIndex, k)
				rf.matchIndex[server] = newIndex
				rf.mu.Unlock()
			} else if !ok {

				return
			}
		}
	}(server)
}

func max(x, y int) int {
	if x < y {
		return y
	}
	return x
}
func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func (rf *Raft) sendHeartbeat(server int) {
	go func(server int) {
		args := AppendEntriesArgs{}
		reply := AppendEntriesReply{}
		rf.mu.Lock()
		args.Term = rf.currentTerm
		//args.LeaderID = rf.me
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()
		ok := rf.sendAppendEntries(server, &args, &reply)
		if !reply.Success && ok && reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.voteTimeout = getNewVoteTimeout()
			rf.persist()
			rf.mu.Unlock()
		}
	}(server)
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
	isLeader := (rf.state == Leader)

	if !isLeader {
		return index, term, isLeader
	}

	// Your code here (2B).
	rf.mu.Lock()
	index = rf.nextIndex[rf.me]
	rf.nextIndex[rf.me] = rf.nextIndex[rf.me] + 1
	term = rf.currentTerm
	newEntry := LogEntry{Term: term, Command: command}
	rf.log = append(rf.log, newEntry)
	rf.matchIndex[rf.me] = index
	rf.persist()
	rf.mu.Unlock()

	fmt.Printf("%v S %d : Start I: %d T: %d \n", time.Now().Format("15:04:05.000000"), rf.me, index, term)

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

func (rf *Raft) doHeartbeats() {
	for i := range rf.peers {
		if i != rf.me {
			lastNewIndex := rf.findNewIndex(i)

			if lastNewIndex <= 0 {
				rf.sendHeartbeat(i)
			} else {
				rf.sendIndex(i, lastNewIndex)
			}
		}
	}
}

func (rf *Raft) startElection() {
	fmt.Printf("%v S %d : Starting vote, current term %d \n", time.Now().Format("15:04:05.000000"), rf.me, rf.currentTerm)
	go func() {
		var votesFor int64
		var votesTotal int64

		for i := range rf.peers {
			if i != rf.me {
				go func(server int) {
					args := RequestVoteArgs{}
					reply := RequestVoteReply{}
					rf.mu.Lock()
					args.Term = rf.currentTerm
					args.CandidateID = rf.me
					args.LastLogIndex = len(rf.log) - 1
					args.LastLogTerm = rf.log[args.LastLogIndex].Term
					rf.mu.Unlock()
					res := rf.sendRequestVote(server, &args, &reply)
					fmt.Printf("%v S %d : Got vote %v validity %v,  total %d \n", time.Now().Format("15:04:05.000000"), rf.me, reply.VoteGranted, res, votesFor)
					atomic.AddInt64(&votesTotal, 1)
					if res && reply.VoteGranted {
						atomic.AddInt64(&votesFor, 1)
					}
				}(i)
			}
		}

		for rf.state == Candidate {
			time.Sleep(time.Duration(10) * time.Millisecond)
			if rf.killed() {
				return
			}
			if votesFor >= int64(len(rf.peers)/2) {
				fmt.Printf("%v S %d : Become leader \n", time.Now().Format("15:04:05.000000"), rf.me)
				rf.mu.Lock()
				rf.state = Leader
				rf.voteTimeout = getNewVoteTimeout()
				rf.votedFor = -1
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}
				rf.persist()
				rf.mu.Unlock()
				rf.doHeartbeats()
				return
			} else if votesTotal >= int64(len(rf.peers)-1) {
				fmt.Printf("%vf S %d : Failed to become leader \n", time.Now().Format("15:04:05.000000"), rf.me)
				rf.mu.Lock()
				rf.state = Follower
				rf.voteTimeout = getNewVoteTimeout()
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				return
			}
		}

	}()
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.voteTimeout = getNewVoteTimeout()
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = []LogEntry{LogEntry{}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// this goroutine periodically sends heartbeats to followers
	go func(rf *Raft) {
		for {
			time.Sleep(150 * time.Millisecond)
			if rf.killed() {
				return
			}
			if rf.state == Leader {
				rf.doHeartbeats()
			}
		}
	}(rf)

	// this goroutine monitors vote timeout and starts a vote if it expires
	go func(rf *Raft) {
		for {
			time.Sleep(100 * time.Millisecond)
			if rf.killed() {
				return
			}
			//fmt.Printf("%v S %d : State is %v vote timeout is %v \n", time.Now().Format("15:04:05.000000"), rf.me, rf.state, rf.voteTimeout.Format("15:04:05.000000"))
			if rf.state == Follower {
				if time.Now().After(rf.voteTimeout) {
					rf.mu.Lock()
					rf.currentTerm = rf.currentTerm + 1
					rf.state = Candidate
					rf.voteTimeout = getNewVoteTimeout()
					rf.votedFor = rf.me
					rf.persist()
					rf.mu.Unlock()
					rf.startElection()
				}
			}
		}
	}(rf)

	return rf
}
