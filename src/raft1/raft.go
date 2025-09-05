package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"log"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         Log
	// Volatile state on all servers:
	commitIndex int
	lastApplied int
	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// 状态 Leader、Candidate、Follower
	state State
	// 用于重定向
	leaderId int

	// 事件处理系统
	// 通用事件通道
	eventChan chan interface{}
	// 重置计时器信号，选举使用
	timerResetChan chan struct{}
	// 停止计时器信号
	timerStopChan chan struct{}
	// 选举计时器
	electionTimer *time.Timer

	// 投票计数器
	voteGranted int
	// 应用指令到状态机
	applyCh chan raftapi.ApplyMsg
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type Log struct {
	index0  int // entries 第一个元素在整个日志中的绝对位置，即当快照发生时，index0 仍然是连续的
	offset  int // 最后一个日志在 entries 的相对位置，但不知道自己在整个日志中的绝对位置
	entries []LogEntry
}

// return LastEntry's index and Term
func (l *Log) LastEntry() (lastIndex int, lastTerm int) {
	i := l.offset
	return l.index0 + i, l.entries[i].Term
}

// 从 index 开始追加日志，返回该日志所在位置和 term
func (rf *Raft) AppendEntry(entries ...LogEntry) (int, int) {
	log.Printf("S%v 追加日志: %+v", rf.me, entries)
	l := rf.log
	l.entries = append(l.entries, entries...)
	l.offset += len(entries)
	i := l.index0 + l.offset
	return i, l.entries[i].Term
}

// slice 日志, index 是在整个 logs 中的绝对位置
func (l *Log) sliceEnd(index int) (entries []LogEntry, lastIndex, lastTerm int) {
	// index 不能比 leader 最后一个日志 index 大
	lastIndex = l.index0 + l.offset
	lastTerm = l.entries[l.offset].Term
	if index > lastIndex {
		return []LogEntry{}, lastIndex, lastTerm
	}

	start := index - l.index0
	entries = l.entries[start:]
	return entries, lastIndex, lastTerm
}

// 是否为同一个日志
func (l *Log) isSameLogEntry(index, term int) bool {
	lastIndex, _ := l.LastEntry()
	if index > lastIndex || l.entries[index-l.index0].Term != term {
		return false
	}
	return true
}

// index 位置的 index（在整个 logs 的 index） 和 term
func (l *Log) getTerm(index int) (entry LogEntry) {
	i := index - l.index0
	if i > l.offset {
		return LogEntry{Term: -1}
	}
	return l.entries[i]
}

// 开始选举事件
type StartElectionEvent struct{}

// 投票请求事件，处理 candidate 投票请求
type VoteRequestEvent struct {
	args  *RequestVoteArgs
	reply *RequestVoteReply
	done  chan struct{} // 阻塞 RPC 协程，避免事件还没处理，就返回调用者，导致调用者处理响应异常
}

// 投票响应事件
type VoteResponseEvent struct {
	from  int
	reply *RequestVoteReply
}

// Leader 发送追加日志事件
type StartAppendEntriesEvent struct {
	to int // 追加给谁，意味着单播
}

// 追加日志响应事件
type AppendEntriesResponseEvent struct {
	from            int
	reply           *AppendEntriesReply
	appendLastIndex int // 记录当时追加的最后一个日志的 index
	appendLastTerm  int // 记录当时追加的最后一个日志的 term
}

// Follower 处理追加日志事件
type AppendEntriesEvent struct {
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
	done  chan struct{} // 阻塞 RPC 协程，避免事件还没处理，就返回调用者，导致调用者处理响应异常
}

func (rf *Raft) sendEvent(event interface{}) {
	select {
	case rf.eventChan <- event:
	default:
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	done := make(chan struct{})
	rf.sendEvent(VoteRequestEvent{args, reply, done})
	<-done // 等待事件处理完成
	// 问题是这里会一直阻塞，可能协程泄露，先简单这样实现
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// 可选的优化
	ConflictTerm  int // 发生冲突的 entry 的 term
	ConflictIndex int // 发生冲突的 entry 的 term 的首个日志索引
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// todo
	log.Printf("收到 AppendEntries %+v", args)
	done := make(chan struct{})
	rf.sendEvent(AppendEntriesEvent{args, reply, done})
	<-done // 等待事件处理完成
	// 问题是这里会一直阻塞，可能协程泄露，先简单这样实现
	// todo 排查思路应该是发送方的command数组为什么是 nil，所以这里收到的是 nil
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// todo
	log.Printf("发送 AppendEntries %+v", args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) StartElectionEventHandler(e StartElectionEvent) {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteGranted = 1
	DPrintf("Term: %v, S%v 开始选举，成为候选人", rf.currentTerm, rf.me)
	rf.timerResetChan <- struct{}{}

	index, term := rf.log.LastEntry()
	args := RequestVoteArgs{rf.currentTerm, rf.me, index, term}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(i, &args, &reply); ok {
				rf.sendEvent(VoteResponseEvent{i, &reply})
			}
		}(i)
	}
}

func (rf *Raft) RequestVoteHandler(e VoteRequestEvent) {
	defer close(e.done)
	// 候选人 term 比我小，直接拒绝
	if e.args.Term < rf.currentTerm {
		DPrintf("S%v(Term: %v), 拒绝给 S%v 投票，因为它的 term=%v, 比我小", rf.me, rf.currentTerm, e.args.CandidateId, e.args.Term)
		e.reply.Term = rf.currentTerm
		e.reply.VoteGranted = false
		return
	}

	// 如果候选人的任期更大，更新自己的状态
	if e.args.Term > rf.currentTerm {
		rf.currentTerm = e.args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	// raft 节点每个 Term 都只有一票，所以我已经在本轮投票给非请求者了，直接拒绝
	if rf.currentTerm == e.args.Term && rf.votedFor >= 0 && rf.votedFor != e.args.CandidateId {
		//DPrintf("S%v(Term: %v), 拒绝给 S%v 投票，因为我投票给了 S%v", rf.me, rf.currentTerm, e.args.CandidateId, rf.votedFor)
		e.reply.Term = rf.currentTerm
		e.reply.VoteGranted = false
		return
	}

	// 检查候选人的日志是否至少和自己一样新
	lastLogIndex, lastTerm := rf.log.LastEntry()
	// 候选人的最后日志任期更小
	if e.args.LastLogTerm < lastTerm {
		//DPrintf("S%v(Term: %v), 拒绝给 S%v 投票，因为候选人的最后日志任期更小, term=%v", rf.me, rf.currentTerm, e.args.CandidateId, e.args.LastLogTerm)
		e.reply.Term = rf.currentTerm
		e.reply.VoteGranted = false
	} else if e.args.LastLogTerm == lastTerm && e.args.LastLogIndex < lastLogIndex {
		//DPrintf("S%v(Term: %v), 拒绝给 S%v 投票，因为候选人的任期相同但索引更小, term=%v", rf.me, rf.currentTerm, e.args.CandidateId, e.args.LastLogTerm)
		// 任期相同但索引更小
		e.reply.Term = rf.currentTerm
		e.reply.VoteGranted = false
	} else {
		// 候选人的日志足够新
		// 当前 term 有票 && 请求者的日志是最新的，投给它
		rf.votedFor = e.args.CandidateId
		rf.currentTerm = e.args.Term
		e.reply.Term = rf.currentTerm
		e.reply.VoteGranted = true
		// 计时器重置
		rf.resetElectionTimer(250, 400)
		DPrintf("Term: %v, S%v 投票给 S%v, reply VoteGranted=%v", rf.currentTerm, rf.me, e.args.CandidateId, e.reply.VoteGranted)
	}
}

func (rf *Raft) VoteResponseHandler(e VoteResponseEvent) {
	// 目前实现是不会重复投票，所以只统计一次，如果请求丢失还会补发请求，那么这里要修改
	//DPrintf("Term: %v, S%v, 收到 S%v 投票响应: %v", rf.currentTerm, rf.me, e.from, e.reply.VoteGranted)

	// 忽略过期的响应
	if e.reply.Term != rf.currentTerm {
		DPrintf("Term: %v, S%v, 收到 S%v 在 term=%v 时的投票响应, 丢弃", rf.currentTerm, rf.me, e.from, e.reply.Term)
		return
	}

	if !e.reply.VoteGranted {
		//DPrintf("Term: %v, S%v 拒绝给 S%v 选票", rf.currentTerm, e.from, rf.me)
		return
	}

	rf.voteGranted++
	if rf.voteGranted > len(rf.peers)/2 {
		DPrintf("Term: %v, S%v 成为 Leader", rf.currentTerm, rf.me)
		rf.state = Leader
		rf.leaderId = rf.me
		// 这里重置了计时器，但是要考虑，如果转为了 follower，那么定时器应该重置为 150 ~ 300 ms，所以当有 term 比自己高的 leader 出现、心跳大部分没响应时，应该这样做
		rf.resetElectionTimer(100, 200)
		rf.sendEvent(StartAppendEntriesEvent{})
	}
}

func (rf *Raft) StartAppendEntriesEventHandler(e StartAppendEntriesEvent) {
	// 这是心跳，多播
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.SendAppendForOne(i)
	}
}

func (rf *Raft) SendAppendForOne(to int) {
	// 利用心跳完成日志追赶
	entries, appendLastIndex, appendLastTerm := rf.log.sliceEnd(rf.nextIndex[to])
	PrevLogIndex := rf.nextIndex[to] - 1
	log.Printf("S%v 发送追加日志到 S%v, PrevLogIndex:  %v, logs: %+v", rf.me, to, PrevLogIndex, entries)
	PrevLogTerm := rf.log.getTerm(PrevLogIndex).Term
	args := &AppendEntriesArgs{rf.currentTerm, rf.me, PrevLogIndex, PrevLogTerm, entries, rf.commitIndex}
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(to, args, &reply)
	rf.sendEvent(AppendEntriesResponseEvent{to, &reply, appendLastIndex, appendLastTerm})
}

func (rf *Raft) AppendEntriesResponseHandler(e AppendEntriesResponseEvent) {
	// 如果 follower 任期比我大，则主动退位，即使这个 Follower 并不代表拥有最新日志，交给 Term 去处理，重新开始选举或者做其他什么事
	if !e.reply.Success && e.reply.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = e.reply.Term
		rf.resetElectionTimer(250, 400)
	}

	log.Printf("S%v 追加日志收到 S%v 的响应: %+v", rf.me, e.from, e.reply)
	if e.reply.Success {
		// 可以更新  nextIndex 和 matchIndex
		rf.nextIndex[e.from] = e.appendLastIndex + 1
		rf.matchIndex[e.from] = e.appendLastIndex + 1
		// 判断是否能够：提交日志、应用到状态机
		rf.leaderCommitAndApplyLog(e.appendLastIndex, e.appendLastTerm)
	} else {
		// 日志不匹配，更新 nextIndex
		rf.nextIndex[e.from] = e.reply.ConflictIndex
		// 马上重试
		rf.SendAppendForOne(e.from)
	}
}

func (rf *Raft) AppendEntriesHandler(e AppendEntriesEvent) {
	defer close(e.done)
	log.Printf("S%v 收到 S%v 的追加日志请求: %+v", rf.me, e.args.LeaderId, e.args)
	// 无论如何 term 应该先保证，这意味着一个时代的开始
	if e.args.Term < rf.currentTerm {
		e.reply.Term = rf.currentTerm
		e.reply.Success = false
		return
	}

	prevLogIndex, prevLogTerm, leaderCommitIndex := e.args.PrevLogIndex, e.args.PrevLogTerm, e.args.LeaderCommitIndex
	latestLogIndex, _ := rf.log.LastEntry()

	if leaderCommitIndex > rf.commitIndex {
		rf.followerCommitAndApplyLog(leaderCommitIndex, latestLogIndex)
	}

	entries := rf.log.entries
	if !rf.log.isSameLogEntry(prevLogIndex, prevLogTerm) {
		e.reply.Success = false
		// 优化，按 term 回溯
		// 找到发生冲突的 term 的首个日志 index
		_, term := rf.log.LastEntry()
		for i, entry := range entries {
			if entry.Term == term {
				e.reply.ConflictTerm = term
				e.reply.ConflictIndex = i + rf.log.index0
				break
			}
		}
		return
	}

	// 确定 Leader 的领导地位
	rf.state = Follower
	rf.leaderId = e.args.LeaderId
	rf.resetElectionTimer(250, 400)
	// 追加日志/覆盖发生冲突的日志
	rf.log.entries = entries[:e.args.PrevLogIndex+1]
	log.Printf("Follower S%v 追加日志: %+v", rf.me, entries)
	rf.AppendEntry(e.args.Entries...)
	e.reply.Term = rf.currentTerm
	e.reply.Success = true
}

func (rf *Raft) leaderCommitAndApplyLog(index, term int) {
	// 只有提交当前 term 日志时，才能采用大多数副本的方式，否则可能会被覆盖
	if rf.currentTerm == term && index > rf.commitIndex {
		majority := 1
		for _, matchIndex := range rf.matchIndex {
			lastLogTerm := rf.log.getTerm(index).Term
			if matchIndex > index && lastLogTerm == rf.currentTerm {
				majority++
			}
		}
		if majority > len(rf.peers)/2 {
			// 提交追加的日志，即持久化
			rf.commitIndex = index
			// 只要 commitIndex > lastApplied，就可以应用到状态机
			if rf.commitIndex > rf.lastApplied {
				// 应用日志到 applyChan
				go rf.applyLog()
			}
		}
	}
}

// 应用日志
func (rf *Raft) applyLog() {
	// 循环应用，因为不支持批量应用
	for i := rf.lastApplied; i <= rf.commitIndex; i++ {
		entry := rf.log.getTerm(i)
		rf.applyCh <- raftapi.ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: i}
		rf.lastApplied++
	}
}

func (rf *Raft) followerCommitAndApplyLog(leaderCommitIndex, latestLogIndex int) {
	rf.commitIndex = min(leaderCommitIndex, latestLogIndex)
	rf.applyLog()
}

func (rf *Raft) handleEvent(event interface{}) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch e := event.(type) {
	case StartElectionEvent:
		rf.StartElectionEventHandler(e)
	case VoteRequestEvent:
		rf.RequestVoteHandler(e)
	case VoteResponseEvent:
		rf.VoteResponseHandler(e)
	case StartAppendEntriesEvent:
		rf.StartAppendEntriesEventHandler(e)
	case AppendEntriesResponseEvent:
		rf.AppendEntriesResponseHandler(e)
	case AppendEntriesEvent:
		rf.AppendEntriesHandler(e)
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
// 返回的只是 "开始处理" 的确认，不代表现在就提交了
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	log.Printf("Leader S%v 收到 command: %+v", rf.me, command)
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	// 不是 leader 不需要添加日志
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.killed() {
		return -1, -1, false
	}
	isLeader = true
	entry := LogEntry{rf.currentTerm, command}
	log.Printf("entry=%+v", entry)
	// todo 为什么这个方法里面就为 nil 了？
	index, term = rf.AppendEntry(entry)
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
	close(rf.timerStopChan)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 产生一个 [start, end) 之间的随机毫秒数
func (rf *Raft) randomElectionTimeout(start, end int) time.Duration {
	if start >= end {
		// 简单点，直接挂掉
		log.Panic("error: start >= end")
		rf.Kill()
	}
	return time.Duration(start+rand.Intn(end-start+1)) * time.Millisecond
}

func (rf *Raft) eventLoop() {
	DPrintf("S%v 开始事件循环", rf.me)
	for !rf.killed() {
		select {
		case event := <-rf.eventChan:
			rf.handleEvent(event)
		case <-rf.electionTimer.C:
			rf.handleTimeout()
		case <-rf.timerResetChan:
			rf.resetElectionTimer(150, 151)
		case <-rf.timerStopChan:
			return
		}
	}
}

func (rf *Raft) handleTimeout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch rf.state {
	case Follower, Candidate:
		rf.state = Candidate
		rf.sendEvent(StartElectionEvent{})
	case Leader:
		rf.resetElectionTimer(100, 200)
		// 开始心跳
		rf.sendEvent(StartAppendEntriesEvent{})
	default:
	}
}

// 使用 [start, end) 毫秒，重置超时计时器
func (rf *Raft) resetElectionTimer(start, end int) {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(rf.randomElectionTimeout(start, end))
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.applyCh = applyCh
	// index 0 放入 term 0，确保初始的一致性检查总是成功
	rf.log = Log{
		index0:  0,
		offset:  0,
		entries: []LogEntry{{Term: 0, Command: nil}},
	}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = rf.log.index0 + 1
	}
	rf.matchIndex = make([]int, len(peers))

	rf.state = Follower
	rf.eventChan = make(chan interface{}, 100)
	rf.timerResetChan = make(chan struct{}, 1)
	rf.timerStopChan = make(chan struct{})
	rf.electionTimer = time.NewTimer(rf.randomElectionTimeout(150, 300))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.eventLoop()

	return rf
}
