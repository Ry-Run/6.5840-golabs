package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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
	CurrentTerm int
	VotedFor    int
	Log         Log
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
	// 停止计时器信号
	timerStopChan chan struct{}
	// 选举计时器
	electionTimer *time.Timer

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
	Index0  int // 即快照后第一个日志的索引，相当于绝对位置
	Entries []LogEntry
}

// return LastEntry's index and Term
func (l *Log) LastEntry() (lastIndex int, lastTerm int) {
	i := len(l.Entries) - 1
	return l.Index0 + i, l.Entries[i].Term
}

// 从 index 开始追加日志，返回第一个日志所在位置和 term
func (rf *Raft) AppendEntry(entries ...LogEntry) (int, int) {
	rf.Log.Entries = append(rf.Log.Entries, entries...)
	// 持久化一次 Entries
	rf.persist()
	return rf.Log.LastEntry()
}

// slice [index, lastIndex] 日志, index 是在整个 logs 中的绝对位置，返回 lastIndex 的绝对位置
func (l *Log) sliceEnd(index int) (entries []LogEntry, lastIndex, lastTerm int) {
	// index 不能比 leader 最后一个日志 index 大
	lastIndex, lastTerm = l.LastEntry()
	if index > lastIndex {
		return []LogEntry{}, lastIndex, lastTerm
	}

	start := index - l.Index0
	entries = l.Entries[start:]
	return entries, lastIndex, lastTerm
}

// slice [start, end] 日志, start、end 是在整个 logs 中的绝对位置，返回 start 的绝对位置
func (l *Log) slice(start, end int) (entries []LogEntry, startIndex, startTerm int) {
	lastIndex, _ := l.LastEntry()

	// todo 应该大于index0
	if 0 <= start && start <= end && end <= lastIndex {
		relativeStart := start - l.Index0
		relativeEnd := end - l.Index0
		entries = l.Entries[relativeStart : relativeEnd+1]
		return entries, start, l.getEntry(start).Term
	}

	return []LogEntry{}, -1, -1
}

// 指定索引位置的日志条目，是否为同一个日志
func (l *Log) isSameLogEntry(index, term int) bool {
	entry := l.getEntry(index)
	if entry.Term != term {
		return false
	}
	return true
}

// index 位置的 entry（在整个 logs 的 index，即绝对位置） 和 term
func (l *Log) getEntry(index int) (entry LogEntry) {
	i := index - l.Index0
	if i >= len(l.Entries) {
		return LogEntry{Term: -1}
	}
	return l.Entries[i]
}

// 判断 index, term 这个日志日志是不是至少最新
func (l *Log) isAtLeast(index, term int) bool {
	// 检查候选人的日志是否至少和自己一样新
	lastLogIndex, lastTerm := l.LastEntry()
	// 候选人的最后日志任期更小
	if term < lastTerm || term == lastTerm && index < lastLogIndex {
		return false
	}
	return true
}

// 判断 index, term 这个日志日志是不是至少最新
func (l *Log) hasLogsToReplicate(nextIndex int) bool {
	lastIndex, _ := l.LastEntry()
	return nextIndex <= lastIndex
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
	from        int
	reply       *RequestVoteReply
	term        int  // 发起投票时的 term
	voteGranted *int // 指向当前选举的投票计数器
}

// Follower 处理追加日志事件
type AppendEntriesEvent struct {
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
	done  chan struct{} // 阻塞 RPC 协程，避免事件还没处理，就返回调用者，导致调用者处理响应异常
}

func (rf *Raft) sendEvent(event interface{}) {
	rf.eventChan <- event
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.state == Leader
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log Log

	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&Log) != nil {
		log.Printf("解码失败 in readPersist")
		return
	}

	rf.CurrentTerm = CurrentTerm
	rf.VotedFor = VotedFor
	rf.Log = Log
}

// how many bytes in Raft's persisted Log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the Log through (and including)
// that index. Raft should now trim its Log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	// 0 位置不用制作为快照
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
	XTerm  int // 发生冲突的 entry 的 term
	XIndex int // 发生冲突的 entry 的 term 的首个日志索引
	XLen   int // Follower 的日志长度
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	done := make(chan struct{})
	rf.sendEvent(AppendEntriesEvent{args, reply, done})
	<-done // 等待事件处理完成
	// 问题是这里会一直阻塞，可能协程泄露，先简单这样实现
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
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) StartElectionEventHandler(e StartElectionEvent) {
	rf.state = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	voteGranted := 1
	DPrintf("Term: %v, S%v 开始选举，成为候选人", rf.CurrentTerm, rf.me)
	// 进入新任期，CurrentTerm、VotedFor 变化，持久化一次
	rf.persist()

	index, term := rf.Log.LastEntry()
	args := RequestVoteArgs{rf.CurrentTerm, rf.me, index, term}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			//如果已经不再是候选者 则直接返回
			if state != Candidate {
				return
			}
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(i, &args, &reply); ok {
				rf.sendEvent(VoteResponseEvent{i, &reply, args.Term, &voteGranted})
			}
		}(i)
	}
}

func (rf *Raft) RequestVoteHandler(e VoteRequestEvent) {
	defer close(e.done)
	// 候选人 term 比我小，直接拒绝
	if e.args.Term < rf.CurrentTerm {
		DPrintf("S%v(Term: %v), 拒绝给 S%v 投票，因为它的 term=%v, 比我小", rf.me, rf.CurrentTerm, e.args.CandidateId, e.args.Term)
		e.reply.Term = rf.CurrentTerm
		e.reply.VoteGranted = false
		return
	}

	// 如果候选人的任期更大，更新自己的状态
	if e.args.Term > rf.CurrentTerm {
		rf.CurrentTerm = e.args.Term
		rf.VotedFor = -1
		rf.leaderId = -1
		rf.state = Follower
		// CurrentTerm、VotedFor 变化，持久化一次
		rf.persist()
	}

	// raft 节点每个 Term 都只有一票，所以我已经在本轮投票给非请求者了，直接拒绝
	if rf.VotedFor >= 0 && rf.VotedFor != e.args.CandidateId {
		e.reply.Term = rf.CurrentTerm
		e.reply.VoteGranted = false
		return
	}

	// 检查候选人的日志是否至少和自己一样新
	lastLogIndex, lastTerm := rf.Log.LastEntry()
	// 候选人的最后日志任期更小
	if e.args.LastLogTerm < lastTerm {
		e.reply.Term = rf.CurrentTerm
		e.reply.VoteGranted = false
	} else if e.args.LastLogTerm == lastTerm && e.args.LastLogIndex < lastLogIndex {
		// 任期相同但索引更小
		e.reply.Term = rf.CurrentTerm
		e.reply.VoteGranted = false
	} else {
		// 候选人的日志足够新
		// 当前 term 有票 && 请求者的日志是最新的，投给它
		rf.VotedFor = e.args.CandidateId
		rf.leaderId = e.args.CandidateId
		rf.CurrentTerm = e.args.Term
		e.reply.Term = rf.CurrentTerm
		e.reply.VoteGranted = true
		// 计时器重置
		rf.resetElectionTimer(250, 400)
		DPrintf("Term: %v, S%v 投票给 S%v, reply VoteGranted=%v", rf.CurrentTerm, rf.me, e.args.CandidateId, e.reply.VoteGranted)
		// CurrentTerm、VotedFor 变化，持久化一次
		rf.persist()
	}
}

func (rf *Raft) VoteResponseHandler(e VoteResponseEvent) {
	// 如果响应携带更新的任期，退回 Follower
	if e.reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = e.reply.Term
		rf.state = Follower
		rf.VotedFor = -1
		rf.leaderId = -1
		rf.persist()
		rf.resetElectionTimer(250, 400)
		return
	}

	// 防止被其他 term 响应修改
	if rf.CurrentTerm != e.term {
		return
	}

	if rf.state != Candidate {
		return
	}

	if !e.reply.VoteGranted {
		return
	}

	*e.voteGranted++
	if *e.voteGranted > len(rf.peers)/2 {
		DPrintf("Term: %v, S%v 成为 Leader", rf.CurrentTerm, rf.me)
		rf.state = Leader
		rf.leaderId = rf.me
		lastIndex, _ := rf.Log.LastEntry()
		// 重置 nextIndex、matchIndex
		for i, _ := range rf.peers {
			if i != rf.me {
				rf.nextIndex[i] = lastIndex + 1
				rf.matchIndex[i] = 0
				// 开始复制日志的协程
				log.Printf("leader s%v 启动日志复制、心跳协程", rf.me)
				go rf.replicateLog(i)
			}
		}
	}
}

func (rf *Raft) replicateLog(peer int) {
	heartbeatTicker := time.NewTicker(50 * time.Millisecond) // 心跳间隔
	defer heartbeatTicker.Stop()

	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		// 检查是否有日志需要复制
		nextIndex := rf.nextIndex[peer]
		var s string

		// 准备参数
		var entries []LogEntry

		// 有日志时发生日志，空闲时发送心跳
		if rf.Log.hasLogsToReplicate(nextIndex) {
			// 有日志需要复制
			entries, _, _ = rf.Log.sliceEnd(nextIndex)
			s = "日志"
		} else {
			// 心跳负责保持地位、确认日志一致性
			entries = []LogEntry{}
			s = "心跳"
		}

		PrevLogIndex := max(nextIndex-1, 0)
		PrevLogTerm := rf.Log.getEntry(PrevLogIndex).Term

		log.Printf("S%v 发送%s到 S%v, PrevLogIndex: %v, PrevLogTerm: %v, commitIndex:%v", rf.me, s, peer, PrevLogIndex, PrevLogTerm, rf.commitIndex)
		args := AppendEntriesArgs{rf.CurrentTerm, rf.me, PrevLogIndex, PrevLogTerm, entries, rf.commitIndex}

		// 阻塞调用前解锁
		rf.mu.Unlock()
		//  发送 AppendEntries 请求
		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(peer, &args, &reply)

		if !ok {
			// RPC 失败，等待一段时间后重试
			log.Printf("S%v 发送%s RPC 到 S%v 失败，等待一段时间后重试", rf.me, s, peer)
			continue
		}

		// 处理响应
		rf.mu.Lock()

		log.Printf("leader S%v 收到 S%v 的追加日志响应: %+v", rf.me, peer, reply)
		// 如果 follower 任期比我大，则主动退位
		if reply.Term > rf.CurrentTerm {
			log.Printf("S%v 的 term 更大，Leader S%v 退位", peer, rf.me)
			rf.state = Follower
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.leaderId = -1
			// CurrentTerm 变化，持久化一次
			rf.persist()
			rf.mu.Unlock()
			return
		}

		// 只处理当前 term 的响应
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			commitIndex := PrevLogIndex + len(entries)
			// 可以更新  nextIndex 和 matchIndex
			rf.nextIndex[peer] = commitIndex + 1
			rf.matchIndex[peer] = commitIndex
			log.Printf("Leader S%v 设置 S%v 的 nextIndex=%v, matchIndex=%v, entries=%+v", rf.me, peer, commitIndex+1, commitIndex, rf.Log.Entries)
			//log.Printf("Leader S%v 设置 S%v 的 nextIndex=%v, matchIndex=%v", rf.me, peer, PrevLogIndex + 1, PrevLogIndex)
			rf.leaderCommitAndApplyLog(commitIndex, rf.Log.getEntry(commitIndex).Term)
		} else {
			conflictTerm := reply.XTerm
			if conflictTerm == -1 {
				// case 3: Follower 的日志太短
				rf.nextIndex[peer] = max(reply.XLen, 1)
				log.Printf("Leader S%v 设置 S%v 的 nextIndex=%v", rf.me, peer, max(reply.XLen, 1))
			} else {
				peerNextIndex := -1
				for i := len(rf.Log.Entries); i > 0; i-- {
					if rf.Log.Entries[i-1].Term == conflictTerm {
						peerNextIndex = i + rf.Log.Index0
						break
					}
				}

				if peerNextIndex == -1 {
					// case 1: Leader 没有 XTerm: 意味着 Leader 的日志在这个任期上是"缺失"的
					// 应该直接回退到 Follower 中该任期开始的位置（XIndex），从这里开始同步日志，覆盖掉 Follower 该任期的日志。
					rf.nextIndex[peer] = reply.XIndex
				} else {
					// case 2: Leader 有 XTerm
					// 如果 Leader 有相同的任期，但日志内容可能不同，Leader 应该找到自己日志中该任期的最后一个条目，然后从下一个位置开始同步。
					// 目的是快速跳过 Follower 中可能存在的该任期的所有冲突条目。
					rf.nextIndex[peer] = peerNextIndex
				}
			}
			// 立即重试，不等待心跳
			rf.mu.Unlock()
			continue
		}

		// 等待下一次心跳或新日志
		hasLogsToReplicate := rf.Log.hasLogsToReplicate(rf.nextIndex[peer])
		rf.mu.Unlock()

		if hasLogsToReplicate {
			// 如果有日志需要复制，立即继续处理下一个日志
			continue
		} else {
			// 如果没有日志需要复制，等待心跳间隔
			<-heartbeatTicker.C // 阻塞 x 毫秒后 heartbeatTicker 发出信号
		}
	}
}

func (rf *Raft) AppendEntriesHandler(e AppendEntriesEvent) {
	defer close(e.done)
	log.Printf("Term=%v 的 S%v 收到 Term=%v 的 S%v 的追加日志请求: %+v", rf.CurrentTerm, rf.me, e.args.Term, e.args.LeaderId, e.args)

	// 无论如何 term 应该先保证，这意味着一个时代的开始
	if e.args.Term < rf.CurrentTerm {
		e.reply.Success = false
		e.reply.Term = rf.CurrentTerm
		return
	}

	// 如果是更新到更高的任期，重置本地任期与投票；若同一任期，仅确认对方为Leader，不要清空投票，避免同任期重复投票
	if e.args.Term > rf.CurrentTerm {
		rf.state = Follower
		rf.CurrentTerm = e.args.Term
		rf.leaderId = e.args.LeaderId
		rf.VotedFor = -1
		rf.persist()
	} else {
		// e.args.Term == rf.CurrentTerm
		rf.state = Follower
		rf.leaderId = e.args.LeaderId
	}

	e.reply.Term = rf.CurrentTerm
	prevLogIndex, prevLogTerm, leaderCommitIndex := e.args.PrevLogIndex, e.args.PrevLogTerm, e.args.LeaderCommitIndex
	latestLogIndex, _ := rf.Log.LastEntry()
	entries := rf.Log.Entries
	// 只要收到有效任期的 AppendEntries，就应该重置选举计时器
	rf.resetElectionTimer(250, 400)

	if !rf.Log.isSameLogEntry(prevLogIndex, prevLogTerm) {
		log.Printf("S%v 日志不一致，entries=%+v", rf.me, rf.Log.Entries)
		e.reply.Success = false

		// prevLogIndex 超出 Follower 日志范围
		if prevLogIndex > latestLogIndex {
			e.reply.XTerm = -1
			e.reply.XIndex = -1
			e.reply.XLen = len(entries)
			return
		}

		// 优化，按 term 回溯
		// 找到发生冲突的 term 的首个日志 index
		entry := rf.Log.getEntry(prevLogIndex)
		ConflictTerm := entry.Term
		e.reply.XTerm = ConflictTerm
		for i := prevLogIndex - rf.Log.Index0; i >= 0; i-- {
			log.Printf("S%v 回溯日志, i=%v, entry=%v, PrevLogTerm=%v", rf.me, i, entry, prevLogTerm)
			//log.Printf("S%v 回溯日志, i=%v,  =%v", rf.me, i, prevLogTerm)
			if entries[i-1].Term != ConflictTerm {
				e.reply.XIndex = i + rf.Log.Index0
				// 立即删除冲突的日志
				prevLogRelativeIndex := e.reply.XIndex - rf.Log.Index0
				rf.Log.Entries = entries[:prevLogRelativeIndex]
				break
			}
		}
		log.Printf("S%v 回溯日志, XTerm=%v, XIndex=%v", rf.me, entry.Term, e.reply.XIndex)
		return
	}

	// 获取截断日志的相对索引，e.args.PrevLogIndex >= rf.commitIndex 确保不会擦除提交的日志
	if len(e.args.Entries) > 0 {
		prevLogRelativeIndex := e.args.PrevLogIndex - rf.Log.Index0
		// 追加日志/覆盖发生冲突的日志
		rf.Log.Entries = entries[:prevLogRelativeIndex+1]
		// 追加日志
		rf.AppendEntry(e.args.Entries...)
		// 经历了删除、追加日志，重新计算最新日志 index
		latestLogIndex, _ = rf.Log.LastEntry()
	}
	log.Printf("Follower S%v 追加日志后: %+v, latestLogIndex: %v", rf.me, rf.Log.Entries, latestLogIndex)
	e.reply.Success = true
	// 尝试提交日志
	rf.followerCommitAndApplyLog(leaderCommitIndex, latestLogIndex)
}

func (rf *Raft) leaderCommitAndApplyLog(index, term int) {
	// 只有提交当前 term 日志时，才能采用大多数副本的方式，否则可能会被覆盖
	if rf.CurrentTerm == term && index > rf.commitIndex {
		majority := 1
		for _, matchIndex := range rf.matchIndex {
			if matchIndex >= index {
				majority++
			}
		}
		log.Printf("leader S%v 追加日志响应 majority=%v", rf.me, majority)
		// rf.commitIndex < index 是为了确保，达到 f+1 时更新了commitIndex，然后在剩余的响应回来时又来重置 commitIndex，这样在并发情况下可能存在问题
		if majority > len(rf.peers)/2 {
			// 提交追加的日志，即持久化
			rf.commitIndex = index
			log.Printf("leader S%v 提交日志 commitIndex=%v", rf.me, rf.commitIndex)
			// 尝试应用到状态机
			rf.applyLog()
		}
	}
}

func (rf *Raft) followerCommitAndApplyLog(leaderCommitIndex, latestLogIndex int) {
	if rf.commitIndex < leaderCommitIndex {
		rf.commitIndex = min(leaderCommitIndex, latestLogIndex)
		log.Printf("Follower S%v 提交日志 commitIndex=%+v", rf.me, rf.commitIndex)
		rf.applyLog()
	}
}

// 如果 commitIndex > lastApplied 所有机器都应该应用日志，注意应用日志时，要按顺序
func (rf *Raft) applyLog() {
	// 只要 commitIndex > lastApplied，就可以应用到状态机
	if rf.commitIndex > rf.lastApplied {
		//log.Printf("S%v 开始应用日志, 现有 logs: %+v, lastApplied=%v, commitIndex=%v", rf.me, rf.Log.Entries, rf.lastApplied, rf.commitIndex)
		log.Printf("S%v 开始应用日志, lastApplied=%v, commitIndex=%v", rf.me, rf.lastApplied, rf.commitIndex)
		// 循环应用，因为不支持批量应用
		willApply := rf.lastApplied + 1
		// 截取日志
		entries, startIndex, _ := rf.Log.slice(willApply, rf.commitIndex)
		// 应用日志时，要按顺序，这里有并发问题导致日志乱序
		go func() {
			for index, entry := range entries {
				rf.applyCh <- raftapi.ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: index + startIndex}
			}
		}()
		// 重要!! 直接更新 lastApplied，否则上面的计算+协程优化没有意义
		rf.lastApplied = rf.commitIndex
		log.Printf("S%v 应用日志: %+v, lastApplied=%v, commitIndex=%v", rf.me, entries, rf.lastApplied, rf.commitIndex)
		//log.Printf("S%v 应用日志: lastApplied=%v, commitIndex=%v", rf.me, rf.lastApplied, rf.commitIndex)
	}
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
	case AppendEntriesEvent:
		rf.AppendEntriesHandler(e)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 返回的只是 "开始处理" 的确认，不代表现在就提交了
func (rf *Raft) Start(command interface{}) (int, int, bool) {
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
	log.Printf("Leader S%v 收到 command: %+v", rf.me, command)
	isLeader = true
	entry := LogEntry{rf.CurrentTerm, command}
	// 立即追加日志
	index, term = rf.AppendEntry(entry)
	//log.Printf("leader S%v: 服务器追加日志后: %+v", rf.me, rf.Log.Entries)
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
	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
	}
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
		rf.resetElectionTimer(250, 400)
		rf.sendEvent(StartElectionEvent{})
	case Leader:
	default:
	}
}

// 使用 [start, end) 毫秒，重置超时计时器
func (rf *Raft) resetElectionTimer(start, end int) {
	// 停止计时器，如果返回 false 表示计时器已经触发
	if !rf.electionTimer.Stop() {
		// 尝试从通道中取出可能存在的待处理事件
		select {
		case <-rf.electionTimer.C: // 清空通道
		default: // 如果通道为空，则不阻塞
		}
	}
	rf.electionTimer.Reset(rf.randomElectionTimeout(start, end)) // 重置或设置
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
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.leaderId = -1
	rf.applyCh = applyCh
	// index 0 放入 term 0，确保初始的一致性检查总是成功
	rf.Log = Log{
		Index0:  0,
		Entries: []LogEntry{{Term: 0, Command: nil}}, // 初始一致性日志
	}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i, _ := range rf.peers {
		rf.nextIndex[i] = rf.Log.Index0 + 1
		rf.matchIndex[i] = 0
	}

	rf.state = Follower
	rf.eventChan = make(chan interface{}, 1000)
	rf.timerStopChan = make(chan struct{})
	rf.electionTimer = time.NewTimer(rf.randomElectionTimeout(250, 400)) // N 毫秒后发送一次消息，一次性
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("R[%d_%d] is now online.\n", rf.me, rf.CurrentTerm)

	go rf.eventLoop()

	return rf
}
