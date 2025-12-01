package raft

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
	Index0            int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// 可选的优化
	XTerm     int  // 发生冲突的 entry 的 term
	XIndex    int  // 发生冲突的 entry 的 term 的首个日志索引
	XLen      int  // Follower 的日志长度
	XSnapshot bool // 需要快照
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	done := make(chan struct{})
	rf.sendEvent(AppendEntriesEvent{args, reply, done})
	<-done // 等待事件处理完成
	// 问题是这里会一直阻塞，可能协程泄露，先简单这样实现
}

type InstallSnapshotArgs struct {
	Term              int    // leader 当前 term
	LeaderId          int    // 以便追随者可以重定向客户端
	LastIncludedIndex int    // 快照将替换包括此索引在内的所有条目
	LastIncludedTerm  int    // lastIncludedIndex 的 term
	Offset            int    // 快照文件中块位置的偏移量，切片传输使用，未实现
	Data              []byte // 快照块的原始字节，从偏移量开始
	IsLast            bool   // 如果这是最后一个块/后面还有块
}

type InstallSnapshotReply struct {
	Term int // 当前 term，供领导者自我更新
}

// 处理 InstallSnapshot RPC
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	done := make(chan struct{})
	rf.sendEvent(InstallSnapshotEvent{args, reply, done})
	<-done // 等待事件处理完成
	// 问题是这里会一直阻塞，可能协程泄露，先简单这样实现
}
