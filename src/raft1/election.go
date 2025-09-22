package raft

//import (
//	"log"
//	"math/rand"
//	"sync/atomic"
//	"time"
//)
//
//// example RequestVote RPC arguments structure.
//// field names must start with capital letters!
//type RequestVoteArgs struct {
//	// Your data here (3A, 3B).
//	Term         int
//	CandidateId  int
//	LastLogIndex int
//	LastLogTerm  int
//}
//
//// example RequestVote RPC reply structure.
//// field names must start with capital letters!
//type RequestVoteReply struct {
//	// Your data here (3A).
//	Term        int
//	VoteGranted bool
//}
//
//// example RequestVote RPC handler.
//func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
//	// Your code here (3A, 3B).
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	defer rf.persist()
//
//	// 候选人 term 比我小，直接拒绝
//	if args.Term < rf.CurrentTerm {
//		DPrintf("S%v(Term: %v), 拒绝给 S%v 投票，因为它的 term=%v, 比我小", rf.me, rf.CurrentTerm, args.CandidateId, args.Term)
//		reply.Term = rf.CurrentTerm
//		reply.VoteGranted = false
//		return
//	}
//
//	// 如果候选人的任期更大，更新自己的状态
//	if args.Term > rf.CurrentTerm {
//		rf.CurrentTerm = args.Term
//		rf.VotedFor = -1
//		rf.leaderId = -1
//		rf.state = Follower
//		// CurrentTerm、VotedFor 变化，持久化一次
//	}
//
//	reply.Term = rf.CurrentTerm
//	reply.VoteGranted = false
//	// raft 节点每个 Term 都只有一票，所以我已经在本轮投票给非请求者了，直接拒绝
//	if rf.VotedFor >= 0 && rf.VotedFor != args.CandidateId {
//		return
//	}
//
//	// 检查候选人的日志是否至少和自己一样新
//	lastLogIndex, lastTerm := rf.Log.LastEntry()
//	if args.LastLogTerm < lastTerm {
//		// 候选人的最后日志任期更小
//		return
//	} else if args.LastLogTerm == lastTerm && args.LastLogIndex < lastLogIndex {
//		// 任期相同但索引更小
//		return
//	} else {
//		// 候选人的日志足够新
//		// 当前 term 有票 && 请求者的日志是最新的，投给它
//		rf.state = Follower
//		rf.VotedFor = args.CandidateId
//		rf.leaderId = args.CandidateId
//		reply.VoteGranted = true
//		// 计时器重置
//		rf.resetElectionTimer(250, 400)
//		DPrintf("Term: %v, S%v 投票给 S%v, reply VoteGranted=%v", rf.CurrentTerm, rf.me, args.CandidateId, reply.VoteGranted)
//		// CurrentTerm、VotedFor 变化，持久化一次
//	}
//}
//
//// example code to send a RequestVote RPC to a server.
//// server is the index of the target server in rf.peers[].
//// expects RPC arguments in args.
//// fills in *reply with RPC reply, so caller should
//// pass &reply.
//// the types of the args and reply passed to Call() must be
//// the same as the types of the arguments declared in the
//// handler function (including whether they are pointers).
////
//// The labrpc package simulates a lossy network, in which servers
//// may be unreachable, and in which requests and replies may be lost.
//// Call() sends a request and waits for a reply. If a reply arrives
//// within a timeout interval, Call() returns true; otherwise
//// Call() returns false. Thus Call() may not return for a while.
//// A false return can be caused by a dead server, a live server that
//// can't be reached, a lost request, or a lost reply.
////
//// Call() is guaranteed to return (perhaps after a delay) *except* if the
//// handler function on the server side does not return.  Thus there
//// is no need to implement your own timeouts around Call().
////
//// look at the comments in ../labrpc/labrpc.go for more details.
////
//// if you're having trouble getting RPC to work, check that you've
//// capitalized all field names in structs passed over RPC, and
//// that the caller passes the address of the reply struct with &, not
//// the struct itself.
//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}
//
//func (rf *Raft) sendRequestVoteAndHandle(server int, args *RequestVoteArgs, voteGranted *int32) {
//	reply := &RequestVoteReply{}
//	ok := rf.sendRequestVote(server, args, reply)
//	if !ok {
//		return
//	}
//	rf.VoteResponseHandler(server, args, reply, voteGranted)
//}
//
//func (rf *Raft) VoteResponseHandler(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteGranted *int32) {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	defer rf.persist()
//
//	// 如果响应携带更新的任期，退回 Follower
//	if reply.Term > rf.CurrentTerm {
//		rf.CurrentTerm = reply.Term
//		rf.state = Follower
//		rf.VotedFor = -1
//		rf.leaderId = -1
//		rf.resetElectionTimer(250, 400)
//		return
//	}
//
//	// 防止被其他 term 响应修改
//	if rf.CurrentTerm != args.Term {
//		return
//	}
//
//	if rf.state != Candidate {
//		return
//	}
//
//	if !reply.VoteGranted {
//		return
//	}
//
//	if atomic.AddInt32(voteGranted, 1) > int32(len(rf.peers)/2) {
//		DPrintf("Term: %v, S%v 成为 Leader", rf.CurrentTerm, rf.me)
//		rf.state = Leader
//		rf.leaderId = rf.me
//		rf.VotedFor = -1
//
//		lastIndex, _ := rf.Log.LastEntry()
//		// 重置 nextIndex、matchIndex
//		for i, _ := range rf.peers {
//			if i != rf.me {
//				rf.nextIndex[i] = lastIndex + 1
//				rf.matchIndex[i] = 0
//				// 开始复制日志的协程
//				log.Printf("leader s%v 启动日志复制、心跳协程", rf.me)
//				go rf.replicateLog(i)
//			}
//		}
//		// todo 广播心跳协程，允许发送心跳
//	}
//}
//
//func (rf *Raft) ticker() {
//	for rf.killed() == false {
//
//		// Your code here (3A)
//		// Check if a leader election should be started.
//
//		// pause for a random amount of time between 50 and 350
//		// milliseconds.
//		rf.mu.Lock()
//		// 检查选举计时器是否超时
//		select {
//		case <-rf.electionTimer.C:
//			rf.mu.Lock()
//			// 计时器触发，处理选举超时
//			if rf.state != Leader {
//				rf.startElection()
//			}
//			rf.mu.Unlock()
//		default: // 计时器未触发，继续等待
//		}
//		rf.mu.Unlock()
//		time.Sleep(10 * time.Millisecond)
//	}
//}
//
//func (rf *Raft) startElection() {
//	rf.state = Candidate
//	rf.CurrentTerm++
//	rf.VotedFor = rf.me
//	DPrintf("Term: %v, S%v 开始选举，成为候选人", rf.CurrentTerm, rf.me)
//	// 进入新任期，CurrentTerm、VotedFor 变化，持久化一次
//	rf.persist()
//
//	index, term := rf.Log.LastEntry()
//	args := RequestVoteArgs{rf.CurrentTerm, rf.me, index, term}
//	voteGranted := int32(1)
//	for server := range rf.peers {
//		if server == rf.me {
//			continue
//		}
//		go rf.sendRequestVoteAndHandle(server, &args, &voteGranted)
//	}
//}
//
//// 使用 [start, end) 毫秒，重置超时计时器
//func (rf *Raft) resetElectionTimer(start, end int) {
//	// 停止计时器，如果返回 false 表示计时器已经触发
//	if !rf.electionTimer.Stop() {
//		// 尝试从通道中取出可能存在的待处理事件
//		select {
//		case <-rf.electionTimer.C: // 清空通道
//		default: // 如果通道为空，则不阻塞
//		}
//	}
//	rf.electionTimer.Reset(rf.randomElectionTimeout(start, end)) // 重置或设置
//}
