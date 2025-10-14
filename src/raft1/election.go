package raft

//
//import (
//	"log"
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
//		rf.resetElectionTimer()
//		DPrintf("Term: %v, S%v 投票给 S%v, reply VoteGranted=%v", rf.CurrentTerm, rf.me, args.CandidateId, reply.VoteGranted)
//		// CurrentTerm、VotedFor 变化，持久化一次
//	}
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
//		rf.resetElectionTimer()
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
//		// todo 广播心跳协程，允许发送心跳，为了简单这里用不着这样实现
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
//			// 计时器触发，处理选举超时
//			if rf.state != Leader {
//				rf.startElection()
//			}
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
//func (rf *Raft) resetElectionTimer() {
//	// 停止计时器，如果返回 false 表示计时器已经触发
//	if !rf.electionTimer.Stop() {
//		// 尝试从通道中取出可能存在的待处理事件
//		select {
//		case <-rf.electionTimer.C: // 清空通道
//		default: // 如果通道为空，则不阻塞
//		}
//	}
//	rf.electionTimer.Reset(rf.randomElectionTimeout(250, 400)) // 重置或设置
//}
