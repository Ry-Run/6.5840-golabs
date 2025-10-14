package raft

//
//import (
//	"log"
//	"time"
//
//	"6.5840/raftapi"
//)
//
//type LogEntry struct {
//	Term    int
//	Command interface{}
//}
//
//type Log struct {
//	Index0  int // 即快照后第一个日志的索引，相当于绝对位置
//	Entries []LogEntry
//}
//
//// return LastEntry's index and Term
//func (l *Log) LastEntry() (lastIndex int, lastTerm int) {
//	i := len(l.Entries) - 1
//	return l.Index0 + i, l.Entries[i].Term
//}
//
//// 从 index 开始追加日志，返回第一个日志所在位置和 term
//func (rf *Raft) AppendEntry(entries ...LogEntry) (int, int) {
//	rf.Log.Entries = append(rf.Log.Entries, entries...)
//	// 持久化一次 Entries
//	rf.persist()
//	return rf.Log.LastEntry()
//}
//
//// slice [index, lastIndex] 日志, index 是在整个 logs 中的绝对位置，返回 lastIndex 的绝对位置
//func (l *Log) sliceEnd(index int) (entries []LogEntry, lastIndex, lastTerm int) {
//	// index 不能比 leader 最后一个日志 index 大
//	lastIndex, lastTerm = l.LastEntry()
//	if index > lastIndex {
//		return []LogEntry{}, lastIndex, lastTerm
//	}
//
//	start := index - l.Index0
//	entries = l.Entries[start:]
//	return entries, lastIndex, lastTerm
//}
//
//// slice [start, end] 日志, start、end 是在整个 logs 中的绝对位置，返回 start 的绝对位置
//func (l *Log) slice(start, end int) (entries []LogEntry, startIndex, startTerm int) {
//	lastIndex, _ := l.LastEntry()
//
//	// todo 应该大于index0
//	if 0 <= start && start <= end && end <= lastIndex {
//		relativeStart := start - l.Index0
//		relativeEnd := end - l.Index0
//		entries = l.Entries[relativeStart : relativeEnd+1]
//		return entries, start, l.getEntry(start).Term
//	}
//
//	return []LogEntry{}, -1, -1
//}
//
//// 指定索引位置的日志条目，是否为同一个日志
//func (l *Log) isSameLogEntry(index, term int) bool {
//	entry := l.getEntry(index)
//	if entry.Term != term {
//		return false
//	}
//	return true
//}
//
//// index 位置的 entry（在整个 logs 的 index，即绝对位置） 和 term
//func (l *Log) getEntry(index int) (entry LogEntry) {
//	i := index - l.Index0
//	if i >= len(l.Entries) {
//		return LogEntry{Term: -1}
//	}
//	return l.Entries[i]
//}
//
//// 判断 index, term 这个日志日志是不是至少最新
//func (l *Log) isAtLeast(index, term int) bool {
//	// 检查候选人的日志是否至少和自己一样新
//	lastLogIndex, lastTerm := l.LastEntry()
//	// 候选人的最后日志任期更小
//	if term < lastTerm || term == lastTerm && index < lastLogIndex {
//		return false
//	}
//	return true
//}
//
//// 判断 index, term 这个日志日志是不是至少最新
//func (l *Log) hasLogsToReplicate(nextIndex int) bool {
//	lastIndex, _ := l.LastEntry()
//	return nextIndex <= lastIndex
//}
//
//type AppendEntriesArgs struct {
//	Term              int
//	LeaderId          int
//	PrevLogIndex      int
//	PrevLogTerm       int
//	Entries           []LogEntry
//	LeaderCommitIndex int
//}
//
//type AppendEntriesReply struct {
//	Term    int
//	Success bool
//	// 可选的优化
//	XTerm  int // 发生冲突的 entry 的 term
//	XIndex int // 发生冲突的 entry 的 term 的首个日志索引
//	XLen   int // Follower 的日志长度
//}
//
//func (rf *Raft) applyLog() {
//	for !rf.killed() {
//		rf.mu.Lock()
//		// 只要 commitIndex > lastApplied，就可以应用到状态机
//		if rf.commitIndex > rf.lastApplied {
//			//log.Printf("S%v 开始应用日志, 现有 logs: %+v, lastApplied=%v, commitIndex=%v", rf.me, rf.Log.Entries, rf.lastApplied, rf.commitIndex)
//			log.Printf("S%v 开始应用日志, lastApplied=%v, commitIndex=%v", rf.me, rf.lastApplied, rf.commitIndex)
//			// 循环应用，因为不支持批量应用
//			willApply := rf.lastApplied + 1
//			// 截取日志
//			entries, _, _ := rf.Log.slice(willApply, rf.commitIndex)
//			// 应用日志时，要按顺序，这里有并发问题导致日志乱序
//			for index, entry := range entries {
//				rf.mu.Unlock()
//				rf.applyCh <- raftapi.ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: index + willApply}
//				rf.mu.Lock()
//				rf.lastApplied++
//			}
//			log.Printf("S%v 应用日志: %+v, lastApplied=%v, commitIndex=%v", rf.me, entries, rf.lastApplied, rf.commitIndex)
//			//log.Printf("S%v 应用日志: lastApplied=%v, commitIndex=%v", rf.me, rf.lastApplied, rf.commitIndex)
//		}
//		rf.mu.Unlock()
//		time.Sleep(10 * time.Millisecond)
//	}
//}
//
//func (rf *Raft) replicateLog(peer int) {
//	heartbeatTicker := time.NewTicker(50 * time.Millisecond) // 心跳间隔
//	defer heartbeatTicker.Stop()
//
//	for !rf.killed() {
//		rf.mu.Lock()
//		if rf.state != Leader {
//			rf.mu.Unlock()
//			return
//		}
//
//		// 检查是否有日志需要复制
//		nextIndex := rf.nextIndex[peer]
//		var s string
//
//		// 准备参数
//		var entries []LogEntry
//
//		// 有日志时发生日志，空闲时发送心跳
//		if rf.Log.hasLogsToReplicate(nextIndex) {
//			// 有日志需要复制
//			entries, _, _ = rf.Log.sliceEnd(nextIndex)
//			s = "日志"
//		} else {
//			// 心跳负责保持地位、确认日志一致性
//			entries = []LogEntry{}
//			s = "心跳"
//		}
//
//		PrevLogIndex := max(nextIndex-1, 0)
//		PrevLogTerm := rf.Log.getEntry(PrevLogIndex).Term
//
//		log.Printf("S%v 发送%s到 S%v, PrevLogIndex: %v, PrevLogTerm: %v, commitIndex:%v", rf.me, s, peer, PrevLogIndex, PrevLogTerm, rf.commitIndex)
//		args := AppendEntriesArgs{rf.CurrentTerm, rf.me, PrevLogIndex, PrevLogTerm, entries, rf.commitIndex}
//
//		// 阻塞调用前解锁
//		rf.mu.Unlock()
//		//  发送 AppendEntries 请求
//		var reply AppendEntriesReply
//		ok := rf.sendAppendEntries(peer, &args, &reply)
//
//		if !ok {
//			// RPC 失败，等待一段时间后重试
//			log.Printf("S%v 发送%s RPC 到 S%v 失败，等待一段时间后重试", rf.me, s, peer)
//			continue
//		}
//
//		// 处理响应
//		rf.mu.Lock()
//
//		log.Printf("leader S%v 收到 S%v 的追加日志响应: %+v", rf.me, peer, reply)
//		// 如果 follower 任期比我大，则主动退位
//		if reply.Term > rf.CurrentTerm {
//			log.Printf("S%v 的 term 更大，Leader S%v 退位", peer, rf.me)
//			rf.state = Follower
//			rf.CurrentTerm = reply.Term
//			rf.VotedFor = -1
//			rf.leaderId = -1
//			// CurrentTerm 变化，持久化一次
//			rf.persist()
//			rf.mu.Unlock()
//			return // todo 如果直接使用replicateLog，这里应该是 continue
//		}
//
//		// 只处理当前 term 的响应
//		if reply.Term != rf.CurrentTerm || rf.state != Leader {
//			rf.mu.Unlock()
//			return
//		}
//
//		if reply.Success {
//			commitIndex := PrevLogIndex + len(entries)
//			// 可以更新  nextIndex 和 matchIndex
//			rf.nextIndex[peer] = commitIndex + 1
//			rf.matchIndex[peer] = commitIndex
//			log.Printf("Leader S%v 设置 S%v 的 nextIndex=%v, matchIndex=%v, entries=%+v", rf.me, peer, commitIndex+1, commitIndex, rf.Log.Entries)
//			//log.Printf("Leader S%v 设置 S%v 的 nextIndex=%v, matchIndex=%v", rf.me, peer, PrevLogIndex + 1, PrevLogIndex)
//			rf.leaderCommitAndApplyLog(commitIndex, rf.Log.getEntry(commitIndex).Term)
//		} else {
//			conflictTerm := reply.XTerm
//			if conflictTerm == -1 {
//				// case 3: Follower 的日志太短
//				rf.nextIndex[peer] = max(reply.XLen, 1)
//				log.Printf("Leader S%v 设置 S%v 的 nextIndex=%v", rf.me, peer, max(reply.XLen, 1))
//			} else {
//				peerNextIndex := -1
//				for i := len(rf.Log.Entries); i > 0; i-- {
//					if rf.Log.Entries[i-1].Term == conflictTerm {
//						peerNextIndex = i + rf.Log.Index0
//						break
//					}
//				}
//
//				if peerNextIndex == -1 {
//					// case 1: Leader 没有 XTerm: 意味着 Leader 的日志在这个任期上是"缺失"的
//					// 应该直接回退到 Follower 中该任期开始的位置（XIndex），从这里开始同步日志，覆盖掉 Follower 该任期的日志。
//					rf.nextIndex[peer] = reply.XIndex
//				} else {
//					// case 2: Leader 有 XTerm
//					// 如果 Leader 有相同的任期，但日志内容可能不同，Leader 应该找到自己日志中该任期的最后一个条目，然后从下一个位置开始同步。
//					// 目的是快速跳过 Follower 中可能存在的该任期的所有冲突条目。
//					rf.nextIndex[peer] = peerNextIndex
//				}
//			}
//			// 立即重试，不等待心跳
//			rf.mu.Unlock()
//			continue
//		}
//
//		// 等待下一次心跳或新日志
//		hasLogsToReplicate := rf.Log.hasLogsToReplicate(rf.nextIndex[peer])
//		rf.mu.Unlock()
//
//		if hasLogsToReplicate {
//			// 如果有日志需要复制，立即继续处理下一个日志
//			continue
//		} else {
//			// 如果没有日志需要复制，等待心跳间隔
//			<-heartbeatTicker.C // 阻塞 x 毫秒后 heartbeatTicker 发出信号
//		}
//	}
//}
//
//func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
//	log.Printf("Term=%v 的 S%v 收到 Term=%v 的 S%v 的追加日志请求: %+v", rf.CurrentTerm, rf.me, args.Term, args.LeaderId, args)
//
//	// 无论如何 term 应该先保证，这意味着一个时代的开始
//	if args.Term < rf.CurrentTerm {
//		reply.Success = false
//		reply.Term = rf.CurrentTerm
//		return
//	}
//
//	if args.Term >= rf.CurrentTerm {
//		rf.state = Follower
//		rf.CurrentTerm = args.Term
//		rf.leaderId = args.LeaderId
//		rf.VotedFor = args.LeaderId
//		rf.persist()
//	}
//
//	reply.Term = rf.CurrentTerm
//	prevLogIndex, prevLogTerm, leaderCommitIndex := args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommitIndex
//	latestLogIndex, _ := rf.Log.LastEntry()
//	entries := rf.Log.Entries
//	// 只要收到有效任期的 AppendEntries，就应该重置选举计时器
//	rf.resetElectionTimer()
//
//	if !rf.Log.isSameLogEntry(prevLogIndex, prevLogTerm) {
//		log.Printf("S%v 日志不一致，entries=%+v", rf.me, rf.Log.Entries)
//		reply.Success = false
//
//		// prevLogIndex 超出 Follower 日志范围
//		if prevLogIndex > latestLogIndex {
//			reply.XTerm = -1
//			reply.XIndex = -1
//			reply.XLen = len(entries)
//			return
//		}
//
//		// 优化，按 term 回溯
//		// 找到发生冲突的 term 的首个日志 index
//		entry := rf.Log.getEntry(prevLogIndex)
//		ConflictTerm := entry.Term
//		reply.XTerm = ConflictTerm
//		for i := prevLogIndex - rf.Log.Index0; i >= 0; i-- {
//			log.Printf("S%v 回溯日志, i=%v, entry=%v, PrevLogTerm=%v", rf.me, i, entry, prevLogTerm)
//			//log.Printf("S%v 回溯日志, i=%v,  =%v", rf.me, i, prevLogTerm)
//			if entries[i-1].Term != ConflictTerm {
//				reply.XIndex = i + rf.Log.Index0
//				// 立即删除冲突的日志
//				prevLogRelativeIndex := reply.XIndex - rf.Log.Index0
//				rf.Log.Entries = entries[:prevLogRelativeIndex]
//				break
//			}
//		}
//		log.Printf("S%v 回溯日志, XTerm=%v, XIndex=%v", rf.me, entry.Term, reply.XIndex)
//		return
//	}
//
//	// 获取截断日志的相对索引，args.PrevLogIndex >= rf.commitIndex 确保不会擦除提交的日志
//	if len(args.Entries) > 0 {
//		prevLogRelativeIndex := args.PrevLogIndex - rf.Log.Index0
//		// 追加日志/覆盖发生冲突的日志
//		rf.Log.Entries = entries[:prevLogRelativeIndex+1]
//		// 追加日志
//		rf.AppendEntry(args.Entries...)
//		// 经历了删除、追加日志，重新计算最新日志 index
//		latestLogIndex, _ = rf.Log.LastEntry()
//	}
//	log.Printf("Follower S%v 追加日志后: %+v, latestLogIndex: %v", rf.me, rf.Log.Entries, latestLogIndex)
//	reply.Success = true
//	// 尝试提交日志
//	if rf.commitIndex < leaderCommitIndex {
//		rf.commitIndex = min(leaderCommitIndex, latestLogIndex)
//		log.Printf("Follower S%v 提交日志 commitIndex=%+v", rf.me, rf.commitIndex)
//		// todo 可以广播允许提交日志了，当前没有这样实现
//	}
//}
//
//func (rf *Raft) leaderCommitAndApplyLog(index, term int) {
//	// 只有提交当前 term 日志时，才能采用大多数副本的方式，否则可能会被覆盖
//	if rf.CurrentTerm == term && index > rf.commitIndex {
//		majority := 1
//		for _, matchIndex := range rf.matchIndex {
//			if matchIndex >= index {
//				majority++
//			}
//		}
//		log.Printf("leader S%v 追加日志响应 majority=%v", rf.me, majority)
//		// rf.commitIndex < index 是为了确保，达到 f+1 时更新了commitIndex，然后在剩余的响应回来时又来重置 commitIndex，这样在并发情况下可能存在问题
//		if majority > len(rf.peers)/2 {
//			// 提交追加的日志，即持久化
//			rf.commitIndex = index
//			log.Printf("leader S%v 提交日志 commitIndex=%v", rf.me, rf.commitIndex)
//		}
//	}
//}
