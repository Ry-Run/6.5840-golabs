package raft

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

// 安装快照事件
type InstallSnapshotEvent struct {
	args  *InstallSnapshotArgs
	reply *InstallSnapshotReply
	done  chan struct{} // 阻塞 RPC 协程，避免事件还没处理，就返回调用者，导致调用者处理响应异常
}
