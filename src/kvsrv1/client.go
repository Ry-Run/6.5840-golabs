package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
// 返回当前值和版本，如果键不存在，则返回 ErrNoKey。遇到除 ErrNoKey 以外的所有错误，它都会一直重试。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}
	for {
		ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if reply.Err == rpc.OK {
			return reply.Value, reply.Version, reply.Err
		} else if reply.Err == rpc.ErrNoKey {
			return "", 0, rpc.ErrNoKey
		}
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
// 只要 Put 的版本与服务器上的版本匹配，就会用值更新键。如果版本号不匹配，服务器应该返回 ErrVersion。
// 如果 Put 在其第一个 RPC 上收到 ErrVersion，Put 应该返回 ErrVersion，因为 Put 绝对没有在服务器上执行。
// 如果服务器在重发 RPC 上返回 ErrVersion，那么 Put 必须返回 ErrMaybe 到应用程序，因为它前面的 RPC 可能已经由服务器成功处理，但响应丢失了，而 Clerk 不知道 Put 是否执行。
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}

	initial := true
	for {
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if !ok {
			initial = false
			continue
		}
		if !initial && reply.Err == rpc.ErrVersion {
			return rpc.ErrMaybe
		}
		return reply.Err
	}
}
