package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	// key to (version to value)
	KV map[string]VerToVal
}

type VerToVal map[rpc.Tversion]string

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.mu = sync.Mutex{}
	kv.KV = make(map[string]VerToVal)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	verAndVal, ok := kv.KV[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	maxVersion := uint64(0)
	for version, _ := range verAndVal {
		u := uint64(version)
		if u > maxVersion {
			maxVersion = u
		}
	}

	ver := rpc.Tversion(maxVersion)
	reply.Value = verAndVal[ver]
	reply.Version = ver
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	verToVal, ok := kv.KV[args.Key]
	// key 不存在
	if !ok {
		if args.Version == 0 {
			verToVal = make(VerToVal)
			kv.KV[args.Key] = verToVal
			verToVal.Update(0, args.Value)
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
			return
		}
	} else {
		// key 存在
		_, ok := verToVal[args.Version]
		if !ok {
			reply.Err = rpc.ErrVersion
			return
		}
		verToVal.Update(args.Version, args.Value)
		reply.Err = rpc.OK
	}
}

func (verToVal VerToVal) Update(version rpc.Tversion, value string) {
	delete(verToVal, version)
	version.Inc()
	verToVal[version] = value
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
