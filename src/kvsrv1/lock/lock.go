package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	l        string
	cid      string
	tversion rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.l = l
	lk.cid = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		// 键值服务器有一个标识位，多客户端就来竞争这个标识位 l
		value, tversion, err := lk.ck.Get(lk.l)
		if err == rpc.ErrNoKey {
			// 标识位未初始化
			lk.tversion = 0
		} else if value == "" {
			// 标识位被释放
			lk.tversion = tversion
		} else {
			// 标识位被占用，等待
			continue
		}

		// 尝试抢占标识位
		putErr := lk.ck.Put(lk.l, lk.cid, lk.tversion)
		if putErr == rpc.OK {
			// 抢占成功
			lk.tversion.Inc()
			return
		} else if putErr == rpc.ErrMaybe {
			// 可能执行成功，也可能没有执行成功，再检查一次标识位
			value, tversion, _ := lk.ck.Get(lk.l)
			if value == lk.cid {
				// 抢占成功
				lk.tversion = tversion
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	// 释放标识位
	lk.ck.Put(lk.l, "", lk.tversion)
}
