package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key     string
	version rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, key: l, version: 0}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			err := lk.ck.Put(lk.key, "held", 0)
			if err == rpc.OK {
				lk.version = 1
				return
			} else {
				continue
			}
		}

		if err == rpc.OK && val == "free" {
			err := lk.ck.Put(lk.key, "held", ver)
			if err == rpc.OK {
				lk.version = ver + 1
				return
			} else {
				continue
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	err := lk.ck.Put(lk.key, "free", lk.version)
}
