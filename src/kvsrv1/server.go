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
	kvStore   map[string]string
	kvVersion map[string]rpc.Tversion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.kvStore = make(map[string]string)
	kv.kvVersion = make(map[string]rpc.Tversion)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.kvStore[args.Key]; !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = kv.kvStore[args.Key]
	reply.Version = kv.kvVersion[args.Key]
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

	version, ok := kv.kvVersion[args.Key]
	if !ok && args.Version != 0 {
		reply.Err = rpc.ErrNoKey
		return
	}

	if ok && version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	kv.kvStore[args.Key] = args.Value
	kv.kvVersion[args.Key] = args.Version + 1
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
