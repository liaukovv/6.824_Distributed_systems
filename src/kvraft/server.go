package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string // Get Put Append
	Token     string
	ClerkId   int64
}

type OpRes struct {
	ErrCode Err
	Value   string
	ClerkId int64
	Token   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store     map[string]string
	wait      map[int]chan OpRes
	prevEntry map[int64]string
}

func (kv *KVServer) AddToLog(op Op) OpRes {

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return OpRes{ErrCode: ErrWrongLeader}
	}
	kv.mu.Lock()
	if _, ok := kv.wait[index]; !ok {
		kv.wait[index] = make(chan OpRes, 1)
	}
	kv.mu.Unlock()
	select {
	case res := <-kv.wait[index]:
		if res.ClerkId == op.ClerkId && res.Token == op.Token {
			kv.mu.Lock()
			delete(kv.wait, index)
			newTerm, stillLeader := kv.rf.GetState()
			kv.mu.Unlock()
			if !stillLeader || term != newTerm {
				return OpRes{ErrCode: ErrWrongLeader}
			}
			return res
		}
		return OpRes{ErrCode: ErrWrongLeader}
	case <-time.After(1000 * time.Millisecond):
		kv.mu.Lock()
		delete(kv.wait, index)
		kv.mu.Unlock()
		return OpRes{ErrCode: ErrWrongLeader}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Key: args.Key, Operation: "Get", Token: args.Token, ClerkId: args.ClerkId}

	res := kv.AddToLog(op)

	reply.Err = res.ErrCode
	reply.Value = res.Value
	return

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{Key: args.Key, Value: args.Value, Operation: args.Op, Token: args.Token, ClerkId: args.ClerkId}
	fmt.Printf("%v : PutAppend %v \n", kv.me, args)
	res := kv.AddToLog(op)

	reply.Err = res.ErrCode
	return
}

func (kv *KVServer) Apply(op Op) OpRes {
	isDuplicate := false

	fmt.Printf("%v : Apply %v \n", kv.me, op)
	if token, ok := kv.prevEntry[op.ClerkId]; ok {
		if token == op.Token {
			isDuplicate = true
		}
	}

	res := OpRes{ErrCode: ErrWrongLeader, Token: op.Token, ClerkId: op.ClerkId}

	if op.Operation == "Put" {
		if !isDuplicate {
			kv.store[op.Key] = op.Value
			kv.prevEntry[op.ClerkId] = op.Token
		}
		res.ErrCode = OK

	} else if op.Operation == "Append" {
		if !isDuplicate {
			val, ok := kv.store[op.Key]
			if ok {
				kv.store[op.Key] = val + op.Value
			} else {
				kv.store[op.Key] = op.Value
			}
			kv.prevEntry[op.ClerkId] = op.Token
		}
		res.ErrCode = OK

	} else if op.Operation == "Get" {

		val, ok := kv.store[op.Key]
		if ok {
			res.Value = val
			res.ErrCode = OK
		} else {
			res.ErrCode = ErrNoKey
		}
	}

	return res
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	fmt.Println("Create server ", me)

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.wait = make(map[int]chan OpRes)
	kv.store = make(map[string]string)
	kv.prevEntry = make(map[int64]string)

	// You may need initialization code here.
	go func() {
		for {
			msg := <-kv.applyCh

			op := msg.Command.(Op)
			kv.mu.Lock()
			res := kv.Apply(op)
			resCh, ok := kv.wait[msg.CommandIndex]
			kv.mu.Unlock()

			if ok {

				resCh <- res
			}

		}
	}()

	return kv
}
