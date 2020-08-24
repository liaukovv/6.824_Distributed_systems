package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	executing  map[string]bool
	lastLeader int
	id         int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.executing = make(map[string]bool)
	ck.lastLeader = 0
	ck.id = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ret := ""
	i := ck.lastLeader
	numServ := len(ck.servers)
	token := getExecToken()
	fmt.Printf("Get called key: %v \n", key)
	for {
		server := i % numServ
		var args GetArgs
		var reply GetReply
		args.Key = key
		args.Token = token
		args.ClerkId = ck.id
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == OK {
				ret = reply.Value
				ck.lastLeader = server
				return ret
			} else if reply.Err == ErrNoKey {
				ret = ""
				ck.lastLeader = server
				return ret
			}
		}
		i = i + 1
	}

}

func getExecToken() string {
	return fmt.Sprint(nrand())
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	fmt.Printf("%v called key: %v value: %v \n", op, key, value)
	i := ck.lastLeader
	numServ := len(ck.servers)
	token := getExecToken()
	_, ok := ck.executing[token]
	if ok {
		return
	}

	//ck.executing[token] = true

	for {
		server := i % numServ
		var args PutAppendArgs
		var reply PutAppendReply
		args.Op = op
		args.Key = key
		args.Value = value
		args.Token = token
		args.ClerkId = ck.id
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				delete(ck.executing, token)
				ck.lastLeader = server
				return
			} else if reply.Err == ErrNoKey {
				delete(ck.executing, token)
				ck.lastLeader = server
				return
			}
		}
		i = i + 1
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
