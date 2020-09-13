package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeaderServer int
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
	ck.lastLeaderServer = -1
	// You'll have to add code here.
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
	args := &GetArgs{}
	args.Key = key
	for {
		var initialServer int
		if ck.lastLeaderServer == -1 {
			initialServer = 0
		} else {
			initialServer = ck.lastLeaderServer
		}
		for i := initialServer; i < initialServer+len(ck.servers); i++ {
			reply := &GetReply{}
			ok := ck.servers[i%len(ck.servers)].Call("KVServer.Get", args, reply)
			if ok && reply.Err == OK {
				ck.lastLeaderServer = i % len(ck.servers)
				log.Printf("%d: Get success with %s:%s", ck.lastLeaderServer, key, reply.Value)
				return reply.Value
			}
		}
		time.Sleep(time.Duration(100 * time.Millisecond))
	}
	return ""
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
	args := &PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	for {
		var initialServer int
		if ck.lastLeaderServer == -1 {
			initialServer = 0
		} else {
			initialServer = ck.lastLeaderServer
		}
		for i := initialServer; i < initialServer+len(ck.servers); i++ {
			reply := &PutAppendReply{}
			ok := ck.servers[i%len(ck.servers)].Call("KVServer.PutAppend", args, reply)
			if ok && reply.Err == OK {
				ck.lastLeaderServer = i % len(ck.servers)
				log.Printf("%d: %s success with %s:%s", ck.lastLeaderServer, op, key, value)
				return
			}
		}
		time.Sleep(time.Duration(100 * time.Millisecond))
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
