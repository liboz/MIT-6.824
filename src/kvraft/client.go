package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

const (
	TimeoutInterval = time.Duration(1 * time.Second)
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeaderServer int
	id               int64
	operationNumber  int
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
	ck.id = nrand()
	ck.operationNumber = 0
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) getInitialServer() int {
	var initialServer int
	if ck.lastLeaderServer == -1 {
		initialServer = 0
	} else {
		initialServer = ck.lastLeaderServer
	}
	return initialServer
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
	ck.operationNumber += 1
	args := &GetArgs{}
	args.ClientId = ck.id
	args.ClientOperationNumber = ck.operationNumber
	args.Key = key

	responseCh := make(chan *GetReply)

	initialServer := ck.getInitialServer()
	for {

		for i := initialServer; i < initialServer+len(ck.servers); i++ {
			go func(i int) {
				reply := &GetReply{}
				ck.servers[i%len(ck.servers)].Call("KVServer.Get", args, reply)
				responseCh <- reply
			}(i)

			select {
			case <-time.After(TimeoutInterval):
				DPrintf("timing out Get request to %d", i)
				break
			case reply := <-responseCh:
				if reply.Err == OK {
					ck.lastLeaderServer = i % len(ck.servers)
					DPrintf("%d: Get success with %s:%s", ck.lastLeaderServer, key, reply.Value)
					return reply.Value
				}
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
	ck.operationNumber += 1
	args := &PutAppendArgs{}
	args.ClientId = ck.id
	args.ClientOperationNumber = ck.operationNumber
	args.Key = key
	args.Value = value
	args.Op = op
	initialServer := ck.getInitialServer()
	responseCh := make(chan *PutAppendReply)
	for {

		for i := initialServer; i < initialServer+len(ck.servers); i++ {
			go func(i int) {
				reply := &PutAppendReply{}
				DPrintf("Sending request from client to server %d", i)
				ck.servers[i%len(ck.servers)].Call("KVServer.PutAppend", args, reply)
				responseCh <- reply
			}(i)
			select {
			case <-time.After(TimeoutInterval):
				DPrintf("timing out PutAppend request to %d", i)
				break
			case reply := <-responseCh:
				DPrintf("client reply from %d: %v", i, reply)
				if reply.Err == OK {
					ck.lastLeaderServer = i % len(ck.servers)
					DPrintf("%d: %s success with %s:%s", ck.lastLeaderServer, op, key, value)
					return
				}
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
