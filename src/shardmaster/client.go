package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id               int64
	operationNumber  int
	lastLeaderServer int
}

type ResponseChItem struct {
	Id       int
	Response interface{}
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
	// Your code here.
	ck.id = nrand()
	ck.operationNumber = 0
	ck.lastLeaderServer = -1
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

func (ck *Clerk) Query(num int) Config {
	ck.operationNumber += 1
	args := &QueryArgs{}
	// Your code here.
	args.ClientInfo.ClientId = ck.id
	args.ClientInfo.ClientOperationNumber = ck.operationNumber
	args.Num = num
	responseCh := make(chan ResponseChItem)
	finishedCh := make(chan bool)
	initialServer := ck.getInitialServer()
	go func() {
		for {
			select {
			case <-finishedCh:
				return
			default:
				// try each known server.
				for i := initialServer; i < initialServer+len(ck.servers); i++ {
					go func(i int) {
						srv := ck.servers[i%len(ck.servers)]
						reply := &QueryReply{}
						ok := srv.Call("ShardMaster.Query", args, &reply)
						if ok {
							select {
							case <-time.After(time.Duration(50 * time.Millisecond)):
								return
							case responseCh <- ResponseChItem{Id: i, Response: reply}:
								return
							}
						}
					}(i)
					time.Sleep(100 * time.Millisecond) // wait 100ms before trying next server
				}
			}

		}
	}()

	for reply := range responseCh {
		if !reply.Response.(*QueryReply).WrongLeader {
			ck.lastLeaderServer = reply.Id % len(ck.servers)
			go func() {
				finishedCh <- true
			}()
			return reply.Response.(*QueryReply).Config
		}
	}
	return Config{}
}

func (ck *Clerk) QueryHigher(num int) []Config {
	ck.operationNumber += 1
	args := &QueryArgs{}
	// Your code here.
	args.ClientInfo.ClientId = ck.id
	args.ClientInfo.ClientOperationNumber = ck.operationNumber
	args.Num = num
	responseCh := make(chan ResponseChItem)
	finishedCh := make(chan bool)
	initialServer := ck.getInitialServer()
	go func() {
		for {
			select {
			case <-finishedCh:
				return
			default:
				// try each known server.
				for i := initialServer; i < initialServer+len(ck.servers); i++ {
					go func(i int) {
						srv := ck.servers[i%len(ck.servers)]
						reply := &QueryHigherReply{}
						ok := srv.Call("ShardMaster.QueryHigher", args, &reply)
						if ok {
							select {
							case <-time.After(time.Duration(50 * time.Millisecond)):
								return
							case responseCh <- ResponseChItem{Id: i, Response: reply}:
								return
							}
						}
					}(i)
					time.Sleep(100 * time.Millisecond) // wait 100ms before trying next server
				}
			}

		}
	}()

	for reply := range responseCh {
		if !reply.Response.(*QueryHigherReply).WrongLeader {
			ck.lastLeaderServer = reply.Id % len(ck.servers)
			go func() {
				finishedCh <- true
			}()
			return reply.Response.(*QueryHigherReply).Configs
		}
	}
	return []Config{}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.operationNumber += 1
	args := &JoinArgs{}
	// Your code here.
	args.ClientInfo.ClientId = ck.id
	args.ClientInfo.ClientOperationNumber = ck.operationNumber
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.operationNumber += 1
	args := &LeaveArgs{}
	// Your code here.
	args.ClientInfo.ClientId = ck.id
	args.ClientInfo.ClientOperationNumber = ck.operationNumber
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.operationNumber += 1
	args := &MoveArgs{}
	// Your code here.
	args.ClientInfo.ClientId = ck.id
	args.ClientInfo.ClientOperationNumber = ck.operationNumber
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && !reply.WrongLeader {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
