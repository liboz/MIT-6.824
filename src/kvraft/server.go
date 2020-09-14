package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

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

func DPrint(v ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Print(v...)
	}
	return
}

type Op struct {
	Key                   string
	Value                 string
	OperationType         string
	ClientId              int64
	ClientOperationNumber int
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	KV           map[string]string
	seen         map[int64]int

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if !kv.killed() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		op := Op{}
		op.Key = args.Key
		op.OperationType = GET
		op.ClientOperationNumber = args.ClientOperationNumber
		expectedIndex, _, isLeader := kv.rf.Start(op)
		if isLeader {
			log.Printf("%d: listening for Get with expectedIndex %d", kv.me, expectedIndex)
			val, err := kv.handleMessage(expectedIndex, args.ClientOperationNumber)
			log.Printf("%d: received Get message %v", kv.me, val)
			reply.Err = err
			reply.Value = val
			return
		} else {
			reply.Err = ErrWrongLeader
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if !kv.killed() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		op := Op{}
		op.Key = args.Key
		op.Value = args.Value
		op.OperationType = args.Op
		op.ClientId = args.ClientId
		op.ClientOperationNumber = args.ClientOperationNumber
		expectedIndex, _, isLeader := kv.rf.Start(op)
		if isLeader {
			log.Printf("%d: listening for %s with expectedIndex %d", kv.me, args.Op, expectedIndex)
			_, err := kv.handleMessage(expectedIndex, args.ClientOperationNumber)
			reply.Err = err
			return
		} else {
			reply.Err = ErrWrongLeader
			return
		}
	}
}

func appendToKV(KV map[string]string, key string, value string) {
	val, ok := KV[key]
	if ok {
		KV[key] = val + value
	} else {
		KV[key] = value
	}
}

func (kv *KVServer) processApplyChMessage(msg raft.ApplyMsg) (string, Err) {
	if msg.CommandValid {
		command := msg.Command.(Op)
		commandType := command.OperationType
		switch commandType {
		case PUT:
			kv.KV[command.Key] = command.Value
		case APPEND:
			previousOperationNumber, ok := kv.seen[command.ClientId]
			if !ok || previousOperationNumber < command.ClientOperationNumber {
				appendToKV(kv.KV, command.Key, command.Value)
				kv.seen[command.ClientId] = command.ClientOperationNumber
			} else {
				log.Printf("skipped message id %d as we have already seen it", command.ClientOperationNumber)
			}
		case GET:
			val, ok := kv.KV[command.Key]
			if ok {
				return val, OK
			} else {
				return "", ErrNoKey
			}
		default:
			log.Printf("this should not happen!!!!!!!!!!!!!!: %v", msg)
			panic("REALLY BAD")
		}
		log.Print(kv.KV)
	} else {
		log.Printf("message skipped: %v", msg)
	}
	return "", OK
}

func (kv *KVServer) handleMessage(expectedIndex int, expectedClientOperationNumber int) (string, Err) {
	var val string
	var err Err
	var msg raft.ApplyMsg
	for msg = range kv.applyCh {
		val, err = kv.processApplyChMessage(msg)
		if expectedIndex == msg.CommandIndex {
			break
		}
	}
	command := msg.Command.(Op)
	if command.ClientOperationNumber != expectedClientOperationNumber {
		log.Printf("%d: No Longer leader", kv.me)
		return "", ErrWrongLeader
	}
	return val, err
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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.KV = make(map[string]string)
	kv.seen = make(map[int64]int)

	return kv
}
