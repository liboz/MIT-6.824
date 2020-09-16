package kvraft

import (
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
	applyChanMap map[int]ApplyChanMapItem
	killCh       chan bool

	// Your definitions here.
}

type ApplyChanMapItem struct {
	ch                            chan KVMapItem
	expectedClientOperationNumber int
}

type KVMapItem struct {
	err Err
	val string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if !kv.killed() {
		op := Op{}
		op.Key = args.Key
		op.OperationType = GET
		op.ClientOperationNumber = args.ClientOperationNumber
		val, err := kv.startOp(op, "Get")
		reply.Value = val
		reply.Err = err
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if !kv.killed() {
		op := Op{}
		op.Key = args.Key
		op.Value = args.Value
		op.OperationType = args.Op
		op.ClientId = args.ClientId
		op.ClientOperationNumber = args.ClientOperationNumber
		_, err := kv.startOp(op, args.Op)
		reply.Err = err
		return
	}
}

func (kv *KVServer) startOp(op Op, OpType string) (string, Err) {
	kv.mu.Lock()
	expectedIndex, _, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()
	if isLeader {
		log.Printf("%d: listening for %s with expectedIndex %d and operation %v", kv.me, OpType, expectedIndex, op)
		log.Print("got lock before add to dict")
		msgCh := make(chan KVMapItem)
		kv.mu.Lock()
		kv.applyChanMap[expectedIndex] = ApplyChanMapItem{ch: msgCh, expectedClientOperationNumber: op.ClientOperationNumber}
		kv.mu.Unlock()
		log.Print("release lock after add to dict")

		select {
		case <-time.After(TimeoutInterval):
			log.Printf("%d: timed out waiting for message for %s with expectedIndex %d and operation %v", kv.me, OpType, expectedIndex, op)
			log.Print("got lock before deleting from dict in timeout")

			kv.mu.Lock()
			defer kv.mu.Unlock()
			delete(kv.applyChanMap, expectedIndex)
			log.Print("release lock deleting from dict in timeout")

			return "", ErrWrongLeader
		case msg := <-msgCh:
			log.Printf("%d: reply: %v", kv.me, msg)
			log.Print("got lock before deleting from dict")

			kv.mu.Lock()
			defer kv.mu.Unlock()
			delete(kv.applyChanMap, expectedIndex)
			log.Print("release lock deleting from dict")

			return msg.val, msg.err
		}
	} else {
		return "", ErrWrongLeader
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
		log.Printf("%d: Got message: %v", kv.me, msg)
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
		log.Printf("%d: %v", kv.me, kv.KV)
	} else {
		log.Printf("message skipped: %v", msg)
	}
	return "", OK
}

func (kv *KVServer) getMessages() {
	for {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			val, err := kv.processApplyChMessage(msg)
			kv.sendMessageToApplyChanMap(msg, val, err)
			kv.mu.Unlock()
		case <-kv.killCh:
			return
		}
	}
}

func (kv *KVServer) sendMessageToApplyChanMap(msg raft.ApplyMsg, val string, err Err) {
	command := msg.Command.(Op)
	index := msg.CommandIndex
	applyChanMapItem, ok := kv.applyChanMap[index]
	if ok {
		messageCh := applyChanMapItem.ch
		expectedClientOperationNumber := applyChanMapItem.expectedClientOperationNumber
		if command.ClientOperationNumber != expectedClientOperationNumber {
			log.Printf("%d: No Longer leader", kv.me)
			messageCh <- KVMapItem{val: "", err: ErrWrongLeader}
		} else {
			messageCh <- KVMapItem{val: val, err: err}
		}
	}
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
	kv.killCh <- true
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
	kv.applyChanMap = make(map[int]ApplyChanMapItem)
	kv.killCh = make(chan bool)
	go func() {
		kv.getMessages()
	}()

	return kv
}
