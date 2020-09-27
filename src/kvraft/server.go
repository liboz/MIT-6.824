package kvraft

import (
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

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

	maxraftstate            int // snapshot if log grows this big
	KV                      map[string]string
	seen                    map[int64]int
	applyChanMap            map[int]ApplyChanMapItem
	killCh                  chan bool
	raftStateSizeToSnapshot int
	saveSnapshotCh          chan SaveSnapshotChItem

	// Your definitions here.
}

type ApplyChanMapItem struct {
	ch                chan KVMapItem
	expectedOperation Op
}

type KVMapItem struct {
	err Err
	val string
}

type SaveSnapshotChItem struct {
	Term  int
	Index int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if !kv.killed() {
		op := Op{}
		op.Key = args.Key
		op.OperationType = GET
		op.ClientId = args.ClientId
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
	expectedIndex, _, isLeader := kv.rf.Start(op)
	if isLeader {
		kv.mu.Lock()
		DPrintf("%d: listening for %s with expectedIndex %d and operation %v", kv.me, OpType, expectedIndex, op)
		msgCh := make(chan KVMapItem)
		kv.applyChanMap[expectedIndex] = ApplyChanMapItem{ch: msgCh, expectedOperation: op}
		kv.mu.Unlock()

		select {
		case <-time.After(TimeoutInterval):
			DPrintf("%d: timed out waiting for message for %s with expectedIndex %d and operation %v", kv.me, OpType, expectedIndex, op)
			kv.mu.Lock()
			defer kv.mu.Unlock()
			delete(kv.applyChanMap, expectedIndex)

			return "", ErrWrongLeader
		case msg := <-msgCh:
			DPrintf("%d: reply: %v, original op %v and opType %s", kv.me, msg, op, OpType)
			return msg.val, msg.err
		}
	} else {
		return "", ErrWrongLeader
	}
}

func (kv *KVServer) modifyKV(command Op, commandType string, operation func(map[string]string, string, string)) {
	previousOperationNumber, ok := kv.seen[command.ClientId]
	if !ok || previousOperationNumber < command.ClientOperationNumber {
		operation(kv.KV, command.Key, command.Value)
		kv.seen[command.ClientId] = command.ClientOperationNumber
	} else {
		DPrintf("%d: skipped  message id %d %s from %d as we have already seen it. previous seen operation is %d ", kv.me,
			command.ClientOperationNumber, commandType, command.ClientId, previousOperationNumber)
	}
}

func putToKV(KV map[string]string, key string, value string) {
	KV[key] = value
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
		DPrintf("%d: Got message; isSnapshot: %v; %v", kv.me, msg.IsSnapshot, msg)
		if msg.IsSnapshot {
			snapshot := msg.Command.(map[string]string)
			kv.KV = copyMap(snapshot)
		} else {
			command := msg.Command.(Op)
			commandType := command.OperationType

			switch commandType {
			case PUT:
				kv.modifyKV(command, commandType, putToKV)
			case APPEND:
				kv.modifyKV(command, commandType, appendToKV)
			case GET:
				val, ok := kv.KV[command.Key]
				if ok {
					return val, OK
				} else {
					return "", ErrNoKey
				}
			default:
				DPrintf("this should not happen!!!!!!!!!!!!!!: %v", msg)
				panic("REALLY BAD")
			}
		}
		DPrintf("%d: %v", kv.me, kv.KV)

	} else {
		DPrintf("%d: message skipped: %v", kv.me, msg)
	}
	return "", OK
}

func (kv *KVServer) getMessages() {
	for {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			val, err := kv.processApplyChMessage(msg)
			if !msg.IsSnapshot {
				command := msg.Command.(Op)
				index := msg.CommandIndex
				applyChanMapItem, ok := kv.applyChanMap[index]
				delete(kv.applyChanMap, index)
				kv.mu.Unlock()
				if kv.maxraftstate != -1 && msg.StateSize >= kv.raftStateSizeToSnapshot {
					go kv.sendSaveSnapshotCh(index, msg.Term)
				}
				if ok {
					kv.sendMessageToApplyChanMap(applyChanMapItem, command, val, err)
				}
			}
		case <-kv.killCh:
			return
		}
	}
}

func (kv *KVServer) sendMessageToApplyChanMap(applyChanMapItem ApplyChanMapItem, command Op, val string, err Err) {
	messageCh := applyChanMapItem.ch
	expectedOperation := applyChanMapItem.expectedOperation
	var msg KVMapItem
	if command != expectedOperation {
		DPrintf("%d: No Longer leader", kv.me)
		msg = KVMapItem{val: "", err: ErrWrongLeader}
	} else {
		msg = KVMapItem{val: val, err: err}
	}
	select {
	case messageCh <- msg:
		return
	default:
		log.Printf("%d: tried to send message %v to apply channel, but it was not available for listening", kv.me, expectedOperation)
	}
}

func (kv *KVServer) sendSaveSnapshotCh(index int, term int) {
	saveSnapshotMsg := SaveSnapshotChItem{}
	saveSnapshotMsg.Index = index
	saveSnapshotMsg.Term = term
	select {
	case kv.saveSnapshotCh <- saveSnapshotMsg:
	case <-kv.killCh:
		return
	}
}

func copyMap(original map[string]string) map[string]string {
	copy := make(map[string]string)
	for key, value := range original {
		copy[key] = value
	}
	return copy
}

func (kv *KVServer) listenSaveSnapshotCh() {
	for {
		select {
		case msg := <-kv.saveSnapshotCh:
			kv.mu.Lock()
			copy := copyMap(kv.KV)
			log.Printf("%d: saving snapshot with lastIndex: %d; lastTerm: %d; data: %v", kv.me, msg.Index, msg.Term, copy)
			kv.mu.Unlock()
			kv.rf.SaveSnapshot(copy, msg.Index, msg.Term)
		case <-kv.killCh:
			return
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
	close(kv.killCh)
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
	kv.raftStateSizeToSnapshot = int(math.Trunc(float64(kv.maxraftstate) * 0.8))
	kv.saveSnapshotCh = make(chan SaveSnapshotChItem)

	go func() {
		kv.getMessages()
	}()

	go func() {
		kv.listenSaveSnapshotCh()
	}()

	return kv
}
