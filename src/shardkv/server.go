package shardkv

import (
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"../kvraft"
	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
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

const (
	TimeoutServerInterval = time.Duration(1 * time.Second)
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key                   string
	Value                 string
	OperationType         string
	ShardNumber           int
	ClientId              int64
	ClientOperationNumber int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead                    int32 // set by Kill()
	killCh                  chan bool
	shardmasterClerk        *shardmaster.Clerk
	shardInfo               [shardmaster.NShards]int
	ShardKV                 map[int]map[string]string
	seen                    map[int64]int
	applyChanMap            map[int]ApplyChanMapItem
	raftStateSizeToSnapshot int
}

type KVMapItem struct {
	err Err
	val string
}

type ApplyChanMapItem struct {
	ch                chan KVMapItem
	expectedOperation Op
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	if !kv.killed() {
		val, err := kv.startOp(args.Key, "", kvraft.GET, args.ClientInfo)
		reply.Value = val
		reply.Err = err
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if !kv.killed() {
		_, err := kv.startOp(args.Key, args.Value, args.Op, args.ClientInfo)
		reply.Err = err
		return
	}
}

func (kv *ShardKV) startOp(Key string, Value string, OpType string, ClientInfo ClientInformation) (string, Err) {
	op := Op{}
	op.Key = Key
	op.Value = Value
	op.OperationType = OpType
	op.ClientId = ClientInfo.ClientId
	op.ClientOperationNumber = ClientInfo.ClientOperationNumber
	shardNumber := key2shard(Key)
	op.ShardNumber = shardNumber
	kv.mu.Lock()
	if kv.shardInfo[shardNumber] != kv.gid {
		return "", ErrWrongGroup
	}
	kv.mu.Unlock()
	expectedIndex, _, isLeader := kv.rf.Start(op)
	if isLeader {
		kv.mu.Lock()
		DPrintf("%d-%d: listening for %s with expectedIndex %d and operation %v", kv.gid, kv.me, OpType, expectedIndex, op)
		msgCh := make(chan KVMapItem)
		kv.applyChanMap[expectedIndex] = ApplyChanMapItem{ch: msgCh, expectedOperation: op}
		kv.mu.Unlock()

		select {
		case <-time.After(TimeoutServerInterval):
			DPrintf("%d-%d: timed out waiting for message for %s with expectedIndex %d and operation %v", kv.gid, kv.me, OpType, expectedIndex, op)
			kv.mu.Lock()
			defer kv.mu.Unlock()
			delete(kv.applyChanMap, expectedIndex)

			return "", ErrWrongLeader
		case msg := <-msgCh:
			DPrintf("%d-%d: reply: %v, original op %v and opType %s", kv.gid, kv.me, msg, op, OpType)
			return msg.val, msg.err
		}
	} else {
		return "", ErrWrongLeader
	}
}

func (kv *ShardKV) modifyKV(command Op, commandType string, operation func(map[string]string, string, string)) {
	previousOperationNumber, ok := kv.seen[command.ClientId]
	if !ok || previousOperationNumber < command.ClientOperationNumber {
		if kv.ShardKV[command.ShardNumber] == nil {
			kv.ShardKV[command.ShardNumber] = make(map[string]string)
		}
		operation(kv.ShardKV[command.ShardNumber], command.Key, command.Value)
		kv.seen[command.ClientId] = command.ClientOperationNumber
	} else {
		DPrintf("%d-%d: skipped  message id %d %s from %d as we have already seen it. previous seen operation is %d ", kv.gid, kv.me,
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

func (kv *ShardKV) processApplyChMessage(msg raft.ApplyMsg) (string, Err) {
	if msg.CommandValid {
		DPrintf("%d-%d: Got message; commandIndex: %d, isSnapshot: %v; %v", kv.gid, kv.me, msg.CommandIndex, msg.IsSnapshot, msg)
		if msg.IsSnapshot {
			snapshot := msg.Command.(map[int]map[string]string)
			kv.ShardKV = CopyMap(snapshot)
			kv.seen = kvraft.CopyMapInt64(msg.Seen)
			DPrintf("%d-%d: after changing to snapshot we have %v", kv.gid, kv.me, kv.ShardKV)
		} else {
			command := msg.Command.(Op)
			commandType := command.OperationType

			switch commandType {
			case kvraft.PUT:
				kv.modifyKV(command, commandType, putToKV)
			case kvraft.APPEND:
				kv.modifyKV(command, commandType, appendToKV)
			case kvraft.GET:
				val, ok := kv.ShardKV[command.ShardNumber][command.Key]
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
		log.Printf("%d-%d: %v", kv.gid, kv.me, kv.ShardKV)

	} else {
		DPrintf("%d-%d: message skipped: %v", kv.gid, kv.me, msg)
	}
	return "", OK
}

func (kv *ShardKV) getMessages() {
	for {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			shardNumber := msg.Command.(Op).ShardNumber
			if kv.shardInfo[shardNumber] != kv.gid {
				// ignore messages not for this shard
				continue
			}
			val, err := kv.processApplyChMessage(msg)
			if !msg.IsSnapshot {
				command := msg.Command.(Op)
				index := msg.CommandIndex
				applyChanMapItem, ok := kv.applyChanMap[index]
				delete(kv.applyChanMap, index)
				if kv.maxraftstate != -1 && msg.StateSize >= kv.raftStateSizeToSnapshot {
					copy := CopyMap(kv.ShardKV)
					copyOfSeen := kvraft.CopyMapInt64(kv.seen)
					DPrintf("%d-%d: saving snapshot with lastIndex: %d; lastTerm: %d; seen: %v; data: %v", kv.gid, kv.me, index, msg.Term, copyOfSeen, copy)
					//go kv.sendSaveSnapshot(index, msg.Term, copy, copyOfSeen)
				}
				kv.mu.Unlock()
				if ok {
					kv.sendMessageToApplyChanMap(applyChanMapItem, command, val, err)
				}
			} else {
				kv.mu.Unlock()
			}
		case <-kv.killCh:
			return
		}
	}
}

func (kv *ShardKV) sendMessageToApplyChanMap(applyChanMapItem ApplyChanMapItem, command Op, val string, err Err) {
	messageCh := applyChanMapItem.ch
	expectedOperation := applyChanMapItem.expectedOperation
	var msg KVMapItem
	if command != expectedOperation {
		DPrintf("%d-%d: No Longer leader", kv.gid, kv.me)
		msg = KVMapItem{val: "", err: ErrWrongLeader}
	} else {
		msg = KVMapItem{val: val, err: err}
	}
	select {
	case messageCh <- msg:
		return
	default:
		log.Printf("%d-%d: tried to send message %v to apply channel, but it was not available for listening", kv.gid, kv.me, expectedOperation)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.

	// Your code here, if desired.
	close(kv.killCh)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func Equal(a, b [shardmaster.NShards]int) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func (kv *ShardKV) getConfig() {
	for {
		if !kv.killed() {
			kv.mu.Lock()
			newConfig := kv.shardmasterClerk.Query(-1)
			shardInfo := newConfig.Shards
			if !Equal(shardInfo, kv.shardInfo) {
				log.Printf("%d-%d, Reconfiguring", kv.gid, kv.me)
				kv.shardInfo = shardInfo
			}
			kv.mu.Unlock()
			time.Sleep(time.Duration(100 * time.Millisecond))
		} else {
			return
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.shardmasterClerk = shardmaster.MakeClerk(kv.masters)
	kv.killCh = make(chan bool)
	kv.ShardKV = make(map[int]map[string]string)
	kv.seen = make(map[int64]int)
	kv.applyChanMap = make(map[int]ApplyChanMapItem)
	kv.raftStateSizeToSnapshot = int(math.Trunc(float64(kv.maxraftstate) * 0.8))

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.getConfig()
	go kv.getMessages()

	return kv
}
