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
	Data                  map[string]string
	Config                shardmaster.Config
	OperationType         string
	ShardNumber           int
	ConfigNumber          int
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
	shardmasterConfig       shardmaster.Config
	ShardKV                 map[int]map[string]string
	seen                    map[int64]int
	applyChanMap            map[int]ApplyChanMapItem
	raftStateSizeToSnapshot int
	clientId                int64
	clientOperationNumber   int
	shardsLeftToReceive     map[int]bool
	shardsLeftToSend        map[int]int
}

type KVMapItem struct {
	err Err
	val string
}

type ApplyChanMapItem struct {
	ch                            chan KVMapItem
	expectedClientId              int64
	expectedClientOperationNumber int
}

func (kv *ShardKV) InstallShard(args *InstallShardArgs, reply *InstallShardReply) {
	if !kv.killed() {
		op := Op{}
		op.Data = args.Data
		op.OperationType = INSTALLSHARD
		op.ClientId = args.ClientInfo.ClientId
		op.ClientOperationNumber = args.ClientInfo.ClientOperationNumber
		op.ShardNumber = args.ShardNumber
		op.ConfigNumber = args.ConfigNumber
		kv.mu.Lock()
		log.Print("TRYING TO INSTALL SHARD", kv.gid, op.ConfigNumber, args.ShardNumber, kv.shardmasterConfig.Shards)
		if kv.cantRespondToShardNumber(args.ShardNumber) {
			kv.mu.Unlock()
			reply.Err = ErrWrongGroup
			return
		}
		reply.ConfigNumber = kv.shardmasterConfig.Num
		kv.mu.Unlock()
		_, err := kv.startOpBase(op)
		reply.Err = err
		return
	}
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

func (kv *ShardKV) cantRespondToShardNumber(shardNumber int) bool {
	if shardNumber != -1 && kv.shardmasterConfig.Shards[shardNumber] != kv.gid {
		return true
	} else {
		return false
	}
}

func (kv *ShardKV) waitingForShard(shardNumber int) bool {
	if kv.shardsLeftToReceive[shardNumber] {
		return true
	} else {
		return false
	}
}

func (kv *ShardKV) maybeWaitForShard(shardNumber int) {
	for {
		if !kv.killed() {
			log.Print("waiting!", kv.waitingForShard(shardNumber))
			if kv.waitingForShard(shardNumber) {
				kv.mu.Unlock()
				time.Sleep(time.Duration(100 * time.Millisecond))
				kv.mu.Lock()
			} else {
				kv.mu.Unlock()
				return
			}
		} else {
			return
		}
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
	if kv.cantRespondToShardNumber(shardNumber) {
		kv.mu.Unlock()
		return "", ErrWrongGroup
	}
	kv.maybeWaitForShard(shardNumber)
	return kv.startOpBase(op)
}

func (kv *ShardKV) startOpBase(op Op) (string, Err) {
	expectedIndex, _, isLeader := kv.rf.Start(op)
	if isLeader {
		kv.mu.Lock()
		DPrintf("%d-%d: listening for %s with expectedIndex %d and operation %v", kv.gid, kv.me, op.OperationType, expectedIndex, op)
		msgCh := make(chan KVMapItem)
		kv.applyChanMap[expectedIndex] = ApplyChanMapItem{ch: msgCh, expectedClientOperationNumber: op.ClientOperationNumber, expectedClientId: op.ClientId}
		kv.mu.Unlock()

		select {
		case <-time.After(TimeoutServerInterval):
			DPrintf("%d-%d: timed out waiting for message for %s with expectedIndex %d and operation %v", kv.gid, kv.me, op.OperationType, expectedIndex, op)
			kv.mu.Lock()
			defer kv.mu.Unlock()
			delete(kv.applyChanMap, expectedIndex)

			return "", ErrWrongLeader
		case msg := <-msgCh:
			DPrintf("%d-%d: reply: %v, original op %v and opType %s", kv.gid, kv.me, msg, op, op.OperationType)
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
			case SENDSHARDS:
				if kv.shardmasterConfig.Num < command.ConfigNumber {
					oldShards := kv.getShardsToHandle(kv.shardmasterConfig.Shards)
					newShards := kv.getShardsToHandle(command.Config.Shards)
					shardsToSend := kv.getShardsToSend(newShards, oldShards, command.Config.Shards)
					shardsToReceive := kv.getShardsToReceive(newShards, oldShards, command.Config.Shards, kv.shardmasterConfig.Num)
					for shard := range shardsToReceive {
						kv.shardsLeftToReceive[shard] = true
					}
					for shard, target := range shardsToSend {
						kv.shardsLeftToSend[shard] = target
					}
					kv.shardmasterConfig = command.Config
					if _, isLeader := kv.rf.GetState(); isLeader {
						allArgs := kv.makeInstallShardArgs()
						kv.sendShardsToNewHandlers(allArgs)
					}
					log.Printf("%d-%d, Reconfiguring from %v to %v; sending %v and receiving %v", kv.gid, kv.me, oldShards, newShards, shardsToSend, shardsToReceive)
				} else {
					log.Printf("%d-%d: Received out of order command we have configNumber %d but received command was %d", kv.gid, kv.me, kv.shardmasterConfig.Num, command.ConfigNumber)
				}
			case INSTALLSHARD:
				_, exists := kv.shardsLeftToReceive[command.ShardNumber]
				log.Print("LEFT", kv.shardsLeftToReceive)
				if kv.shardmasterConfig.Num == command.ConfigNumber && exists {
					for index, value := range command.Data {
						if kv.ShardKV[command.ShardNumber] == nil {
							kv.ShardKV[command.ShardNumber] = make(map[string]string)
						}
						kv.ShardKV[command.ShardNumber][index] = value
					}
					delete(kv.shardsLeftToReceive, command.ShardNumber)
					log.Printf("%d-%d, Received shard #%d with data %v; updated shardKv is %v", kv.gid, kv.me, command.ShardNumber, command.Data, kv.ShardKV)
				} else {
					log.Printf("%d-%d: Received out of order command we have configNumber %d but received command was %d", kv.gid, kv.me, kv.shardmasterConfig.Num, command.ConfigNumber)
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
			if kv.cantRespondToShardNumber(shardNumber) {
				// ignore messages not for this shard
				kv.mu.Unlock()
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
	expectedClientId := applyChanMapItem.expectedClientId
	expectedClientOperationNumber := applyChanMapItem.expectedClientOperationNumber
	var msg KVMapItem
	if command.ClientId != expectedClientId || command.ClientOperationNumber != expectedClientOperationNumber {
		DPrintf("%d-%d: No Longer leader", kv.gid, kv.me)
		msg = KVMapItem{val: "", err: ErrWrongLeader}
	} else {
		msg = KVMapItem{val: val, err: err}
	}
	select {
	case messageCh <- msg:
		return
	default:
		DPrintf("%d-%d: tried to send message %v: %v to apply channel, but it was not available for listening", kv.gid, kv.me, expectedClientId, expectedClientOperationNumber)
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

func (kv *ShardKV) getShardsToReceive(newShardsToHandle map[int]bool, oldShardsToHandle map[int]bool,
	currentShardInfo [shardmaster.NShards]int, oldConfigNumber int) map[int]bool {
	result := make(map[int]bool)
	if oldConfigNumber == 0 {
		return result
	}
	for index := range newShardsToHandle {
		_, ok := oldShardsToHandle[index]
		if !ok {
			result[index] = true
		}
	}
	return result
}

func (kv *ShardKV) getShardsToSend(newShardsToHandle map[int]bool, oldShardsToHandle map[int]bool, currentShardInfo [shardmaster.NShards]int) map[int]int {
	result := make(map[int]int)
	for index := range oldShardsToHandle {
		_, ok := newShardsToHandle[index]
		if !ok {
			result[index] = currentShardInfo[index]
		}
	}
	return result
}

func (kv *ShardKV) getShardsToHandle(shardInfo [shardmaster.NShards]int) map[int]bool {
	result := make(map[int]bool)
	for index, data := range shardInfo {
		if data == kv.gid {
			result[index] = true
		}
	}
	return result
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

func (kv *ShardKV) handleInstallShardResponse(args InstallShardArgs, reply InstallShardReply) {
	kv.mu.Lock()
	log.Printf("%d-%d: DELETING LEFT TO SEND %d %v; our config num %d; theirs %d", kv.gid, kv.me, args.ShardNumber, kv.shardsLeftToSend, kv.shardmasterConfig.Num, reply.ConfigNumber)
	if kv.shardmasterConfig.Num == reply.ConfigNumber {
		delete(kv.shardsLeftToSend, args.ShardNumber)
	}
	log.Printf("%d-%d: AFTER DELETING LEFT TO SEND %d %v", kv.gid, kv.me, args.ShardNumber, kv.shardsLeftToSend)

	kv.mu.Unlock()
}

func (kv *ShardKV) makeInstallShardArgs() map[string]InstallShardArgs {
	allArgs := make(map[string]InstallShardArgs)
	log.Printf("%d-%d: LEFT TO SEND %v", kv.gid, kv.me, kv.shardsLeftToSend)

	for shardNumber, gid := range kv.shardsLeftToSend {
		if servers, ok := kv.shardmasterConfig.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				args := InstallShardArgs{}
				args.ShardNumber = shardNumber
				args.Data = kv.ShardKV[shardNumber]
				args.ConfigNumber = kv.shardmasterConfig.Num
				args.ClientInfo.ClientId = kv.clientId
				args.ClientInfo.ClientOperationNumber = kv.clientOperationNumber
				allArgs[servers[si]] = args
			}
		}
	}
	return allArgs
}

func (kv *ShardKV) sendShardsToNewHandlers(allArgs map[string]InstallShardArgs) {
	for serverName, args := range allArgs {
		go func(serverName string, args InstallShardArgs) {
			srv := kv.make_end(serverName)
			reply := &InstallShardReply{}
			ok := srv.Call("ShardKV.InstallShard", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				kv.handleInstallShardResponse(args, *reply)
			}
		}(serverName, args)
	}
}

func (kv *ShardKV) getConfig() {
	for {
		if !kv.killed() {
			kv.mu.Lock()
			newConfig := kv.shardmasterClerk.Query(-1)
			if newConfig.Num > kv.shardmasterConfig.Num {
				log.Printf("%d-%d: start consensus for reconfiguring to config #%d from config #%d", kv.gid, kv.me, newConfig.Num, kv.shardmasterConfig.Num)
				op := Op{}
				op.OperationType = SENDSHARDS
				op.ClientId = kv.clientId
				op.ClientOperationNumber = kv.clientOperationNumber
				op.ConfigNumber = newConfig.Num
				op.Config = newConfig
				op.ShardNumber = -1
				kv.mu.Unlock()
				go kv.startOpBase(op)
			} else {
				kv.mu.Unlock()
			}
			time.Sleep(time.Duration(100 * time.Millisecond))
		} else {
			return
		}
	}
}

func (kv *ShardKV) sendInstallShardsWhenNeeded() {
	for {
		if !kv.killed() {
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.mu.Lock()
				log.Printf("%d-%d: CHECKING LEFT TO SEND %v", kv.gid, kv.me, kv.shardsLeftToSend)
				if len(kv.shardsLeftToSend) > 0 {
					allArgs := kv.makeInstallShardArgs()
					kv.mu.Unlock()
					kv.sendShardsToNewHandlers(allArgs)
				} else {
					kv.mu.Unlock()
				}
			}
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
	labgob.Register(InstallShardArgs{})
	labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.clientId = nrand()

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.shardmasterClerk = shardmaster.MakeClerk(kv.masters)
	kv.killCh = make(chan bool)
	kv.ShardKV = make(map[int]map[string]string)
	kv.seen = make(map[int64]int)
	kv.applyChanMap = make(map[int]ApplyChanMapItem)
	kv.raftStateSizeToSnapshot = int(math.Trunc(float64(kv.maxraftstate) * 0.8))
	kv.shardsLeftToReceive = make(map[int]bool)
	kv.shardsLeftToSend = make(map[int]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.getConfig()
	go kv.getMessages()
	go kv.sendInstallShardsWhenNeeded()

	return kv
}
