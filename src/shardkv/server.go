package shardkv

import (
	"bytes"
	"log"
	"math"
	"math/bits"
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

type ShardKVSnapshot struct {
	ShardKV             map[int]map[string]string
	ShardsLeftToReceive map[int]map[int]bool     // config num to shard
	ShardsLeftToSend    map[int]map[int][]string // config number to shard number to server to send to
	ConfigNumber        int
}

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
	Seen                  map[int64]int
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
	shardsLeftToReceive     map[int]map[int]bool     // config num to shard
	shardsLeftToSend        map[int]map[int][]string // config number to shard number to server to send to
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
		op.OperationType = INSTALL_SHARD
		op.ShardNumber = args.ShardNumber
		op.ConfigNumber = args.ConfigNumber
		op.Seen = args.Seen
		log.Printf("%d-%d: TRYING TO INSTALL SHARD on config #%d and shardNumber %d", kv.gid, kv.me, args.ConfigNumber, args.ShardNumber)
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
	waiting := false
	for _, shardMap := range kv.shardsLeftToReceive {
		if shardMap[shardNumber] {
			waiting = true
			break
		}
	}
	return waiting
}

func (kv *ShardKV) maybeWaitForShard(shardNumber int) {
	for {
		if !kv.killed() {
			log.Printf("%d-%d: waiting! %d %v %v", kv.gid, kv.me, shardNumber, kv.waitingForShard(shardNumber), kv.shardsLeftToReceive)
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
	op.ConfigNumber = kv.shardmasterConfig.Num
	kv.maybeWaitForShard(shardNumber)
	return kv.startOpBase(op)
}

func (kv *ShardKV) startOpBase(op Op) (string, Err) {
	expectedIndex, _, isLeader := kv.rf.Start(op)
	if isLeader {
		kv.mu.Lock()
		log.Printf("%d-%d: listening for %s with expectedIndex %d and operation %v", kv.gid, kv.me, op.OperationType, expectedIndex, op)
		msgCh := make(chan KVMapItem)
		kv.applyChanMap[expectedIndex] = ApplyChanMapItem{ch: msgCh, expectedClientOperationNumber: op.ClientOperationNumber, expectedClientId: op.ClientId}
		kv.mu.Unlock()

		select {
		case <-time.After(TimeoutServerInterval):
			log.Printf("%d-%d: timed out waiting for message for %s with expectedIndex %d and operation %v", kv.gid, kv.me, op.OperationType, expectedIndex, op)
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
		log.Printf("%d-%d: Got message; commandIndex: %d, isSnapshot: %v; %v", kv.gid, kv.me, msg.CommandIndex, msg.IsSnapshot, msg)
		if msg.IsSnapshot {
			var snapshot ShardKVSnapshot
			rawSnapshotData := msg.Command.([]byte)
			snapshotR := bytes.NewBuffer(rawSnapshotData)
			snapshotD := labgob.NewDecoder(snapshotR)
			snapshotD.Decode(&snapshot)

			kv.ShardKV = CopyMap(snapshot.ShardKV)
			kv.shardsLeftToReceive = CopyShardsToReceive(snapshot.ShardsLeftToReceive)
			kv.shardsLeftToSend = CopyShardsToSend(snapshot.ShardsLeftToSend)
			kv.shardmasterConfig = kv.shardmasterClerk.Query(snapshot.ConfigNumber)
			kv.seen = kvraft.CopyMapInt64(msg.Seen)
			DPrintf("%d-%d: after changing to snapshot we have %v", kv.gid, kv.me, kv.ShardKV)
		} else {
			command := msg.Command.(Op)
			commandType := command.OperationType

			switch commandType {
			case kvraft.PUT:
				if !kv.cantRespondToShardNumber(command.ShardNumber) {
					kv.modifyKV(command, commandType, putToKV)
				} else {
					return "", ErrWrongGroup
				}
			case kvraft.APPEND:
				if !kv.cantRespondToShardNumber(command.ShardNumber) {
					kv.modifyKV(command, commandType, appendToKV)
				} else {
					return "", ErrWrongGroup
				}
			case kvraft.GET:
				if !kv.cantRespondToShardNumber(command.ShardNumber) {
					val, ok := kv.ShardKV[command.ShardNumber][command.Key]
					if ok {
						return val, OK
					} else {
						return "", ErrNoKey
					}
				} else {
					return "", ErrWrongGroup
				}
			case SEND_SHARDS:
				if kv.shardmasterConfig.Num < command.ConfigNumber {
					oldShards := kv.getShardsToHandle(kv.shardmasterConfig.Shards)
					newShards := kv.getShardsToHandle(command.Config.Shards)
					shardsToSend := kv.getShardsToSend(newShards, oldShards, command.Config.Shards)
					shardsToReceive := kv.getShardsToReceive(newShards, oldShards, command.Config.Shards, command.ConfigNumber)
					if len(shardsToReceive) > 0 {
						kv.shardsLeftToReceive[command.Config.Num] = make(map[int]bool)
					}
					if len(shardsToSend) > 0 {
						kv.shardsLeftToSend[command.Config.Num] = make(map[int][]string)
					}
					for shard := range shardsToReceive {
						kv.shardsLeftToReceive[command.Config.Num][shard] = true
					}
					for shard, target := range shardsToSend {
						kv.shardsLeftToSend[command.Config.Num][shard] = command.Config.Groups[target]
					}
					log.Printf("%d-%d: Reconfiguring from config number #%d to #%d: %v to %v; sending %v and receiving %v", kv.gid, kv.me, kv.shardmasterConfig.Num, command.Config.Num, oldShards, newShards, shardsToSend, shardsToReceive)
					kv.shardmasterConfig = command.Config
					if _, isLeader := kv.rf.GetState(); isLeader {
						allArgs := kv.makeInstallShardArgs()
						kv.sendShardsToNewHandlers(allArgs)
					}
				} else {
					log.Printf("%d-%d: Received out of order command SENDSHARD we have configNumber %d but received command was %d", kv.gid, kv.me, kv.shardmasterConfig.Num, command.ConfigNumber)
				}
			case INSTALL_SHARD:
				_, exists := kv.shardsLeftToReceive[command.ConfigNumber]
				exists = exists && kv.shardsLeftToReceive[command.ConfigNumber][command.ShardNumber]
				_, minValueToSend := kv.findMinConfigNumberLeft()
				canInstall := false
				if minValueToSend == 0 || minValueToSend >= command.ConfigNumber {
					canInstall = true
				}
				log.Printf("%d-%d: LEFT%v; minValueToSend: %d command.ConfigNumber %d", kv.gid, kv.me, kv.shardsLeftToReceive, minValueToSend, command.ConfigNumber)
				if exists && canInstall {
					for index, value := range command.Data {
						if kv.ShardKV[command.ShardNumber] == nil {
							kv.ShardKV[command.ShardNumber] = make(map[string]string)
						}
						kv.ShardKV[command.ShardNumber][index] = value
					}
					delete(kv.shardsLeftToReceive[command.ConfigNumber], command.ShardNumber)
					if len(kv.shardsLeftToReceive[command.ConfigNumber]) == 0 {
						delete(kv.shardsLeftToReceive, command.ConfigNumber)
					}
					kv.mergeSeen(command.Seen)
					log.Printf("%d-%d: Received shard #%d config number %d with data %v; current config number is %d updated shardKv is %v", kv.gid, kv.me, command.ShardNumber, command.ConfigNumber, command.Data, kv.shardmasterConfig.Num, kv.ShardKV)
					log.Printf("%d-%d: shardsLeftToReceive is %v ", kv.gid, kv.me, kv.shardsLeftToReceive)
				} else if command.ConfigNumber > kv.shardmasterConfig.Num || !canInstall { // return failure if we still haven't updated our configs appropriately.
					log.Printf("%d-%d: Received and waiting to try again the out of order command INSTALLSHARD to install %d for config %d which is higher than our current config number %d", kv.gid, kv.me, command.ShardNumber, command.ConfigNumber, kv.shardmasterConfig.Num)
					return "", ErrWrongLeader
				} else {
					log.Printf("%d-%d: Received out of order command INSTALLSHARD to install %d we have configNumber %d but received command was %d", kv.gid, kv.me, command.ShardNumber, kv.shardmasterConfig.Num, command.ConfigNumber)
				}
			case INSTALL_SHARD_RESPONSE:
				log.Printf("%d-%d: DELETING LEFT TO SEND shard number: %d send config number: %d; %v", kv.gid, kv.me, command.ShardNumber, command.ConfigNumber, kv.shardsLeftToSend)
				if _, exists := kv.shardsLeftToSend[command.ConfigNumber]; exists {
					delete(kv.shardsLeftToSend[command.ConfigNumber], command.ShardNumber)
					if len(kv.shardsLeftToSend[command.ConfigNumber]) == 0 {
						delete(kv.shardsLeftToSend, command.ConfigNumber)
					}
					delete(kv.ShardKV, command.ShardNumber)
				}
				log.Printf("%d-%d: AFTER DELETING LEFT TO SEND %d %v", kv.gid, kv.me, command.ShardNumber, kv.shardsLeftToSend)
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

func (kv *ShardKV) mergeSeen(sentSeen map[int64]int) {
	for clientId, clientOperationNumber := range sentSeen {
		value, ok := kv.seen[clientId]
		if ok {
			kv.seen[clientId] = raft.Max(value, clientOperationNumber)
		} else {
			kv.seen[clientId] = clientOperationNumber
		}
	}
}

func (kv *ShardKV) sendSaveSnapshot(index int, term int, copyOfKV map[int]map[string]string,
	copyOfSeen map[int64]int, configNumber int, copyOfShardsLeftToSend map[int]map[int][]string, copyOfShardsLeftToReceive map[int]map[int]bool) {
	snapshotBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(snapshotBuffer)
	snapshot := ShardKVSnapshot{}
	snapshot.ShardKV = copyOfKV
	snapshot.ConfigNumber = configNumber
	snapshot.ShardsLeftToSend = copyOfShardsLeftToSend
	snapshot.ShardsLeftToReceive = copyOfShardsLeftToReceive

	encoder.Encode(snapshot)
	encodedSnapshot := snapshotBuffer.Bytes()
	kv.rf.SaveSnapshot(encodedSnapshot, index, term, copyOfSeen)
}

func (kv *ShardKV) getMessages() {
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
				if kv.maxraftstate != -1 && msg.StateSize >= kv.raftStateSizeToSnapshot {
					copy := CopyMap(kv.ShardKV)
					copyOfSeen := kvraft.CopyMapInt64(kv.seen)
					configNumber := kv.shardmasterConfig.Num
					copyOfShardsLeftToSend := CopyShardsToSend(kv.shardsLeftToSend)
					copyOfShardsLeftToReceive := CopyShardsToReceive(kv.shardsLeftToReceive)
					DPrintf("%d-%d: saving snapshot with lastIndex: %d; lastTerm: %d; seen: %v; data: %v", kv.gid, kv.me, index, msg.Term, copyOfSeen, copy)
					go kv.sendSaveSnapshot(index, msg.Term, copy, copyOfSeen, configNumber, copyOfShardsLeftToSend, copyOfShardsLeftToReceive)
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
		log.Printf("%d-%d: tried to send message %v: %v to apply channel, but it was not available for listening", kv.gid, kv.me, expectedClientId, expectedClientOperationNumber)
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
	log.Printf("%d-%d: has been killed", kv.gid, kv.me)

	close(kv.killCh)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) getShardsToReceive(newShardsToHandle map[int]bool, oldShardsToHandle map[int]bool,
	currentShardInfo [shardmaster.NShards]int, newConfigNumber int) map[int]bool {
	result := make(map[int]bool)
	if newConfigNumber == 1 {
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
	op := Op{}
	op.OperationType = INSTALL_SHARD_RESPONSE
	op.ConfigNumber = args.ConfigNumber
	op.ShardNumber = args.ShardNumber
	kv.startOpBase(op)
}

func (kv *ShardKV) findMinConfigNumberLeft() (int, int) {
	var minValueToReceive int
	if len(kv.shardsLeftToReceive) > 0 {
		minValueToReceive = (1<<bits.UintSize)/2 - 1
		for configNumber := range kv.shardsLeftToReceive {
			minValueToReceive = raft.Min(configNumber, minValueToReceive)
		}
	}
	var minValueToSend int
	if len(kv.shardsLeftToSend) > 0 {
		minValueToSend = (1<<bits.UintSize)/2 - 1
		for configNumber := range kv.shardsLeftToSend {
			minValueToSend = raft.Min(configNumber, minValueToSend)
		}
	}
	return minValueToReceive, minValueToSend
}

func (kv *ShardKV) makeInstallShardArgs() map[string][]InstallShardArgs {
	allArgs := make(map[string][]InstallShardArgs)
	minValueToReceive, minValueToSend := kv.findMinConfigNumberLeft()
	var configNumberToSend int
	if minValueToReceive == 0 {
		configNumberToSend = minValueToSend
	} else {
		configNumberToSend = raft.Min(minValueToSend, minValueToReceive)
	}

	log.Printf("%d-%d: LEFT TO SEND for config %d; minValueToSend %d, minValueToReceive %d; leftToSend: %v; left to receive: %v", kv.gid, kv.me, configNumberToSend, minValueToSend, minValueToReceive, kv.shardsLeftToSend, kv.shardsLeftToReceive)

	for shardNumber, servers := range kv.shardsLeftToSend[configNumberToSend] {
		// try each server for the shard.
		for si := 0; si < len(servers); si++ {
			args := InstallShardArgs{}
			args.ShardNumber = shardNumber
			args.Data = kvraft.CopyMap(kv.ShardKV[shardNumber])
			args.ConfigNumber = configNumberToSend
			args.Seen = kvraft.CopyMapInt64(kv.seen)
			if allArgs[servers[si]] == nil {
				allArgs[servers[si]] = []InstallShardArgs{}
			}
			allArgs[servers[si]] = append(allArgs[servers[si]], args)
		}
	}
	log.Print("ALLARGS:", allArgs)
	return allArgs
}

func (kv *ShardKV) sendShardsToNewHandlers(allArgs map[string][]InstallShardArgs) {
	for serverName, args := range allArgs {
		for _, arg := range args {
			go func(serverName string, arg InstallShardArgs) {
				srv := kv.make_end(serverName)
				reply := &InstallShardReply{}
				ok := srv.Call("ShardKV.InstallShard", &arg, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					kv.handleInstallShardResponse(arg, *reply)
				}
			}(serverName, arg)
		}
	}
}

func (kv *ShardKV) getConfig() {
	for {
		if !kv.killed() {
			kv.mu.Lock()
			currentConfigNum := kv.shardmasterConfig.Num
			kv.mu.Unlock()
			newConfig := kv.shardmasterClerk.Query(currentConfigNum + 1)
			if newConfig.Num > currentConfigNum {
				log.Printf("%d-%d: start consensus for reconfiguring to config #%d from config #%d", kv.gid, kv.me, newConfig.Num, currentConfigNum)
				op := Op{}
				op.OperationType = SEND_SHARDS
				op.ConfigNumber = newConfig.Num
				op.Config = newConfig
				op.ShardNumber = -1
				go kv.startOpBase(op)
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
	labgob.Register(ShardKVSnapshot{})

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
	kv.shardsLeftToReceive = make(map[int]map[int]bool)
	kv.shardsLeftToSend = make(map[int]map[int][]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.getConfig()
	go kv.getMessages()
	go kv.sendInstallShardsWhenNeeded()

	return kv
}
