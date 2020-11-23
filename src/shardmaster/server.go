package shardmaster

import (
	"log"
	"sort"
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

const (
	TimeoutServerInterval = time.Duration(1 * time.Second)
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	killCh       chan bool
	applyChanMap map[int]ApplyChanMapItem
	dead         int32 // set by Kill()
	seen         map[int64]int

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Data                  interface{}
	OperationType         string
	ClientId              int64
	ClientOperationNumber int
}

type SMMapItem struct {
	err         Err
	wrongLeader bool
	val         []Config
}

type ApplyChanMapItem struct {
	ch                            chan SMMapItem
	expectedClientId              int64
	expectedClientOperationNumber int
}

func copyConfig(original Config) Config {
	configCopy := Config{}
	configCopy.Num = original.Num
	configCopy.Groups = copyMap(original.Groups)
	return configCopy
}

func copyMap(original map[int][]string) map[int][]string {
	mapCopy := make(map[int][]string)
	for key, value := range original {
		var newSlice = make([]string, len(value))
		copy(newSlice, value)
		mapCopy[key] = newSlice
	}
	return mapCopy
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	if !sm.killed() {
		_, wrongLeader, err := sm.startOp(*args, JOIN, args.ClientInfo)
		reply.WrongLeader = wrongLeader
		reply.Err = err
		return
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	if !sm.killed() {
		_, wrongLeader, err := sm.startOp(*args, LEAVE, args.ClientInfo)
		reply.WrongLeader = wrongLeader
		reply.Err = err
		return
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	if !sm.killed() {
		_, wrongLeader, err := sm.startOp(*args, MOVE, args.ClientInfo)
		reply.WrongLeader = wrongLeader
		reply.Err = err
		return
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	if !sm.killed() {
		val, wrongLeader, err := sm.startOp(args.Num, QUERY, args.ClientInfo)
		if len(val) > 0 {
			reply.Config = val[0]
		}
		reply.WrongLeader = wrongLeader
		reply.Err = err
		return
	}
}

func (sm *ShardMaster) QueryHigher(args *QueryArgs, reply *QueryHigherReply) {
	if !sm.killed() {
		val, wrongLeader, err := sm.startOp(args.Num, QUERY_HIGHER, args.ClientInfo)
		reply.Configs = val
		reply.WrongLeader = wrongLeader
		reply.Err = err
		return
	}
}

func (sm *ShardMaster) startOp(data interface{}, operationType string, clientInfo ClientInformation) ([]Config, bool, Err) {
	op := Op{}
	op.Data = data
	op.OperationType = operationType
	op.ClientId = clientInfo.ClientId
	op.ClientOperationNumber = clientInfo.ClientOperationNumber
	expectedIndex, _, isLeader := sm.rf.Start(op)
	if isLeader {
		sm.mu.Lock()
		DPrintf("%d: listening expectedIndex %d and operation %v", sm.me, expectedIndex, op)
		msgCh := make(chan SMMapItem)
		sm.applyChanMap[expectedIndex] = ApplyChanMapItem{ch: msgCh, expectedClientOperationNumber: op.ClientOperationNumber, expectedClientId: op.ClientId}
		sm.mu.Unlock()

		select {
		case <-time.After(TimeoutServerInterval):
			DPrintf("%d: timed out waiting for message expectedIndex %d and operation %v", sm.me, expectedIndex, op)
			sm.mu.Lock()
			defer sm.mu.Unlock()
			delete(sm.applyChanMap, expectedIndex)

			return []Config{}, true, ""
		case msg := <-msgCh:
			DPrintf("%d: reply: %v, original op %v", sm.me, msg, op)
			return msg.val, msg.wrongLeader, msg.err
		}
	} else {
		return []Config{}, true, ""
	}
}

func (sm *ShardMaster) rebalance(newConfig *Config) {
	keys := make([]int, len(newConfig.Groups))
	i := 0
	for k := range newConfig.Groups {
		keys[i] = k
		i++
	}
	sort.Ints(keys)
	var newShards [NShards]int
	for index := range newShards {
		if len(keys) == 0 {
			newShards[index] = 0

		} else {
			newShards[index] = keys[index%len(keys)]
		}
	}
	newConfig.Shards = newShards
}

func (sm *ShardMaster) copyShards(newConfig *Config, oldConfig *Config) {
	var newShards [NShards]int
	for index := range newShards {
		newShards[index] = oldConfig.Shards[index]
	}
	newConfig.Shards = newShards
}

func (sm *ShardMaster) joinConfig(data interface{}) {
	var lastIndex = len(sm.configs) - 1
	var lastConfig = sm.configs[lastIndex]
	var newConfig = copyConfig(lastConfig)
	newConfig.Num += 1
	var convertedData = data.(JoinArgs)
	for index, data := range convertedData.Servers {
		newConfig.Groups[index] = data
	}
	sm.rebalance(&newConfig)
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) leaveConfig(data interface{}) {
	var lastIndex = len(sm.configs) - 1
	var lastConfig = sm.configs[lastIndex]
	var newConfig = copyConfig(lastConfig)
	newConfig.Num += 1
	var convertedData = data.(LeaveArgs)
	for _, data := range convertedData.GIDs {
		delete(newConfig.Groups, data)
	}
	sm.rebalance(&newConfig)
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) moveConfig(data interface{}) {
	var lastIndex = len(sm.configs) - 1
	var lastConfig = sm.configs[lastIndex]
	var newConfig = copyConfig(lastConfig)
	newConfig.Num += 1
	var convertedData = data.(MoveArgs)
	sm.copyShards(&newConfig, &lastConfig) // using this to copy the shards info
	newConfig.Shards[convertedData.Shard] = convertedData.GID
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) modifyConfig(command Op, commandType string, operation func(interface{})) {
	previousOperationNumber, ok := sm.seen[command.ClientId]
	if !ok || previousOperationNumber < command.ClientOperationNumber {
		DPrintf("%d: commandType %s, %v, before config length %d: %v", sm.me, commandType, command.Data, len(sm.configs), sm.configs[len(sm.configs)-1])
		operation(command.Data)
		sm.seen[command.ClientId] = command.ClientOperationNumber
		DPrintf("%d: commandType %s, %v, after config length %d: %v", sm.me, commandType, command.Data, len(sm.configs), sm.configs[len(sm.configs)-1])
	} else {
		DPrintf("%d: skipped  message id %d %s from %d as we have already seen it. previous seen operation is %d ", sm.me,
			command.ClientOperationNumber, commandType, command.ClientId, previousOperationNumber)
	}
}

func (sm *ShardMaster) processApplyChMessage(msg raft.ApplyMsg) ([]Config, Err) {
	if msg.CommandValid {
		DPrintf("%d: Got message; commandIndex: %d, isSnapshot: %v; %v", sm.me, msg.CommandIndex, msg.IsSnapshot, msg)

		command := msg.Command.(Op)
		commandType := command.OperationType

		switch commandType {
		case JOIN:
			sm.modifyConfig(command, commandType, sm.joinConfig)
		case LEAVE:
			sm.modifyConfig(command, commandType, sm.leaveConfig)
		case MOVE:
			sm.modifyConfig(command, commandType, sm.moveConfig)
		case QUERY:
			var convertedData = command.Data.(int)
			if convertedData == -1 || convertedData > len(sm.configs)-1 {
				return []Config{sm.configs[len(sm.configs)-1]}, OK
			} else {
				return []Config{sm.configs[convertedData]}, OK
			}
		case QUERY_HIGHER:
			var convertedData = command.Data.(int)
			result := []Config{}
			for i := convertedData + 1; i < len(sm.configs); i++ {
				result = append(result, sm.configs[i])
			}
			return result, OK
		default:
			DPrintf("this should not happen!!!!!!!!!!!!!!: %v", msg)
			panic("REALLY BAD")
		}

	} else {
		DPrintf("%d: message skipped: %v", sm.me, msg)
	}
	return []Config{}, OK
}

func (sm *ShardMaster) getMessages() {
	for {
		select {
		case msg := <-sm.applyCh:
			sm.mu.Lock()
			val, err := sm.processApplyChMessage(msg)
			command := msg.Command.(Op)
			index := msg.CommandIndex
			applyChanMapItem, ok := sm.applyChanMap[index]
			sm.mu.Unlock()
			if ok {
				sm.sendMessageToApplyChanMap(applyChanMapItem, command, val, err)
			}
		case <-sm.killCh:
			return
		}
	}
}

func (sm *ShardMaster) sendMessageToApplyChanMap(applyChanMapItem ApplyChanMapItem, command Op, val []Config, err Err) {
	messageCh := applyChanMapItem.ch
	expectedClientId := applyChanMapItem.expectedClientId
	expectedClientOperationNumber := applyChanMapItem.expectedClientOperationNumber
	var msg SMMapItem
	if command.ClientId != expectedClientId || command.ClientOperationNumber != expectedClientOperationNumber {
		DPrintf("%d: No Longer leader", sm.me)
		msg = SMMapItem{val: []Config{}, wrongLeader: true, err: err}
	} else {
		msg = SMMapItem{val: val, wrongLeader: false, err: err}
	}
	select {
	case messageCh <- msg:
		return
	default:
		DPrintf("%d: tried to send message %v: %v to apply channel, but it was not available for listening", sm.me, expectedClientId, expectedClientOperationNumber)
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sm.dead, 1)
	close(sm.killCh)
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.killCh = make(chan bool)
	sm.applyChanMap = make(map[int]ApplyChanMapItem)
	sm.seen = make(map[int64]int)

	go func() {
		sm.getMessages()
	}()

	return sm
}
