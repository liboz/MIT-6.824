package shardkv

import "../kvraft"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	INSTALL_SHARD          = "InstallShard"
	SEND_SHARDS            = "SendShards"
	INSTALL_SHARD_RESPONSE = "InstallShardResponse"
)

type ClientInformation struct {
	ClientId              int64
	ClientOperationNumber int
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientInfo ClientInformation
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientInfo ClientInformation
}

type GetReply struct {
	Err   Err
	Value string
}

type InstallShardArgs struct {
	ShardNumber  int
	Data         map[string]string
	ConfigNumber int
	ClientInfo   ClientInformation
	Seen         map[int64]int
}

type InstallShardReply struct {
	Err Err
}

func CopyMap(original map[int]map[string]string) map[int]map[string]string {
	copy := make(map[int]map[string]string)
	for key, value := range original {
		copy[key] = kvraft.CopyMap(value)
	}
	return copy
}

func CopyShardsToSend(original map[int]map[int][]string) map[int]map[int][]string {
	mapCopy := make(map[int]map[int][]string)
	for key, value := range original {
		mapCopy[key] = CopyShardsToSendMap(value)
	}
	return mapCopy
}

func CopyShardsToSendMap(original map[int][]string) map[int][]string {
	mapCopy := make(map[int][]string)
	for key, value := range original {
		arrayCopy := make([]string, len(value))
		copy(arrayCopy, value)
		mapCopy[key] = value
	}
	return mapCopy
}

func CopyShardsToReceive(original map[int]map[int]bool) map[int]map[int]bool {
	copy := make(map[int]map[int]bool)
	for key, value := range original {
		copy[key] = CopyShardsToReceiveMap(value)
	}
	return copy
}

func CopyShardsToReceiveMap(original map[int]bool) map[int]bool {
	copy := make(map[int]bool)
	for key, value := range original {
		copy[key] = value
	}
	return copy
}
