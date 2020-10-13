package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	PUT    = "Put"
	APPEND = "Append"
	GET    = "Get"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ClientId              int64
	ClientOperationNumber int
	Key                   string
	Value                 string
	Op                    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientId              int64
	ClientOperationNumber int
	Key                   string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
