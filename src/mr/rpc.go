package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type MapJobRequest struct {
}

type MapJobReply struct {
	FileName   string
	NReduce    int
	TaskNumber int
}

type MapJobFinishRequest struct {
	FileName string
}

type MapJobFinishReply struct {
}

type ReduceJobRequest struct {
}

type ReduceJobReply struct {
	TaskNumber int
}

type ReduceJobFinishRequest struct {
	TaskNumber int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
