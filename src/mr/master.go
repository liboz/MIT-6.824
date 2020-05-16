package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

const (
	NotSent    = iota // 0
	Processing = iota // 1
	Proccessed = iota // 2
)

type Master struct {
	TaskNumberToFileMap     map[int]string
	TaskNumberToFileMapLock sync.RWMutex
	JobSentMap              map[string]int
	JobSentMapLock          sync.RWMutex
	MapStepMap              map[string]int
	MapStepMapLock          sync.RWMutex
	AllFiles                []string
	nReduce                 int
	TaskNumber              int
	ReduceJobMap            map[int]int
	ReduceJobMapLock        sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetMapJob(args *MapJobRequest, reply *MapJobReply) error {
	m.JobSentMapLock.Lock()
	defer m.JobSentMapLock.Unlock()
	for _, value := range m.AllFiles {
		_, ok := m.JobSentMap[value]
		if !ok {
			m.JobSentMap[value] = m.TaskNumber

			m.MapStepMapLock.Lock()
			defer m.MapStepMapLock.Unlock()
			m.MapStepMap[value] = Processing

			m.TaskNumberToFileMapLock.Lock()
			defer m.TaskNumberToFileMapLock.Unlock()
			m.TaskNumberToFileMap[m.TaskNumber] = value
			reply.FileName = value
			reply.NReduce = m.nReduce
			reply.TaskNumber = m.TaskNumber
			m.TaskNumber += 1

			return nil
		}
	}
	return errors.New("no more map jobs available")
}

func (m *Master) ReportMapJobComplete(args *MapJobFinishRequest, reply *FinishRequestReply) error {
	taskNumber := args.TaskNumber
	fileName := m.TaskNumberToFileMap[taskNumber]
	m.MapStepMapLock.Lock()
	defer m.MapStepMapLock.Unlock()
	m.MapStepMap[fileName] = Proccessed
	return nil
}

func (m *Master) GetReduceJob(args *ReduceJobRequest, reply *ReduceJobReply) error {
	m.ReduceJobMapLock.Lock()
	defer m.ReduceJobMapLock.Unlock()
	for index, status := range m.ReduceJobMap {
		if status == NotSent {
			reply.TaskNumber = index
			m.ReduceJobMap[index] = Processing
			return nil
		}
	}
	return errors.New("no more reduce jobs available")
}

func (m *Master) ReportReduceJobComplete(args *ReduceJobFinishRequest, reply *FinishRequestReply) error {
	taskNumber := args.TaskNumber
	m.ReduceJobMapLock.Lock()
	defer m.ReduceJobMapLock.Unlock()
	m.ReduceJobMap[taskNumber] = Proccessed
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	log.Print("Listening at:", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.ReduceJobMapLock.RLock()
	defer m.ReduceJobMapLock.RUnlock()
	for _, status := range m.ReduceJobMap {
		if status != Proccessed {
			return false
		}
	}

	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.AllFiles = files
	m.JobSentMap = make(map[string]int)
	m.MapStepMap = make(map[string]int)
	m.TaskNumberToFileMap = make(map[int]string)
	m.ReduceJobMap = make(map[int]int)
	for i := 0; i < nReduce; i++ {
		m.ReduceJobMap[i] = NotSent
	}
	m.nReduce = nReduce
	m.TaskNumberToFileMapLock = sync.RWMutex{}
	m.JobSentMapLock = sync.RWMutex{}
	m.MapStepMapLock = sync.RWMutex{}
	m.ReduceJobMapLock = sync.RWMutex{}

	// Your code here.
	m.server()
	return &m
}
