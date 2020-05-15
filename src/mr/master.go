package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const (
	NotSent    = iota // 0
	Processing = iota // 1
	Proccessed = iota // 2
)

type Master struct {
	TaskNumberToFileMap map[int]string
	JobSentMap          map[string]int
	MapStepMap          map[string]int
	AllFiles            []string
	nReduce             int
	TaskNumber          int
	ReduceJobMap        map[int]int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetJob(args *JobRequest, reply *JobReply) error {
	for _, value := range m.AllFiles {
		_, ok := m.JobSentMap[value]
		if !ok {
			m.JobSentMap[value] = m.TaskNumber
			m.MapStepMap[value] = Processing
			m.TaskNumberToFileMap[m.TaskNumber] = value
			reply.FileName = value
			reply.NReduce = m.nReduce
			reply.TaskNumber = m.TaskNumber
			m.TaskNumber += 1

			return nil
		}
	}
	return errors.New("no more jobs available")
}

func (m *Master) ReportMapJobComplete(args *MapJobFinishRequest, reply *FinishRequestReply) error {
	taskNumber := args.TaskNumber
	fileName := m.TaskNumberToFileMap[taskNumber]
	m.MapStepMap[fileName] = Proccessed
	return nil
}

func (m *Master) GetReduceJob(args *ReduceJobRequest, reply *ReduceJobReply) error {
	for index, status := range m.ReduceJobMap {
		if status == NotSent {
			reply.TaskNumber = index
			m.ReduceJobMap[index] = Processing
			return nil
		}
	}
	return errors.New("no more jobs available")
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

	// Your code here.
	log.Print(files)

	m.server()
	return &m
}
