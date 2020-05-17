package mr

import (
	"errors"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"sync"
	"time"
)

const (
	NotSent    = iota // 0
	Processing = iota // 1
	Proccessed = iota // 2
)

type Master struct {
	TaskNumberToFileMap      map[int]string
	TaskNumberToFileMapLock  sync.RWMutex
	MapJobSentTimeMap        map[string]time.Time
	MapJobSentTimeMapLock    sync.RWMutex
	MapStepMap               map[string]int
	MapStepMapLock           sync.RWMutex
	MapJobCompleteChannel    chan int
	AllFiles                 []string
	nReduce                  int
	TaskNumber               int
	ReduceJobMap             map[int]int
	ReduceJobMapLock         sync.RWMutex
	ReduceJobSentTimeMap     map[int]time.Time
	ReduceJobSentTimeMapLock sync.RWMutex
	ReduceJobCompleteChannel chan int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetMapJob(args *MapJobRequest, reply *MapJobReply) error {
	m.MapJobSentTimeMapLock.Lock()
	defer m.MapJobSentTimeMapLock.Unlock()
	for _, value := range m.AllFiles {
		_, ok := m.MapJobSentTimeMap[value]
		if !ok {
			m.MapJobSentTimeMap[value] = time.Now().Add(time.Second * time.Duration(10))

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

	mapJobsStillWorking := m.checkMapJobsAllDone()
	if mapJobsStillWorking == nil {
		return errors.New("no more map jobs available")
	} else {
		return mapJobsStillWorking
	}
}

func (m *Master) ReportMapJobComplete(args *MapJobFinishRequest, reply *FinishRequestReply) error {
	taskNumber := args.TaskNumber
	m.TaskNumberToFileMapLock.RLock()
	defer m.TaskNumberToFileMapLock.RUnlock()
	fileName := m.TaskNumberToFileMap[taskNumber]
	m.MapStepMapLock.Lock()
	defer m.MapStepMapLock.Unlock()
	m.MapStepMap[fileName] = Proccessed
	return nil
}

func (m *Master) checkMapJobsAllDone() error {
	m.MapStepMapLock.RLock()
	defer m.MapStepMapLock.RUnlock()
	for _, status := range m.MapStepMap {
		if status != Proccessed {
			return errors.New("map jobs are still working. please try again")
		}
	}
	m.MapJobCompleteChannel <- 0
	return nil
}

func (m *Master) GetReduceJob(args *ReduceJobRequest, reply *ReduceJobReply) error {
	m.ReduceJobMapLock.Lock()
	defer m.ReduceJobMapLock.Unlock()
	for index, status := range m.ReduceJobMap {
		if status == NotSent {
			reply.TaskNumber = index
			m.ReduceJobMap[index] = Processing
			m.ReduceJobSentTimeMapLock.Lock()
			defer m.ReduceJobSentTimeMapLock.Unlock()
			m.ReduceJobSentTimeMap[index] = time.Now().Add(time.Second * time.Duration(10))
			return nil
		}
	}

	reduceJobsStillWorking := m.checkReduceJobsAllDone()
	if reduceJobsStillWorking {
		return errors.New("reduce jobs still working try again")
	} else {
		m.ReduceJobCompleteChannel <- 0
		return errors.New("no more reduce jobs available")
	}
}

func (m *Master) ReportReduceJobComplete(args *ReduceJobFinishRequest, reply *FinishRequestReply) error {
	taskNumber := args.TaskNumber
	m.ReduceJobMapLock.Lock()
	defer m.ReduceJobMapLock.Unlock()
	m.ReduceJobMap[taskNumber] = Proccessed
	return nil
}

func (m *Master) checkMapJobTimeout() {
	m.MapJobSentTimeMapLock.Lock()
	defer m.MapJobSentTimeMapLock.Unlock()
	for _, value := range m.AllFiles {
		expirationTime, ok := m.MapJobSentTimeMap[value]
		if ok && expirationTime.Before(time.Now()) {
			m.MapStepMapLock.RLock()
			defer m.MapStepMapLock.RUnlock()
			mapStepMapStatus, mapStepMapOk := m.MapStepMap[value]
			if !mapStepMapOk || mapStepMapStatus != Proccessed {
				delete(m.MapJobSentTimeMap, value)
			}
		}
	}
}

func (m *Master) checkMapJobTimeoutRepeatedly() {
	for {
		select {
		case <-m.MapJobCompleteChannel:
			return
		case <-time.After(time.Second):
			m.checkMapJobTimeout()
		}
	}
}

func (m *Master) checkReduceJobTimeout() {
	m.ReduceJobMapLock.Lock()
	defer m.ReduceJobMapLock.Unlock()
	for index, status := range m.ReduceJobMap {
		if status != Proccessed {
			m.ReduceJobSentTimeMapLock.RLock()
			defer m.ReduceJobSentTimeMapLock.RUnlock()
			expirationTime, ok := m.ReduceJobSentTimeMap[index]
			if ok && expirationTime.Before(time.Now()) {
				delete(m.ReduceJobMap, index)
			}
		}
	}
}

func (m *Master) checkReduceJobTimeoutRepeatedly() {
	for {
		select {
		case <-m.ReduceJobCompleteChannel:
			return
		case <-time.After(time.Second):
			m.checkReduceJobTimeout()
		}
	}
}

func (m *Master) checkReduceJobsAllDone() bool {
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
	done := m.checkReduceJobsAllDone()
	if !done {
		return false
	}

	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		matched, _ := regexp.MatchString(`mr-\d+-\d+`, f.Name())
		if matched {
			os.Remove(f.Name())
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
	m.MapJobSentTimeMap = make(map[string]time.Time)
	m.MapStepMap = make(map[string]int)
	m.TaskNumberToFileMap = make(map[int]string)
	m.ReduceJobMap = make(map[int]int)
	m.ReduceJobSentTimeMap = make(map[int]time.Time)
	for i := 0; i < nReduce; i++ {
		m.ReduceJobMap[i] = NotSent
	}
	m.nReduce = nReduce
	m.TaskNumberToFileMapLock = sync.RWMutex{}
	m.MapJobSentTimeMapLock = sync.RWMutex{}
	m.MapStepMapLock = sync.RWMutex{}
	m.ReduceJobMapLock = sync.RWMutex{}
	m.MapJobCompleteChannel = make(chan int)
	m.ReduceJobCompleteChannel = make(chan int)

	go m.checkMapJobTimeoutRepeatedly()
	go m.checkReduceJobTimeoutRepeatedly()
	if _, err := os.Stat("tmp/"); os.IsNotExist(err) {
		os.Mkdir("tmp/", os.ModeDir)
	}
	m.server()
	return &m
}
