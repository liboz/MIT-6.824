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
	MapJobMap                map[string]int
	MapJobMapLock            sync.RWMutex
	MapJobSentTimeMap        map[string]time.Time
	MapJobSentTimeMapLock    sync.RWMutex
	MapJobCompleteChannel    chan int
	MapJobFinished           bool
	MapJobFinishedLock       sync.RWMutex
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
			m.MapJobSentTimeMap[value] = nowPlus10Seconds()

			m.MapJobMapLock.Lock()
			defer m.MapJobMapLock.Unlock()
			m.MapJobMap[value] = Processing

			reply.FileName = value
			reply.NReduce = m.nReduce
			reply.TaskNumber = m.TaskNumber
			m.TaskNumber += 1

			return nil
		}
	}

	m.MapJobFinishedLock.RLock()
	defer m.MapJobFinishedLock.RUnlock()
	var mapJobsStillWorking error
	if !m.MapJobFinished {
		mapJobsStillWorking = m.checkMapJobsAllDone()
	}
	if mapJobsStillWorking == nil {
		return errors.New("no more map jobs available")
	} else {
		return mapJobsStillWorking
	}
}

func (m *Master) ReportMapJobComplete(args *MapJobFinishRequest, reply *MapJobFinishReply) error {
	fileName := args.FileName
	m.MapJobMapLock.Lock()
	defer m.MapJobMapLock.Unlock()
	m.MapJobMap[fileName] = Proccessed
	return nil
}

func (m *Master) checkMapJobsAllDone() error {
	m.MapJobMapLock.Lock()
	defer m.MapJobMapLock.Unlock()
	for _, status := range m.MapJobMap {
		if status != Proccessed {
			return errors.New("map jobs are still working. please try again")
		}
	}
	m.MapJobCompleteChannel <- 0
	return nil
}

func nowPlus10Seconds() time.Time {
	return time.Now().Add(time.Second * time.Duration(10))
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
			m.ReduceJobSentTimeMap[index] = nowPlus10Seconds()
			return nil
		}
	}

	allDone := m.checkReduceJobsAllDone(false)
	if allDone {
		m.ReduceJobCompleteChannel <- 0
		return errors.New("no more reduce jobs available")
	} else {
		return errors.New("reduce jobs still working try again")
	}
}

func (m *Master) ReportReduceJobComplete(args *ReduceJobFinishRequest, reply *MapJobFinishReply) error {
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
			m.MapJobMapLock.RLock()
			defer m.MapJobMapLock.RUnlock()
			MapJobMapStatus, MapJobMapOk := m.MapJobMap[value]
			if !MapJobMapOk || MapJobMapStatus != Proccessed {
				delete(m.MapJobSentTimeMap, value)
			}
		}
	}
}

func (m *Master) checkMapJobTimeoutRepeatedly() {
	for {
		select {
		case <-m.MapJobCompleteChannel:
			m.MapJobFinishedLock.Lock()
			m.MapJobFinished = true
			defer m.MapJobFinishedLock.Unlock()
			return
		case <-time.After(time.Second):
			m.checkMapJobTimeout()
		}
	}
}

func (m *Master) checkReduceJobTimeout() {
	m.ReduceJobMapLock.Lock()
	defer m.ReduceJobMapLock.Unlock()
	m.ReduceJobSentTimeMapLock.Lock()
	defer m.ReduceJobSentTimeMapLock.Unlock()
	for index, status := range m.ReduceJobMap {
		if status != Proccessed {
			expirationTime, ok := m.ReduceJobSentTimeMap[index]
			if ok && expirationTime.Before(time.Now()) {
				m.ReduceJobMap[index] = NotSent
				delete(m.ReduceJobSentTimeMap, index)
			}
		}
	}
}

func (m *Master) checkReduceJobTimeoutRepeatedly() {
	doTimeoutCheck := true
	for {
		select {
		case <-m.ReduceJobCompleteChannel:
			doTimeoutCheck = false
		case <-time.After(time.Second):
			if doTimeoutCheck {
				m.checkReduceJobTimeout()
			}
		}
	}
}

func (m *Master) checkReduceJobsAllDone(lock bool) bool {
	if lock {
		m.ReduceJobMapLock.RLock()
		defer m.ReduceJobMapLock.RUnlock()
	}
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
	done := m.checkReduceJobsAllDone(true)
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
	m.MapJobMap = make(map[string]int)
	m.ReduceJobMap = make(map[int]int)
	m.ReduceJobSentTimeMap = make(map[int]time.Time)
	for i := 0; i < nReduce; i++ {
		m.ReduceJobMap[i] = NotSent
	}
	m.nReduce = nReduce
	m.MapJobSentTimeMapLock = sync.RWMutex{}
	m.MapJobMapLock = sync.RWMutex{}
	m.ReduceJobMapLock = sync.RWMutex{}
	m.MapJobCompleteChannel = make(chan int)
	m.ReduceJobCompleteChannel = make(chan int)
	m.MapJobFinished = false
	m.MapJobFinishedLock = sync.RWMutex{}

	go m.checkMapJobTimeoutRepeatedly()
	go m.checkReduceJobTimeoutRepeatedly()
	if _, err := os.Stat("tmp/"); os.IsNotExist(err) {
		os.Mkdir("tmp/", os.ModeDir)
	}
	m.server()
	return &m
}
