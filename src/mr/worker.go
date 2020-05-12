package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	reply, success := GetJob()
	if success {
		runMap(reply, mapf)
		ReportJobComplete(reply)
	}

}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func runMap(reply *JobReply, mapf func(string, string) []KeyValue) {
	filename := reply.FileName
	nReduce := reply.NReduce
	taskNumber := reply.TaskNumber
	intermediate := make(map[int][]KeyValue)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		hash := ihash(kv.Key) % nReduce
		_, exists := intermediate[hash]
		if exists {
			intermediate[hash] = append(intermediate[hash], kv)
		} else {
			intermediate[hash] = []KeyValue{kv}
		}
	}

	for key, elements := range intermediate {
		file, err := os.Create(fmt.Sprintf("mr-%d-%d", taskNumber, key))
		check(err)
		enc := json.NewEncoder(file)
		for _, element := range elements {
			err := enc.Encode(&element)
			check(err)
		}
		file.Close()
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func GetJob() (*JobReply, bool) {
	args := JobRequest{}

	reply := &JobReply{}
	success := call("Master.GetJob", &args, &reply)
	if success {
		fmt.Println("Response was ", reply)
		return reply, true
	}
	return nil, false
}

func ReportJobComplete(jobReply *JobReply) (*FinishRequestReply, bool) {
	args := MapJobFinishRequest{}
	args.TaskNumber = jobReply.TaskNumber

	reply := &FinishRequestReply{}
	success := call("Master.ReportMapJobComplete", &args, &reply)
	if success {
		fmt.Println("Job Complete response was ", reply)
		return reply, true
	}
	return nil, false
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
