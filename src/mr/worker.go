package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
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

	var reply *MapJobReply
	var hasMapJob bool
	for {
		reply, hasMapJob = GetMapJob()
		if !hasMapJob {
			break
		}
		runMap(reply, mapf)
		ReportMapJobComplete(reply)
	}

	var reduceJobReply *ReduceJobReply
	var reduceJobSuccess bool
	var error error
	for {
		reduceJobReply, reduceJobSuccess, error = GetReduceJob()
		if !reduceJobSuccess {
			if strings.Contains(error.Error(), "map jobs") {
				log.Print("Trying to get reduce job again in 1 second as map jobs are not done yet")
				time.Sleep(time.Second)
				continue
			}
			break
		}
		runReduce(reduceJobReply, reducef)
		ReportReduceJobComplete(reduceJobReply)
	}

}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func runMap(reply *MapJobReply, mapf func(string, string) []KeyValue) {
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

func runReduce(reduceJobReply *ReduceJobReply, reducef func(string, []string) string) bool {
	taskNumber := reduceJobReply.TaskNumber
	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatal(err)
	}

	resultMap := make(map[string][]string)

	for _, f := range files {
		if strings.HasSuffix(f.Name(), "-"+strconv.Itoa(taskNumber)) {
			filename := f.Name()
			file, err := os.Open(f.Name())
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				_, exists := resultMap[kv.Key]
				if exists {
					resultMap[kv.Key] = append(resultMap[kv.Key], kv.Value)
				} else {
					resultMap[kv.Key] = []string{kv.Value}
				}
			}
		}
	}
	keys := make([]string, 0, len(resultMap))
	for key := range resultMap {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	oname := "mr-out-" + strconv.Itoa(taskNumber)
	ofile, _ := os.Create(oname)

	for _, key := range keys {
		output := reducef(key, resultMap[key])
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	return true
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func GetMapJob() (*MapJobReply, bool) {
	args := MapJobRequest{}

	reply := &MapJobReply{}
	success, _ := call("Master.GetMapJob", &args, &reply)
	if success {
		fmt.Println("Response to get map job was ", reply)
		return reply, true
	}
	return nil, false
}

func ReportMapJobComplete(jobReply *MapJobReply) (*FinishRequestReply, bool) {
	args := MapJobFinishRequest{}
	args.TaskNumber = jobReply.TaskNumber

	reply := &FinishRequestReply{}
	success, _ := call("Master.ReportMapJobComplete", &args, &reply)
	if success {
		return reply, true
	}
	return nil, false
}

func GetReduceJob() (*ReduceJobReply, bool, error) {
	args := ReduceJobRequest{}

	reply := &ReduceJobReply{}
	success, error := call("Master.GetReduceJob", &args, &reply)
	if success {
		fmt.Println("Got reduce job ", reply)
		return reply, true, error
	}
	return nil, false, error

}

func ReportReduceJobComplete(jobReply *ReduceJobReply) (*FinishRequestReply, bool) {
	args := ReduceJobFinishRequest{}
	args.TaskNumber = jobReply.TaskNumber

	reply := &FinishRequestReply{}
	success, _ := call("Master.ReportReduceJobComplete", &args, &reply)
	if success {
		return reply, true
	}
	return nil, false
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) (bool, error) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true, err
	}

	fmt.Println(err)
	return false, err
}
