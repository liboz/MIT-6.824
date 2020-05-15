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
		reduceJobReply, reduceJobSuccess := GetReduceJob()
		if reduceJobSuccess {
			runReduce(reduceJobReply, reducef)
		}
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

func runReduce(reduceJobReply *ReduceJobReply, reducef func(string, []string) string) bool {
	taskNumber := reduceJobReply.TaskNumber
	files, err := ioutil.ReadDir("./")
	if err != nil {
		log.Fatal(err)
	}

	resultMap := make(map[string][]string)

	for _, f := range files {
		if strings.HasSuffix(f.Name(), strconv.Itoa(taskNumber)) {
			log.Print(f.Name())
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

func GetReduceJob() (*ReduceJobReply, bool) {
	args := ReduceJobRequest{}

	reply := &ReduceJobReply{}
	success := call("Master.GetReduceJob", &args, &reply)
	if success {
		fmt.Println("Got reduce job ", reply)
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
