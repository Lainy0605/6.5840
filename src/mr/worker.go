package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	live := true
	for live {
		reply := CallGetTask()
		if reply == nil {
			continue
		} else if reply.Category == "Mapper" {
			mapperWork(mapf, reply)
		} else if reply.Category == "Reducer" {
			reducerWork(reducef, reply)
		} else {
			time.Sleep(time.Second * 3)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func mapperWork(mapf func(string, string) []KeyValue, reply *TaskReply) {
	// read file
	content, err := os.ReadFile(reply.FileName)
	if err != nil {
		log.Fatalf("Error-01: read file %v: %v\n", reply.FileName, err)
	}

	// do map
	intermediate := mapf(reply.FileName, string(content))

	var intermediateFileNames []string
	kvByReduce := map[int][]KeyValue{}
	for _, kv := range intermediate {
		key := kv.Key
		reduceIndex := ihash(key) % reply.ReduceNum
		kvByReduce[reduceIndex] = append(kvByReduce[reduceIndex], kv)
	}

	// write intermediate file
	for k, kvs := range kvByReduce {
		intermediateFileName := "mr-" + strconv.Itoa(reply.WorkerIndex) + "-" + strconv.Itoa(k)
		tempFile, err := os.CreateTemp(".", intermediateFileName+"Temp")
		if err != nil {
			log.Fatalf("Error-02: fail to create temp file %v: %v\n", intermediateFileName, err)
		}
		data, err := json.Marshal(kvs)
		if err != nil {
			log.Fatalf("Error-03: fail to encode %v: %v\n", kvs, err)
		}
		_, err = tempFile.Write(data)
		if err != nil {
			log.Fatalf("Error-04: fail to write to temp file %v: %v\n", tempFile.Name(), err)
		}
		tempFile.Close()
		os.Rename(tempFile.Name(), "/Users/effy/Documents/GradeFour/6.5840/src/main/intermediateFiles/"+intermediateFileName)
		intermediateFileNames = append(intermediateFileNames, intermediateFileName)
	}

	ok := CallWorkerDone(reply.WorkerIndex).Ok
	if !ok {
		for _, interFileName := range intermediateFileNames {
			err := os.Remove(interFileName)
			if err != nil {
				log.Fatalf("Error-05: cannot delete file %v: %v\n", interFileName, err)
			}
		}
	}
}

func reducerWork(reducef func(string, []string) string, reply *TaskReply) {
	outFileName := "mr-out-" + strconv.Itoa(reply.WorkerIndex)

	tempFile, err := os.CreateTemp(".", outFileName+"Temp")
	if err != nil {
		log.Fatalf("Error-06: fail to create temp file %v: %v\n", outFileName, err)
	}
	files, err := os.ReadDir("/Users/effy/Documents/GradeFour/6.5840/src/main/intermediateFiles")
	if err != nil {
		log.Fatalf("Error-07: cannot read dir %v\n", err)
	}

	var kva []KeyValue
	for _, file := range files {
		if strings.HasSuffix(file.Name(), strconv.Itoa(reply.WorkerIndex)) { //TODO: should not include mr-out-*
			content, err := os.ReadFile("/Users/effy/Documents/GradeFour/6.5840/src/main/intermediateFiles/" + file.Name())
			if err != nil {
				log.Fatalf("Error-08: cannot read file %v: %v\n", file.Name(), err)
			}

			temp := []KeyValue{}
			err = json.Unmarshal(content, &temp)
			if err != nil {
				log.Fatalf("Error-09: fail to decode json %v : %v\n", content, err)
			}
			kva = append(kva, temp...)
		}
	}

	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	tempFile.Close()
	os.Rename(tempFile.Name(), outFileName)

	ok := CallWorkerDone(reply.WorkerIndex).Ok
	if !ok {
		err := os.Remove(outFileName)
		if err != nil {
			log.Fatalf("Error-05: cannot delete file %v: %v\n", outFileName, err)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallGetTask() *TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		log.Printf("Task type is %v\n", reply.Category)
		return &reply
	} else {
		log.Fatalf("call GetTask failed\n")
		return nil
	}
}

func CallWorkerDone(workerIndex int) *WorkerDoneReply {
	args := WorkerDoneArgs{
		WorkerIndex: workerIndex,
	}
	reply := WorkerDoneReply{
		Ok: false,
	}
	ok := call("Coordinator.WorkerDone", &args, &reply)
	if ok {
		log.Printf("worker %v call WorkerDone success!\n", workerIndex)
		return &reply
	} else {
		log.Fatalf("call WorkerDone failed!\n")
		return nil
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
