package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	fileNames     []string // the name of files to be calculated
	reduceNum     int      // the number of reduce tasks to use
	workerCounter int      // TODO: if appStatus turns to Reducing, workerCounter should be reassigned to 0
	workersStatus []WorkerStatus
	appStatus     string // the status of application, Mapping/Reducing

}

type WorkerStatus struct {
	index     int
	timeStamp int64
	fileName  string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	if c.appStatus == "Mapping" {
		if len(c.fileNames) > 0 { // still have file
			reply.category = "Mapper"
			reply.workerIndex = c.workerCounter
			reply.fileName = c.fileNames[0] // assign the first file to this worker
			c.fileNames = c.fileNames[1:]   // delete the first file
			c.workerCounter++
			c.workersStatus = append(c.workersStatus, WorkerStatus{
				index:     reply.workerIndex,
				timeStamp: time.Now().Unix(),
				fileName:  reply.fileName,
			})
		} else { // no file need to be calculated
			reply.category = "NoWork"
		}
	} else if c.appStatus == "Reducing" {
		if c.workerCounter < c.reduceNum { // the number of workers doesn't exceed
			reply.category = "Reducer"
			reply.workerIndex = c.workerCounter
			c.workerCounter++
			c.workersStatus = append(c.workersStatus, WorkerStatus{
				index:     reply.workerIndex,
				timeStamp: time.Now().Unix(),
			})
		} else { // the number of workers exceeds
			reply.category = "NoWork"
		}
	} else {
		reply.category = "NoWork"
	}

	return nil
}

func (c *Coordinator) WorkerDone(args *WorkerDoneArgs, reply *WorkerDoneReply) error {
	for i, workerStatus := range c.workersStatus {
		if workerStatus.index == args.workerIndex {
			c.workersStatus = append(c.workersStatus[:i], c.workersStatus[i+1:]...)
			reply.ok = true
			break // Expect that only one same index
		}
	}
	if len(c.fileNames)+len(c.workersStatus) == 0 {
		c.appStatus = "Reducing"
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	for i, wokerStatus := range c.workersStatus {
		timeNow := time.Now().Unix()
		if timeNow-wokerStatus.timeStamp > 10 {
			c.workersStatus = append(c.workersStatus[:i], c.workersStatus[i+1:]...)
			c.fileNames = append(c.fileNames, wokerStatus.fileName)
			break
		}
	}

	if c.appStatus == "End" {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.fileNames = files[:]
	c.reduceNum = nReduce
	c.appStatus = "Mapping"

	c.server()
	return &c
}
