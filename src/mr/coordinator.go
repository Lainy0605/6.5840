package mr

import "log"
import "net"
import "net/rpc"
import "net/http"
import "os"
import "sync"
import "time"

type Coordinator struct {
	// Your definitions here.
	fileNames     []string // the name of files to be mapped
	reduceNum     []int    // the index of reducer remaining to be done
	workerCounter int      // if appStatus turns to Reducing, workerCounter should be reassigned to 0
	workersStatus []WorkerStatus
	appStatus     string // the status of application, Mapping/Reducing

	coordinatorGuard sync.Mutex
}

type WorkerStatus struct {
	index     int
	timeStamp int64
	fileName  string
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	c.coordinatorGuard.Lock()

	if c.appStatus == "Mapping" {
		if len(c.fileNames) > 0 { // still have file
			reply.Category = "Mapper"
			reply.WorkerIndex = c.workerCounter
			reply.FileName = c.fileNames[0] // assign the first file to this worker
			reply.ReduceNum = len(c.reduceNum)
			c.fileNames = c.fileNames[1:] // delete the first file
			c.workerCounter++
			c.workersStatus = append(c.workersStatus, WorkerStatus{
				index:     reply.WorkerIndex,
				timeStamp: time.Now().Unix(),
				fileName:  reply.FileName,
			})
		} else { // no file need to be calculated
			reply.Category = "NoWork"
		}
	} else if c.appStatus == "Reducing" {
		if len(c.reduceNum) > 0 { // the number of workers doesn't exceed
			reply.Category = "Reducer"
			reply.WorkerIndex = c.reduceNum[0]
			c.reduceNum = c.reduceNum[1:]
			c.workersStatus = append(c.workersStatus, WorkerStatus{
				index:     reply.WorkerIndex,
				timeStamp: time.Now().Unix(),
			})
		} else { // the number of workers exceeds
			reply.Category = "NoWork"
		}
	} else {
		reply.Category = "NoWork"
	}

	c.coordinatorGuard.Unlock()
	return nil
}

func (c *Coordinator) WorkerDone(args *WorkerDoneArgs, reply *WorkerDoneReply) error {
	c.coordinatorGuard.Lock()

	// TODO: Coordinator should delete intermediate files produced by Mapper if Mapper finishes too late( 10 seconds)
	reply.Ok = false
	for i, workerStatus := range c.workersStatus {
		if workerStatus.index == args.WorkerIndex {
			c.workersStatus = append(c.workersStatus[:i], c.workersStatus[i+1:]...)
			reply.Ok = true
			break // Expect that only one same index
		}
	}
	if len(c.fileNames)+len(c.workersStatus) == 0 {
		switch c.appStatus {
		case "Mapping":
			c.appStatus = "Reducing"
			c.workerCounter = 0
		case "Reducing":
			if len(c.reduceNum) == 0 {
				c.appStatus = "End"
			}
		}
	}

	c.coordinatorGuard.Unlock()
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
	c.coordinatorGuard.Lock()
	for i, wokerStatus := range c.workersStatus {
		timeNow := time.Now().Unix()
		if timeNow-wokerStatus.timeStamp > 10 {
			c.workersStatus = append(c.workersStatus[:i], c.workersStatus[i+1:]...)
			if c.appStatus == "Mapping" {
				c.fileNames = append(c.fileNames, wokerStatus.fileName)
			} else if c.appStatus == "Reducing" {
				c.reduceNum = append(c.reduceNum, wokerStatus.index)
			}
			break
		}
	}

	if c.appStatus == "End" {
		ret = true
	}
	c.coordinatorGuard.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.fileNames = files[:]
	c.workerCounter = 0
	c.appStatus = "Mapping"

	for i := 0; i < nReduce; i++ {
		c.reduceNum = append(c.reduceNum, i)
	}

	c.server()
	return &c
}
