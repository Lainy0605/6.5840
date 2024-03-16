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
	files         []string // the name of files to be mapped
	reduceNum     []int    // the taskId of reducer remaining to be done
	TaskIdCounter int      // if AppPhase turns to Reducing, TaskIdCounter should be reassigned to 0
	AppPhase      AppPhase // the Phase of application, Mapping/Reducing
	TasksStatus   []TaskStatus

	coordinatorGuard sync.Mutex
}

type TaskStatus struct {
	taskId    int
	timeStamp int64
	fileName  string
}

type AppPhase int

const (
	MapPhase AppPhase = iota
	ReducePhase
	End
)

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *TaskArgs, task *Task) error {
	c.coordinatorGuard.Lock()
	defer c.coordinatorGuard.Unlock()

	switch c.AppPhase {
	case MapPhase:
		if len(c.files) > 0 { // still have file
			task.TaskType = MapTask
			task.TaskId = c.TaskIdCounter
			task.FileName = c.files[0] // assign the first file to this worker
			task.ReduceNum = len(c.reduceNum)
			c.files = c.files[1:] // delete the first file
			c.TaskIdCounter++
			c.TasksStatus = append(c.TasksStatus, TaskStatus{
				taskId:    task.TaskId,
				timeStamp: time.Now().Unix(),
				fileName:  task.FileName,
			})
		} else { // no file need to be calculated
			task.TaskType = WaittingTask
		}
	case ReducePhase:
		if len(c.reduceNum) > 0 { // the number of workers doesn't exceed
			task.TaskType = ReduceTask
			task.TaskId = c.reduceNum[0]
			c.reduceNum = c.reduceNum[1:]
			c.TasksStatus = append(c.TasksStatus, TaskStatus{
				taskId:    task.TaskId,
				timeStamp: time.Now().Unix(),
			})
		} else { // the number of workers exceeds
			task.TaskType = WaittingTask
		}
	case End:
		task.TaskType = ExitTask
	}

	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.coordinatorGuard.Lock()
	defer c.coordinatorGuard.Unlock()
	// TODO: Coordinator should delete intermediate files produced by Mapper if Mapper finishes too late( 10 seconds)
	reply.Ok = false
	for i, taskStatus := range c.TasksStatus {
		if taskStatus.taskId == args.TaskId {
			c.TasksStatus = append(c.TasksStatus[:i], c.TasksStatus[i+1:]...)
			reply.Ok = true
			break // Expect that only one same taskId
		}
	}

	switch c.AppPhase {
	case MapPhase:
		if len(c.files)+len(c.TasksStatus) == 0 {
			c.AppPhase = ReducePhase
			c.TaskIdCounter = 0
		}
	case ReducePhase:
		if len(c.TasksStatus)+len(c.reduceNum) == 0 {
			c.AppPhase = End
		}
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
	c.coordinatorGuard.Lock()
	defer c.coordinatorGuard.Unlock()
	for i, wokerStatus := range c.TasksStatus {
		timeNow := time.Now().Unix()
		if timeNow-wokerStatus.timeStamp > 10 {
			c.TasksStatus = append(c.TasksStatus[:i], c.TasksStatus[i+1:]...)
			if c.AppPhase == MapPhase {
				c.files = append(c.files, wokerStatus.fileName)
			} else if c.AppPhase == ReducePhase {
				c.reduceNum = append(c.reduceNum, wokerStatus.taskId)
			}
			break
		}
	}

	if c.AppPhase == End {
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
	c.files = files[:]
	c.TaskIdCounter = 0
	c.AppPhase = MapPhase

	for i := 0; i < nReduce; i++ {
		c.reduceNum = append(c.reduceNum, i)
	}

	c.server()
	return &c
}
