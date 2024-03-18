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
	files             []string // the name of files to be mapped
	reduceNum         int
	taskIdCounter     int      // if appPhase turns to Reducing, taskIdCounter should be reassigned to 0
	appPhase          AppPhase // the Phase of application, Mapping/Reducing
	tasksStatus       map[int]TaskStatus
	mapTaskChannel    chan *Task
	reduceTaskChannel chan *Task

	coordinatorGuard sync.Mutex
}

type TaskStatus struct {
	timeStamp int64
	task      *Task
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

	switch c.appPhase {
	case MapPhase:
		if len(c.mapTaskChannel) > 0 {
			*task = *<-c.mapTaskChannel
			c.tasksStatus[task.TaskId] = TaskStatus{
				timeStamp: time.Now().Unix(),
				task:      task,
			}
		} else {
			task.TaskType = WaitingTask
		}
	case ReducePhase:
		if len(c.reduceTaskChannel) > 0 {
			*task = *<-c.reduceTaskChannel
			c.tasksStatus[task.TaskId] = TaskStatus{
				timeStamp: time.Now().Unix(),
				task:      task,
			}
		} else {
			task.TaskType = WaitingTask
		}
	case End:
		task.TaskType = ExitTask
	}

	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.coordinatorGuard.Lock()
	defer c.coordinatorGuard.Unlock()

	for taskId, _ := range c.tasksStatus {
		if taskId == args.TaskId {
			delete(c.tasksStatus, taskId)
			break
		}
	}

	switch c.appPhase {
	case MapPhase:
		if len(c.mapTaskChannel)+len(c.tasksStatus) == 0 {
			c.appPhase = ReducePhase
		}
	case ReducePhase:
		if len(c.reduceTaskChannel)+len(c.tasksStatus) == 0 {
			c.appPhase = End
		}
	}

	return nil
}

func (c *Coordinator) makeMapTasks() {
	for _, file := range c.files {
		task := Task{
			TaskType:  MapTask,
			TaskId:    c.generateId(),
			FileName:  file,
			ReduceNum: c.reduceNum,
		}

		c.mapTaskChannel <- &task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.reduceNum; i++ {
		task := Task{
			TaskType: ReduceTask,
			TaskId:   c.generateId(),
			ReduceId: i,
		}

		c.reduceTaskChannel <- &task
	}
}

func (c *Coordinator) generateId() int {
	res := c.taskIdCounter
	c.taskIdCounter++
	return res
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
	for taskId, taskStatus := range c.tasksStatus {
		timeNow := time.Now().Unix()
		if timeNow-taskStatus.timeStamp > 10 {
			delete(c.tasksStatus, taskId)
			if c.appPhase == MapPhase {
				c.mapTaskChannel <- taskStatus.task
			} else if c.appPhase == ReducePhase {
				c.reduceTaskChannel <- taskStatus.task
			}
			break
		}
	}

	if c.appPhase == End {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		reduceNum:         nReduce,
		taskIdCounter:     0,
		appPhase:          MapPhase,
		tasksStatus:       make(map[int]TaskStatus),
		mapTaskChannel:    make(chan *Task, len(files)),
		reduceTaskChannel: make(chan *Task, nReduce),
	}

	c.makeMapTasks()
	c.makeReduceTasks()

	c.server()
	return &c
}
