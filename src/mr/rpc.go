package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskArgs struct {
	X int // no use
}

type Task struct {
	TaskType  TaskType // Mapper/Reducer/NoWork
	TaskId    int      // task id
	FileName  string   // which file should calculate, only for Mapper to use
	ReduceNum int      // How many reducer in total, only for Mapper to use
	ReduceId  int      // which reduce work to do, only for Reducer to use
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	ExitTask
)

type TaskDoneArgs struct {
	TaskId int
}

type TaskDoneReply struct {
	X int //no use
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
