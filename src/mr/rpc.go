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
	X int
}

type TaskReply struct {
	Category    string // Mapper/Reducer/NoWork
	WorkerIndex int    // index of current worker
	FileName    string // which file should calculate, only for Mapper to use
	ReduceNum   int    // How many reducer in total, only for Mapper to use
}

type WorkerDoneArgs struct {
	WorkerIndex int
}

type WorkerDoneReply struct {
	Ok bool
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
