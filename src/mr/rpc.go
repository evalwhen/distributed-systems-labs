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

const (
	TaskTypeMap TaskType = iota + 1
	TaskTypeReduce
)

type TaskType int

func (t TaskType) String() string {
	switch t {
	case TaskTypeMap:
		return "map"
	case TaskTypeReduce:
		return "reduce"
	default:
		return "unkown task type"
	}
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskArgs struct {
}

type TaskReply struct {
	Task *Task
}

type NotifyArgs struct {
	Task *Task
}

type NotifyReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
