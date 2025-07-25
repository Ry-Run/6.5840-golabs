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
type CommitTaskArgs struct {
	WorkerId string
	TaskId   string
}

type CommitTaskReply struct {
	Accept bool
}

type GetTaskArgs struct {
	WorkerId string
}

type GetTaskReply struct {
	TaskId     string
	NReduce    int
	Quit       bool
	MapTaskIds []string
}

type RegisterArgs struct {
}

type RegisterReply struct {
	Id string
}

type KeepAliveArgs struct {
	Id string
}

type KeepAliveReply struct {
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
