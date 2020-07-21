package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// RequestTaskArgs request a new task for a worked from master
type RequestTaskArgs struct {
}

// RequestTaskReply is a reply from master containing id of a task to be executed, if it's a map(or reduce) and a key(filename) of input
type RequestTaskReply struct {
	Id      int
	IsMap   bool
	Key     string
	Nreduce int
}

// TaskFinishedArgs is a notification sent from worker to a master in order to update it's status
type TaskFinishedArgs struct {
	Id    int
	Keys  []int
	IsMap bool
}

// TaskFinishedReply is a reply to a notification
type TaskFinishedReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
