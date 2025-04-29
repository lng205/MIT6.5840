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

type RegisterReply struct {
	WorkerID int
	NMap     int
	NReduce  int
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	NoTask
	End
)

type TaskArgs struct {
	WorkerID int
}

type TaskReply struct {
	TaskType TaskType
	TaskID   int

	FileName string
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
