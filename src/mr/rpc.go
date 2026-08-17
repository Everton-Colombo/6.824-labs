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

type GetTaskArgs struct{}

type TaskType int

// TaskType constants
const (
	MapTaskType TaskType = iota
	ReduceTaskType
	WaitTaskType
	ExitTaskType
)

type GetTaskReply struct {
	TaskType TaskType // 0: map, 1: reduce, 2: wait, 3: exit
	FileName string   // only for map tasks
	TaskNum  int
	NReduce  int
}

type TaskReportStatus int

const (
	Success TaskReportStatus = iota
	Failure
)

type ReportTaskArgs struct {
	TaskType TaskType // 0: map, 1: reduce
	TaskNum  int
	Status   TaskReportStatus // 0: success, 1: failure
}

type ReportTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
