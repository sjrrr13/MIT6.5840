package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
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
type TaskType bool
type AssignPhase bool

const (
	// Define task type
	Map    TaskType = false
	Reduce TaskType = true

	// Define AssignPhase
	MapPhase    AssignPhase = false
	ReducePhase AssignPhase = true

	TmpMapFilePath      = "/tmp/tmp_map_file/"
	TmpReduceFilePath   = "/tmp/tmp_res_file/"
	FinalMapFilePath    = "/tmp/final_map_file/"
	FinalReduceFilePath = "/tmp/final_res_file/"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Task struct {
	TaskID    int
	TaskType  TaskType // Map task or Reduce task
	Filename  string
	StartTime time.Time
	WorkerID  int
	NReduce   int
}

type TaskReq struct {
	WorkerID         int
	HaveFinishedTask bool
	FinishedTask     Task
	TemporaryFiles   []string
}

type TaskResp struct {
	WorkerID     int
	AllTasksDone bool
	HaveNewTask  bool
	NewTask      Task
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
