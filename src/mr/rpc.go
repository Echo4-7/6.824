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

const (
	MAP    = "Map"    // Map任务
	REDUCE = "Reduce" // Reduce任务
	DONE   = "Done"   // 无任务
)

const (
	Idle       = "idle"        // 未分配
	InProgress = "in_progress" // 正在进行
	Completed  = "completed"   // 已完成
)

// Add your RPC definitions here.

type Args struct {
	WorkId         int    // 每个Worker的Id
	FinishTaskType string // 完成任务的类型（Map or Reduce）
	FinishTaskId   int    // 已完成任务的Id
}

type Reply struct {
	TaskId      int      // 任务Id
	TaskType    string   // 任务类型（map or reduce）
	FileName    string   // 文件名
	ReduceFiles []string // 等待reduce的文件名
	ReduceId    int      // 当前是第几个Reduce任务
	NReduce     int      // reduce任务的个数
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
