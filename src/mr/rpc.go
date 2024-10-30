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

type Task struct {
	TaskType  int
	Filename  string
	TaskId    int
	ReduceNum int
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type ApplyForTaskArgs struct {
	WorkerId int
}

type ApplyForTaskReply struct {
	TaskType  string
	TaskIndex int
	Filename  string
	ReduceNum int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"       // 定义一个字符串变量 s，并初始化为 "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid()) // 获取当前进程的用户ID，并将其转换为字符串，追加到 s 后面
	return s
}
