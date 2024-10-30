package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"

type Coordinator struct {
	// Your definitions here.
	Lock        sync.Mutex
	TaskType    string
	MapIndex    int
	ReduceIndex int
	Filename    []string
	ReduceNum   int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1

	return nil
}

func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	// 判断任务类型
	if c.MapIndex < len(c.Filename) {
		*reply = ApplyForTaskReply{
			TaskType:  "map",
			TaskIndex: c.MapIndex,
			Filename:  c.Filename[c.MapIndex],
			ReduceNum: c.ReduceNum,
		}
		c.MapIndex++
	} else {
		// 如果超出范围，则开始 "reduce" 任务并重置索引
		*reply = ApplyForTaskReply{
			TaskType:  "reduce",
			TaskIndex: c.ReduceIndex,
			Filename:  fmt.Sprintf("map-%d", c.ReduceIndex),
			ReduceNum: c.ReduceNum,
		}
		c.ReduceIndex++
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)  // 使用 rpc 注册 Coordinator 类型，使该类型的方法可以通过 rpc 被远程调用
	rpc.HandleHTTP() // 使用默认的HTTP处理器处理RPC消息，使程序可以处理通过HTTP发送的RPC请求。
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()        // 调用自定义函数coordinatorSock()，它应该返回一个UNIX套接字名称
	os.Remove(sockname)                  // 删除任何已存在的同名UNIX套接字文件，确保新的套接字能够创建成功。
	l, e := net.Listen("unix", sockname) // 创建一个UNIX域的监听器，监听给定的套接字名称sockname。
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) // 启动一个新的goroutine，使用给定的监听器l和默认的HTTP处理器来服务接收到的HTTP请求。
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool { // 判断工作是否完成
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.MapIndex = 0
	c.ReduceIndex = 1
	c.Filename = files
	c.ReduceNum = nReduce

	c.server()
	return &c
}
