package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Mu             sync.Mutex
	TaskType       string
	MapIndex       int
	ReduceIndex    int
	Filename       []string
	MapNum         int
	ReduceNum      int
	AvailableTasks []Task
	RunningTasks   map[int]Task
}

func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	// 分配任务
	if len(c.AvailableTasks) != 0 {
		if c.AvailableTasks[0].TaskType == "map" {
			*reply = ApplyForTaskReply{
				TaskType:  c.AvailableTasks[0].TaskType,
				TaskId:    c.AvailableTasks[0].TaskId,
				Filename:  c.AvailableTasks[0].Filename,
				ReduceNum: c.ReduceNum,
			}
			c.AvailableTasks[0].WorkerId = args.WorkerId
			c.AvailableTasks[0].DeadLine = time.Now().Add(10 * time.Second)
			c.RunningTasks[c.AvailableTasks[0].TaskId] = c.AvailableTasks[0]
			if len(c.AvailableTasks) > 1 {
				c.AvailableTasks = append(c.AvailableTasks[:0], c.AvailableTasks[1:]...)
			} else {
				c.AvailableTasks = c.AvailableTasks[:0]
			}

		} else if c.AvailableTasks[0].TaskType == "reduce" && c.MapIndex == c.MapNum {
			*reply = ApplyForTaskReply{
				TaskType: c.AvailableTasks[0].TaskType,
				TaskId:   c.AvailableTasks[0].TaskId,
				MapNum:   c.MapNum,
			}
			c.AvailableTasks[0].WorkerId = args.WorkerId
			c.AvailableTasks[0].DeadLine = time.Now().Add(10 * time.Second)
			c.RunningTasks[c.AvailableTasks[0].TaskId] = c.AvailableTasks[0]
			if len(c.AvailableTasks) > 1 {
				c.AvailableTasks = append(c.AvailableTasks[:0], c.AvailableTasks[1:]...)
			} else {
				c.AvailableTasks = c.AvailableTasks[:0]
			}

			// log.Printf("reply = %+v", *reply)
		} else {
			*reply = ApplyForTaskReply{
				TaskType: "wait",
			}
		}

	} else if len(c.AvailableTasks) == 0 && len(c.RunningTasks) == 0 {
		*reply = ApplyForTaskReply{
			TaskType: "end",
		}
		c.TaskType = "end"
	}

	// if c.MapIndex < len(c.Filename) {
	// 	*reply = ApplyForTaskReply{
	// 		TaskType:  "map",
	// 		TaskIndex: c.MapIndex,
	// 		Filename:  c.Filename[c.MapIndex],
	// 		ReduceNum: c.ReduceNum,
	// 	}
	// 	c.MapIndex++
	// } else if c.ReduceIndex < 10 {
	// 	// 如果超出范围，则开始 "reduce" 任务
	// 	*reply = ApplyForTaskReply{
	// 		TaskType:  "reduce",
	// 		TaskIndex: c.ReduceIndex,
	// 		MapNum:    c.MapIndex,
	// 	}
	// 	c.ReduceIndex++
	// } else {
	// 	*reply = ApplyForTaskReply{
	// 		TaskType: "end",
	// 	}
	// 	c.TaskType = "end"
	// }

	// 输出调试
	// fmt.Printf("可用任务队列：\n")
	// for index, task := range c.AvailableTasks {
	// 	fmt.Printf("Index: %d, TaskType: %s, Filename: %s, TaskId: %d, WorkerId: %d\n",
	// 		index, task.TaskType, task.Filename, task.TaskId, task.WorkerId)
	// }

	// fmt.Printf("正在运行的任务队列：\n")
	// for taskID, task := range c.RunningTasks {
	// 	fmt.Printf("TaskID (key): %d, TaskType: %s, Filename: %s, TaskId: %d, WorkerId: %d\n",
	// 		taskID, task.TaskType, task.Filename, task.TaskId, task.WorkerId)
	// }

	// log.Printf("reply: %+v", reply)

	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	delete(c.RunningTasks, args.TaskId)

	// if c.MapIndex == c.MapNum-1 {
	// 	for i := 0; i < c.ReduceNum; i++ {
	// 		task := Task{
	// 			TaskType: "reduce",
	// 			TaskId:   c.ReduceIndex,
	// 		}
	// 		c.ReduceIndex++
	// 		c.AvailableTasks = append(c.AvailableTasks, task)
	// 	}

	// 	log.Printf("reduce Tasks add availableTasks finish\n")
	// 	log.Printf("c.AvailableTasks: %+v", c.AvailableTasks)
	// }
	if args.TaskType == "map" {
		c.MapIndex++
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
	c.Mu.Lock()
	defer c.Mu.Unlock()

	return c.TaskType == "end"
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapIndex:       0,
		ReduceIndex:    0,
		Filename:       files,
		MapNum:         len(files),
		ReduceNum:      nReduce,
		AvailableTasks: make([]Task, 8),
		RunningTasks:   make(map[int]Task),
	}

	for i, file := range files {
		task := Task{
			TaskType: "map",
			Filename: file,
			TaskId:   i,
		}
		c.AvailableTasks[i] = task
	}

	for i := 0; i < c.ReduceNum; i++ {
		task := Task{
			TaskType: "reduce",
			TaskId:   c.ReduceIndex,
		}
		c.ReduceIndex++
		c.AvailableTasks = append(c.AvailableTasks, task)
	}

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			c.Mu.Lock()
			for key, RunningTask := range c.RunningTasks {
				if RunningTask.WorkerId != 0 && time.Now().After(RunningTask.DeadLine) {
					// 回收并重新分配
					log.Printf(
						"Found timed-out %s task %d previously running on worker %d. Prepare to re-assign",
						RunningTask.TaskType, RunningTask.TaskId, RunningTask.WorkerId)
					RunningTask.WorkerId = 0
					c.AvailableTasks = append([]Task{RunningTask}, c.AvailableTasks...)
					delete(c.RunningTasks, key)
				}
			}
			c.Mu.Unlock()
		}
	}()

	c.server()
	return &c
}
