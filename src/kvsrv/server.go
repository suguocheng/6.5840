package kvsrv

import (
	"log"
	"sync"
	// "time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// type Task struct {
// 	TaskType string
// 	TaskId   int64
// 	DeadLine time.Time
// }

type KVServer struct {
	Mu  sync.Mutex
	Map map[string]string
	// RunningTasks map[int64]Task
	TaskQueue  []int64
	EndedTasks map[int64]string
	head       int
	tail       int
	size       int
}

func (kv *KVServer) Add(taskId int64, value string) {
	// 如果队列已满，移除最旧的任务
	if (kv.tail+1)%kv.size == kv.head {
		// 删除最旧的任务
		oldest := kv.TaskQueue[kv.head]
		delete(kv.EndedTasks, oldest)
		kv.head = (kv.head + 1) % kv.size
	}

	// 将新任务添加到队列
	kv.TaskQueue[kv.tail] = taskId
	kv.tail = (kv.tail + 1) % kv.size
	kv.EndedTasks[taskId] = value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	// if _, ok := kv.EndedTasks[args.TaskId]; !ok {
	// task := Task{
	// 	TaskType: "Get",
	// 	DeadLine: time.Now().Add(5 * time.Second),
	// 	TaskId:   args.TaskId,
	// }

	// kv.RunningTasks[task.TaskId] = task

	reply.Value = kv.Map[args.Key]
	// delete(kv.RunningTasks, task.TaskId)
	// }
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	if _, ok := kv.EndedTasks[args.TaskId]; !ok {
		// task := Task{
		// 	TaskType: "Put",
		// 	DeadLine: time.Now().Add(5 * time.Second),
		// 	TaskId:   args.TaskId,
		// }

		// kv.RunningTasks[task.TaskId] = task

		kv.Map[args.Key] = args.Value
		// log.Printf("key: %s value: %s\n", args.Key, args.Value)
		kv.Add(args.TaskId, "")
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	// for _, Task := range kv.TaskQueue {
	// 	log.Printf("Value: %d ", Task)
	// }
	// log.Printf("\n")
	// for key, value := range kv.EndedTasks {
	// 	log.Printf("key: %d, value: %s\n", key, value)
	// }
	// log.Printf("\n")

	if _, ok := kv.EndedTasks[args.TaskId]; !ok {
		// task := Task{
		// 	TaskType: "Put",
		// 	DeadLine: time.Now().Add(5 * time.Second),
		// 	TaskId:   args.TaskId,
		// }

		// kv.RunningTasks[task.TaskId] = task

		value := kv.Map[args.Key]
		kv.Map[args.Key] += args.Value
		// log.Printf("key: %s value: %s\n", args.Key, args.Value)
		reply.Value = value
		kv.Add(args.TaskId, value)

	} else {
		reply.Value = kv.EndedTasks[args.TaskId]
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.

	kv.Map = make(map[string]string)
	// kv.RunningTasks = make(map[int64]Task)
	kv.TaskQueue = make([]int64, 15)
	kv.EndedTasks = make(map[int64]string)
	kv.size = 15

	// go func() {
	// 	for {
	// 		time.Sleep(500 * time.Millisecond)

	// 		kv.Mu.Lock()
	// 		for key, RunningTask := range kv.RunningTasks {
	// 			if time.Now().After(RunningTask.DeadLine) {
	// 				// 超时删除
	// 				log.Printf(
	// 					"Found timed-out %s task %d previously running. Prepare to re-assign",
	// 					RunningTask.TaskType, RunningTask.TaskId)
	// 				delete(kv.RunningTasks, key)
	// 			}
	// 		}
	// 		kv.Mu.Unlock()
	// 	}
	// }()

	return kv
}
