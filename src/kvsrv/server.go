package kvsrv

import (
	"fmt"
	"log"
	"sync"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Task struct {
	TaskType string
	UUID     string
	DeadLine time.Time
}

type KVServer struct {
	Mu           sync.Mutex
	Map          map[string]string
	RunningTasks map[string]Task
	EndedTasks   map[string]Task
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	task := Task{
		TaskType: "Get",
		DeadLine: time.Now().Add(5 * time.Second),
		UUID:     fmt.Sprintf("%d-%d", args.ClientId, args.TaskId),
	}

	kv.RunningTasks[task.UUID] = task

	if _, ok := kv.EndedTasks[task.UUID]; !ok {
		reply.Value = kv.Map[args.Key]
	}

	delete(kv.RunningTasks, task.UUID)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	task := Task{
		TaskType: "Put",
		DeadLine: time.Now().Add(5 * time.Second),
		UUID:     fmt.Sprintf("%d-%d", args.ClientId, args.TaskId),
	}

	kv.RunningTasks[task.UUID] = task

	if _, ok := kv.EndedTasks[task.UUID]; !ok {
		kv.Map[args.Key] = args.Value
	}

	delete(kv.RunningTasks, task.UUID)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.Mu.Lock()
	defer kv.Mu.Unlock()

	task := Task{
		TaskType: "Put",
		DeadLine: time.Now().Add(5 * time.Second),
	}

	UUID := fmt.Sprintf("%d-%d", args.ClientId, args.TaskId)
	kv.RunningTasks[UUID] = task

	if _, ok := kv.EndedTasks[UUID]; !ok {
		value := kv.Map[args.Key]
		kv.Map[args.Key] += args.Value
		reply.Value = value
	}

	delete(kv.RunningTasks, UUID)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.

	kv.Map = make(map[string]string)
	kv.RunningTasks = make(map[string]Task)
	kv.EndedTasks = make(map[string]Task)

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			kv.Mu.Lock()
			for key, RunningTask := range kv.RunningTasks {
				if time.Now().After(RunningTask.DeadLine) {
					// 超时删除
					log.Printf(
						"Found timed-out %s task %s previously running. Prepare to re-assign",
						RunningTask.TaskType, RunningTask.UUID)
					delete(kv.RunningTasks, key)
				}
			}
			kv.Mu.Unlock()
		}
	}()

	return kv
}
