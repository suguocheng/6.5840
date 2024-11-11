package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key } // 这是为了实现 sort.Interface 接口，从而对 Bykey 类型数据排序

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 初始化
	id := os.Getpid()
	log.Printf("Worker %d started\n", id)
	// args := ApplyForTaskArgs{
	// 	WorkerId: id,
	// }
	// reply := ApplyForTaskReply{}

	for {
		args := ApplyForTaskArgs{
			WorkerId: id,
		}
		reply := ApplyForTaskReply{}

		// rpc远程调用申请任务
		ok := call("Coordinator.ApplyForTask", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
		}

		// log.Printf("(Type)reply:%+v", reply)

		if reply.TaskType == "map" {
			log.Printf("filename = %s\n", reply.Filename)
			// log.Printf("Mapid = %d\n", reply.TaskId)

			// 打开并读取文件
			file, err := os.Open(reply.Filename) // 打开文件
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file) // 读取文件内容
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close() // 关闭文件
			kva := mapf(reply.Filename, string(content))

			// 按 Key 的 Hash 值对中间结果进行分桶
			hashedKva := make(map[int][]KeyValue)
			for _, kv := range kva {
				hashed := ihash(kv.Key) % reply.ReduceNum
				hashedKva[hashed] = append(hashedKva[hashed], kv)
			}

			// 将中间内容存入文件
			for i := 0; i < reply.ReduceNum; i++ {
				MapFile := fmt.Sprintf("mr-%d-%d", reply.TaskId, i)
				ofile, _ := os.OpenFile(MapFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
				for _, kv := range hashedKva[i] {
					fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
				}
				ofile.Close()
			}

			// 向协调者rpc通知任务完成
			args2 := FinishTaskArgs{
				TaskType: "map",
				TaskId:   reply.TaskId,
			}
			reply2 := FinishTaskReply{}
			ok := call("Coordinator.FinishTask", &args2, &reply2)
			if !ok {
				fmt.Printf("call failed!\n")
			}

		} else if reply.TaskType == "reduce" {
			log.Printf("ReduceId = %d\n", reply.TaskId)

			// 打开并读取文件
			var lines []string
			for i := 0; i < reply.MapNum; i++ {
				MapFile := fmt.Sprintf("mr-%d-%d", i, reply.TaskId)
				file, err := os.Open(MapFile) // 打开文件
				if err != nil {
					log.Fatalf("cannot open %v", reply.Filename)
				}
				content, err := ioutil.ReadAll(file) // 读取文件内容
				if err != nil {
					log.Fatalf("cannot read %v", reply.Filename)
				}
				file.Close() // 关闭文件
				lines = append(lines, strings.Split(string(content), "\n")...)
			}

			// 把文件内容转换成KeyValue类型
			// log.Printf("len(strcontent) = %d\n", len(strcontent))
			kva := []KeyValue{}
			for _, line := range lines {
				parts := strings.Split(line, " ")
				if len(parts) < 2 {
					continue // 跳过不符合预期格式的行
				}
				kva = append(kva, KeyValue{Key: parts[0], Value: parts[1]})
			}
			// log.Printf("len(kva) = %d\n", len(kva))

			// 按键排序
			sort.Sort(ByKey(kva))

			// 创建或打开文件
			ReduceFile := fmt.Sprintf("mr-out-%d", reply.TaskId)
			var ofile *os.File
			ofile, _ = os.Create(ReduceFile)

			//进行reduce操作
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			ofile.Close()

			// 向协调者rpc通知任务完成
			args2 := FinishTaskArgs{
				TaskType: "reduce",
				TaskId:   reply.TaskId,
			}
			reply2 := FinishTaskReply{}
			ok := call("Coordinator.FinishTask", &args2, &reply2)
			if !ok {
				fmt.Printf("call failed!\n")
			}

		} else if reply.TaskType == "wait" {
			time.Sleep(time.Second)
			continue
		} else if reply.TaskType == "end" {
			break
		}
	}
	log.Printf("Worker %d finished\n", id)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
