package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strings"
import "sort"

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// rpc远程调用申请任务
	id := os.Getpid()
	log.Printf("Worker %d started\n", id)
	args := ApplyForTaskArgs{
		WorkerId: id,
	}
	reply := ApplyForTaskReply{}
	ok := call("Coordinator.ApplyForTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}

	log.Printf("filename = %s\n", reply.Filename)

	if reply.TaskType == "map" {
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
			temname := fmt.Sprintf("map-%d", i)
			_, err := os.Stat(temname)
			var ofile *os.File
			if os.IsNotExist(err) {
				ofile, _ = os.Create(temname)
				for _, kv := range hashedKva[i] {
					fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
				}
				ofile.Close()
			} else {
				ofile, _ = os.OpenFile(temname, os.O_APPEND|os.O_WRONLY, 0644)
				for _, kv := range hashedKva[i] {
					fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
				}
				ofile.Close()
			}
		}
	} else if reply.TaskType == "reduce" {
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

		// 把文件内容转换成KeyValue类型
		strcontent := string(content)
		kva := []KeyValue{}
		lines := strings.Split(strcontent, "\n")
		for _, line := range lines {
			parts := strings.Split(line, "\t")
			if len(parts) < 2 {
				continue // 跳过不符合预期格式的行
			}
			kva = append(kva, KeyValue{Key: parts[0], Value: parts[1]})
		}

		// 按键排序
		sort.Sort(ByKey(kva))

		// 创建或打开文件
		_, err = os.Stat("tem-reduce")
		var ofile *os.File
		if os.IsNotExist(err) {
			ofile, _ = os.Create("tem-reduce")
		} else {
			ofile, _ = os.OpenFile("tem-reduce", os.O_APPEND|os.O_WRONLY, 0644)
		}

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

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

			i = j
		}
		ofile.Close()
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

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
