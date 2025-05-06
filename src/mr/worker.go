package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	for {
		workId := os.Getpid()
		//log.Printf( "get workId: %d", workId)
		args := Args{
			WorkId:         workId,
			FinishTaskType: "",
			FinishTaskId:   -1,
		}
		reply := Reply{}

		//通过RPC连接Coordinator
		call("Coordinator.RequestTasks", &args, &reply)
		log.Printf("call Coordinator")

		// 处理各自的任务分区
		switch reply.TaskType {
		case MAP:
			//log.Printf("workId: %d start Map %d", workId, reply.TaskId)
			// 读取输入文件的内容
			filename := reply.FileName
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("file: %v open failed\n", filename)
			}

			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("file: %v read failed\n", filename)
			}
			//log.Printf("content: %v", content)

			_ = file.Close()
			// 获取键值对数组
			kva := mapf(filename, string(content))
			// 将键值对分成nReduce个桶
			intermediate := make([][]KeyValue, reply.NReduce)
			for _, kv := range kva {
				bucket := ihash(kv.Key) % reply.NReduce
				intermediate[bucket] = append(intermediate[bucket], kv)
			}

			// 将每个桶放到临时文件中,生成中间文件格式为：mr-X-Y
			for i := 0; i < reply.NReduce; i++ {
				tempFile, err := os.CreateTemp("", "mr-tmp-*")
				if err != nil {
					log.Fatalf("create TempFile failed")
				}
				enc := json.NewEncoder(tempFile)
				for _, kv := range intermediate[i] {
					err = enc.Encode(&kv)
					if err != nil {
						log.Fatalf("encode TempFile failed")
					}
				}
				_ = tempFile.Close()
				// 将临时文件重新命名
				os.Rename(tempFile.Name(), fmt.Sprintf("mr-%d-%d", reply.TaskId, i))
			}
			// 汇报任务完成
			args.FinishTaskType = MAP
			args.FinishTaskId = reply.TaskId

			call("Coordinator.ReportTasks", &args, &reply)

		case REDUCE:
			intermediate := []KeyValue{}

			// 读取中间文件

			for _, reduceFile := range reply.ReduceFiles {
				//log.Printf("intermediate: %v", reduceFile)
				file, err := os.Open(reduceFile)
				if err != nil {
					log.Fatalf("%d reduceFile %v open failed\n", reply.TaskId, file)
				}

				// 解析JSON格式键值对
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			// 将键值对进行排序
			sort.Sort(ByKey(intermediate))

			// 创建reduce任务的输出文件
			tempFile, err := os.CreateTemp("", "mr-out-tmp-*")
			if err != nil {
				log.Fatalf("create Reduce TempFile failed")
			}

			// 对每个key调用Reduce函数
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			tempFile.Close()

			// 将临时文件重新命名
			os.Rename(tempFile.Name(), fmt.Sprintf("mr-out-%d", reply.TaskId))

			// 报告reduce任务完成
			args.FinishTaskType = REDUCE
			args.FinishTaskId = reply.TaskId
			call("Coordinator.ReportTasks", &args, &reply)

		case DONE:
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
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
