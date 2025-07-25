package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	workerId, err := CallRegister()
	if err != nil {
		return
	}

	// 创建带取消功能的 context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // 确保 Worker 退出时取消心跳 goroutine

	var wg sync.WaitGroup

	// 心跳
	wg.Add(1)
	go func() {
		defer wg.Done()
		CallKeepLive(workerId, ctx)
	}()

	for {
		// 获取任务、执行任务、输出文件
		taskId, quit, err := CallGetTask(workerId, mapf, reducef)
		if err != nil {
			log.Println(err)
		}

		// master 通知退出
		if quit {
			ctx.Done() // 关闭心跳
			break
		}

		// 提交任务
		CallCommitTask(workerId, taskId)
	}

	wg.Wait()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// 获取任务，map 作业直接执行，reduce 作业则等待所有 map 把分区数据准备好再执行，返回输出文件名
func CallCommitTask(workerId string, taskId string) {
	args := CommitTaskArgs{workerId, taskId}
	reply := CommitTaskReply{}

	ok := call("Coordinator.CommitTask", &args, &reply)
	if ok {
		log.Println("get task success with id: ", reply.Accept)
	} else {
		log.Println("get task failed")
		return
	}

	taskType, realName, err := getTaskType(taskId)
	if err != nil {
		log.Println(err)
		return
	}

	if taskType == "map" {
		if reply.Accept {
			// 移除 map 作业的随机文件名后缀
			files, err := os.ReadDir(realName)
			if err != nil {
				panic(err)
			}

			for _, file := range files {
				oldPath := filepath.Join(realName, file.Name())
				dashIndex := strings.LastIndex(oldPath, "-")
				if dashIndex == -1 {
					continue
				}
				newPath := oldPath[:dashIndex]
				err := os.Rename(oldPath, newPath)
				if err != nil {
					panic(err)
				}
			}
		} else {
			err := os.RemoveAll(realName)
			if err != nil {
				println(err)
			}
		}
	} else if taskType == "reduce" {
		if reply.Accept {
			// reduce 创建的临时文件 TaskId/mr-out-%d-* ，提交被 master 接受后，会移动重命名为 ./mr-out-%d
			files, err := os.ReadDir(realName)
			if err != nil || len(files) == 0 {
				panic(err)
			}
			srcFile := files[0]
			srcName := srcFile.Name()
			srcPath := filepath.Join(realName, srcName)

			destName := srcName[:strings.LastIndex(srcName, "-")]
			destPath := filepath.Join(".", destName)

			err = os.Rename(srcPath, destPath)
			if err != nil {
				panic(err)
			}
		} else {
			err := os.RemoveAll(realName)
			if err != nil {
				println(err)
			}
		}
	} else {
		log.Println("unknow task type")
		return
	}
}

func CallGetTask(workerId string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) (string, bool, error) {
	args := GetTaskArgs{workerId}
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		log.Println("get task success with id: ", reply.TaskId)
	} else {
		log.Println("get task failed")
		return "", reply.Quit, fmt.Errorf("get task failed")
	}

	taskType, realName, err := getTaskType(reply.TaskId)
	if err != nil {
		return reply.TaskId, reply.Quit, err
	}

	content, err := read(realName)
	if err != nil {
		return reply.TaskId, reply.Quit, err
	}

	if taskType == "map" {
		kva := mapf("", string(content))
		partitionMap := make(map[int][]KeyValue)
		for _, kv := range kva {
			partition := ihash(kv.Key) % reply.NReduce
			if val, ok := partitionMap[partition]; !ok {
				partitionMap[partition] = []KeyValue{kv}
			} else {
				partitionMap[partition] = append(val, kv)
			}
		}

		for key, values := range partitionMap {
			// 写入临时文件，存放临时文件的目录为 TaskId 去除 map 前缀，文件名为 reduce-[partition]-*
			// todo map 的 commit 被 master 接受后，会重命名这些文化件为 reduce-[partition]-*
			temp, err := os.CreateTemp(realName, fmt.Sprintf("reduce-%d-*", key))
			if err != nil {
				return reply.TaskId, reply.Quit, err
			}

			enc := json.NewEncoder(temp)
			for _, kv := range values {
				err := enc.Encode(&kv)
				if err != nil {
					return reply.TaskId, reply.Quit, err
				}
			}
			temp.Close()
		}
		return reply.TaskId, reply.Quit, nil
	} else if taskType == "reduce" {
		// map 作业完成后，reduce 作业才会产生，执行 reduce 的逻辑
		// MapTaskIds 的格式是 map-pg-*
		// 获取的 TaskId 为 reduce-[partition] 格式
		partition, err := strconv.Atoi(strings.Split(reply.TaskId, "-")[1])
		if err != nil {
			log.Println(err)
			return reply.TaskId, reply.Quit, err
		}

		intermediate := []KeyValue{}
		for _, mapTaskId := range reply.MapTaskIds {
			// mapTaskId 代表了 map 作业输出的文件夹名，内部的分区文件名格式为 reduce-[partition]
			file, err := os.Open(filepath.Join(mapTaskId, reply.TaskId))
			if err != nil {
				log.Println(err)
				return reply.TaskId, reply.Quit, err
			}
			// 读中间文件的 kv 形式的 json
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
		}

		// 分组算法
		// 排序
		sort.Sort(ByKey(intermediate))

		// 收集
		for i := 0; i < len(intermediate); {
			j := i + 1
			values := []string{intermediate[i].Value}
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				values = append(values, intermediate[j].Value)
				j++
			}
			output := reducef(intermediate[i].Key, values)
			// 写入临时文件，存放临时文件的目录为 TaskId 去除 map 前缀，文件名为 reduce-[partition]-*
			// map 的 commit 被 master 接受后，会重命名这些文化件为 reduce-[partition]-*
			// 创建临时文件 TaskId/mr-out-%d-* 提交并被 master 接受后，会重命名为 mr-out-%d
			temp, err := os.CreateTemp(reply.TaskId, fmt.Sprintf("mr-out-%d-*", partition))
			if err != nil {
				log.Println(err)
				return reply.TaskId, reply.Quit, err
			}
			fmt.Fprintf(temp, "%v %v\n", intermediate[i].Key, output)

			i = j
			temp.Close()
		}
		return reply.TaskId, reply.Quit, nil
	} else {
		log.Println("unknow task type")
		return reply.TaskId, reply.Quit, fmt.Errorf("unknow task type")
	}
}

func CallKeepLive(id string, ctx context.Context) {
	ticker := time.NewTicker(7 * time.Second)
	defer func() {
		print("keep live exit")
		ticker.Stop()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			args := KeepAliveArgs{id}
			reply := KeepAliveReply{}
			ok := call("Coordinator.KeepLive", &args, &reply)
			if ok {
				log.Println("keep live success")
			} else {
				log.Println("keep live failed")
			}
		}
	}
}

func CallRegister() (string, error) {
	args := RegisterArgs{}
	reply := RegisterReply{}

	ok := call("Coordinator.Register", &args, &reply)
	if ok {
		log.Println("register worker success with id: ", reply.Id)
		return reply.Id, nil
	} else {
		log.Println("register worker failed")
		return "", fmt.Errorf("register worker failed")
	}
}

func read(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return nil, err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return nil, err
	}
	file.Close()
	return content, nil
}

func getTaskType(taskId string) (string, string, error) {
	parts := strings.SplitN(taskId, "-", 2)
	if len(parts) < 2 {
		return "", "", fmt.Errorf("unknow task type")
	}
	return parts[0], parts[1], nil
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	log.Printf("connect sock: %s", sockname)

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
