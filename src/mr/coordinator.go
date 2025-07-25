package mr

import (
	"fmt"
	"github.com/google/uuid"
	"log"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	workersMu sync.Mutex
	workers   map[string]TaskWorker // worker 的 状态

	mapTasks    StatusTask // map task 的 状态
	reduceTasks StatusTask // reduce task 的 状态

	partitionMu sync.Mutex
	partitions  map[string][]string // 分区 key（reduce任务的key） -> map 产生 的中间文件，没产生也要设置为 ''

	m, n int // m -> map count, n -> reduce
}

type Status int

const (
	Idle Status = iota
	Busy
	Failed
	Completed
)

type TaskWorker struct {
	id            string
	status        Status
	lastKeepAlive int64
}

type Task struct {
	id         string
	status     Status
	workerId   string
	startMills int64
}

type TaskMap struct { // fix 没必要这种细粒度的锁
	tasksMu sync.Mutex
	tasks   map[string]Task
}

type StatusTask map[Status]*TaskMap

// Your code here -- RPC handlers for the worker to call.

// 同一任务首个提交的 worker 会被采纳，并允许重命名；其余的被拒绝
func (c *Coordinator) CommitTask(args *CommitTaskArgs, reply *CommitTaskReply) error {
	parts := strings.SplitN(args.TaskId, "-", 2)
	if len(parts) < 2 {
		return fmt.Errorf("unknow task type")
	}
	taskType, _ := parts[0], parts[1]

	var busies *TaskMap
	var completes *TaskMap
	if taskType == "map" {
		busies = c.mapTasks.getOrPut(Busy)
		completes = c.mapTasks.getOrPut(Completed)
	} else if taskType == "reduce" {
		busies = c.reduceTasks.getOrPut(Busy)
		completes = c.reduceTasks.getOrPut(Completed)
	} else {
		return fmt.Errorf("unknow task type")
	}

	busies.tasksMu.Lock()
	completes.tasksMu.Lock()
	defer busies.tasksMu.Unlock()
	defer completes.tasksMu.Unlock()

	if val, ok := busies.tasks[args.TaskId]; ok {
		delete(busies.tasks, args.TaskId)
		// 任务为 busy，转换为完成状态
		val.status = Completed
		// WorkerId 设置为第一个完成的 WorkerId
		val.workerId = args.WorkerId
		completes.tasks[args.TaskId] = val
		// 接受结果，允许重命名
		reply.Accept = true
		// 检查，如果所有 map 任务都完成，那么 reduce 任务可以开始创建
		// 这里只是简单的针对同一批输出的 map 作业，所以认为 tasks 只包含该批次的 map 作业
		if len(completes.tasks) >= c.m {
			reduceIdles := c.reduceTasks.getOrPut(Idle)
			reduceIdles.tasksMu.Lock()
			defer reduceIdles.tasksMu.Unlock()
			// 遍历 map 作业的 completes
			for i := 0; i < c.n; i++ {
				reduceKey := fmt.Sprintf("reduce-%d", i)
				reduceIdles.tasks[reduceKey] = Task{id: reduceKey, status: Idle, workerId: "", startMills: 0}
			}
		}
	} else {
		// 拒绝结果
		reply.Accept = false
	}
	return nil
}

// 获取 map 或 reduce 作业，如果有 idle map 则返回，否则返回长时间未完成的 map 作业（任务备份）。否则同理返回 redce 作业
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	reply.NReduce = c.n
	// 获取 map 作业
	err := c.mapTasks.getTask(args.WorkerId, reply)
	// 获取成功，直接返回
	if err == nil {
		return nil
	}
	// 获取 map 作业失败，获取 reduce 作业
	err = c.reduceTasks.getTask(args.WorkerId, reply)
	// reduce 任务需要所有的 MapTaskIds
	mapCompletes := c.mapTasks[Completed]
	mapCompletes.tasksMu.Lock()
	defer mapCompletes.tasksMu.Unlock()
	for key, _ := range mapCompletes.tasks {
		reply.MapTaskIds = append(reply.MapTaskIds, key)
	}

	if err == nil {
		return nil
	}

	// 如果所有 reduce 都执行完了，通知 worker 退出
	reduceCompletes := c.reduceTasks.getOrPut(Completed)
	if len(reduceCompletes.tasks) >= c.n {
		reply.Quit = true
		return fmt.Errorf("Quit")
	}

	// 也没有 reduce 作业，返回错误，也可以提交一个通知退出的任务
	return fmt.Errorf("no task")
}

// 做个简单的区分：原始输入的 map-pg-* 为 map 任务的 key，中间文件 reduce-[partition] 为 reduce 任务的 key
func (statusTask *StatusTask) getTask(workId string, reply *GetTaskReply) error {
	idles := statusTask.getOrPut(Idle)
	busies := statusTask.getOrPut(Busy)
	idles.tasksMu.Lock()
	busies.tasksMu.Lock()
	defer idles.tasksMu.Unlock() // 保证解锁的顺序，defer 是栈
	defer busies.tasksMu.Unlock()
	// 每个map生成 NReduce 个中间文件，存在 WorkerId to reduce-[partition] 中
	// 按照第一个备份任务执行完成来判断任务完成，并重命名，其他的备份任务不可重命名
	nowMills := time.Now().UnixMilli()

	for key, value := range idles.tasks {
		delete(idles.tasks, key)
		busies.tasks[key] = value
		value.status = Busy
		value.workerId = workId
		value.startMills = nowMills
		reply.TaskId = key
		return nil
	}

	// 没获取到 idle 任务，考虑备份任务（落伍者）
	for key, value := range busies.tasks {
		overtime := 10 * time.Second.Milliseconds()
		if nowMills-value.startMills > overtime {
			value.startMills = nowMills // 重新开始计时
			reply.TaskId = key
			return nil
		}
	}

	// 也没有执行时间过长的 busy 任务
	return fmt.Errorf("no task")
}

func (c *Coordinator) KeepLive(args *KeepAliveArgs, reply *KeepAliveReply) error {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()
	worker := c.workers[args.Id]
	worker.lastKeepAlive = time.Now().UnixMilli()
	if worker.status == Failed {
		worker.status = Idle // 为了简单起见，只是重新设置为 idle
	}
	reply = &KeepAliveReply{}
	println("keep alive: ", args.Id, " ", worker.status, " ", worker.lastKeepAlive)
	return nil
}

func (c *Coordinator) Register(args *RegisterArgs, reply *RegisterReply) error {
	c.workersMu.Lock()
	defer c.workersMu.Unlock()
	id := uuid.NewString()
	c.workers[id] = TaskWorker{id, Idle, time.Now().UnixMilli()}
	log.Println("register worker: ", id)
	reply.Id = id
	return nil
}

func (statusTasks StatusTask) getOrPut(key Status) *TaskMap {
	if statusTasks[key] == nil {
		statusTasks[key] = &TaskMap{tasksMu: sync.Mutex{}, tasks: make(map[string]Task)}
	}
	return statusTasks[key]
}

func (statusTask *StatusTask) recoveryTask(workerId string) bool {
	busies := statusTask.getOrPut(Busy)
	busies.tasksMu.Lock()
	defer busies.tasksMu.Unlock()
	for taskId, task := range busies.tasks {
		if task.workerId == workerId {
			task.status = Idle
			task.workerId = ""
			task.startMills = 0
			idles := statusTask.getOrPut(Idle)
			idles.tasksMu.Lock()
			idles.tasks[taskId] = task
			idles.tasksMu.Unlock()
			delete(busies.tasks, taskId)
			return true
		}
	}
	return false
}

func (c *Coordinator) checkWorkers() {
	for {
		c.workersMu.Lock()
		for workerId, worker := range c.workers {
			if worker.status == Failed {
				exec := c.mapTasks.recoveryTask(workerId)
				if !exec {
					c.mapTasks.recoveryTask(workerId)
				}
			}
		}
		c.workersMu.Unlock()
		time.Sleep(5 * time.Second)
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	log.Printf("regist service object: %T on rpc", c)
	rpc.HandleHTTP()
	log.Println("bind rpc on HTTP router")
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	log.Println("remove existed sock: ", sockname)
	l, e := net.Listen("unix", sockname)
	log.Println("relisten : ", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		log.Printf("rpc server started, listening on %v ...\n", sockname)
		http.Serve(l, nil)
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = len(c.reduceTasks[Completed].tasks) == c.n
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.workers = make(map[string]TaskWorker)
	c.mapTasks = make(map[Status]*TaskMap)
	c.m = len(files) - 1
	c.n = 5

	for _, name := range files[1:] {
		taskMap := c.mapTasks.getOrPut(Idle)
		tasks := taskMap.tasks
		mapTaskKey := fmt.Sprintf("%s-%s", "map", name)
		tasks[mapTaskKey] = Task{mapTaskKey, Idle, "", 0}
	}
	fmt.Printf("tasks: %+v\n", c.mapTasks[Idle].tasks)

	c.server()

	// 开启协程检查失败的 worker，将任务重新设置为 idle
	go c.checkWorkers()

	return &c
}
