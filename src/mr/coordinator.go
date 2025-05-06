package mr

import (
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu         sync.Mutex
	mapTask    chan *Task // map任务队列
	reduceTask chan *Task // reduce任务队列
	nReduce    int
	mMap       int
	mapDone    bool
	reduceDone bool
	tasks      map[string]*Task // 任务的映射(key：任务类型+索引）
}

type Task struct {
	FileName    string
	TaskId      int       // 任务Id
	TaskType    string    // 任务类型 Map or Reduce
	StartTime   time.Time // 开始时间
	State       string    // 空闲、正在进行、已完成
	ReduceFiles []string  // 待reduce的文件
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RequestTasks 分配任务
func (c *Coordinator) RequestTasks(args *Args, reply *Reply) error {
	//log.Printf("Coordinator get Call %+v %+v", *args, *reply)

	// 如果map还没执行完，分配map任务
	//log.Println("try get lock")
	c.mu.Lock()
	//log.Println("get lock")
	defer c.mu.Unlock()

	if !c.mapDone {
		select {
		case task := <-c.mapTask:
			reply.TaskType = MAP
			reply.FileName = task.FileName
			reply.TaskId = task.TaskId
			reply.NReduce = c.nReduce

			// 更新任务状态
			task.State = InProgress
			task.StartTime = time.Now()
			return nil
		default:
			// 可能的情况：如果找不到一个空闲任务，但还未执行完(InProgress)
			time.Sleep(100 * time.Millisecond)
			return nil
		}

	} else if c.mapDone && !c.reduceDone { // 如果所有map任务执行完则分配reduce任务
		select {
		case task := <-c.reduceTask:
			reply.TaskType = REDUCE
			reply.TaskId = task.TaskId
			reply.NReduce = c.nReduce
			reply.ReduceFiles = task.ReduceFiles

			// 更新任务状态
			task.State = InProgress
			task.StartTime = time.Now()
			return nil
		default:
			// 可能的情况：如果找不到一个空闲任务，但还未执行完(InProgress)
			time.Sleep(100 * time.Millisecond)
			return nil
		}
	} else if c.mapDone && c.reduceDone {
		reply.TaskType = DONE
		return nil
	}
	return nil
}

func (c *Coordinator) checkTimeOut() {
	// 超时时间10秒
	for {
		// 设置两秒检查一次任务时间
		time.Sleep(2 * time.Second)
		c.mu.Lock()
		now := time.Now()
		for _, task := range c.tasks {
			// 如果任务状态处于正在进行，并且完成时间已经大于10秒
			if task.State == InProgress && now.Sub(task.StartTime) > 10*time.Second {
				// 修改任务状态
				task.State = Idle
				// 维持相应的map映射
				c.tasks[task.TaskType+strconv.Itoa(task.TaskId)] = task
				// log.Printf("task: %v", c.tasks[task.TaskType+strconv.Itoa(task.TaskId)])
				if task.TaskType == MAP {
					c.mapTask <- task
				} else if task.TaskType == REDUCE {
					c.reduceTask <- task
				}
			}
		}
		c.mu.Unlock()
	}
}

// ReportTasks 汇报任务状态
func (c *Coordinator) ReportTasks(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 接受汇报来的任务Id和任务类型，并查询该任务是否存在
	key := args.FinishTaskType + strconv.Itoa(args.FinishTaskId)
	task, exist := c.tasks[key]
	// 根据是否存在和任务状态来判断
	if !exist || task.State != InProgress {
		return nil
	}
	// 更新任务状态已完成
	task.State = Completed
	// 如果是map任务
	if args.FinishTaskType == MAP {
		c.mMap--
		if c.mMap == 0 {
			c.mapDone = true
			// 初始化reduce任务
			for i := 0; i < c.nReduce; i++ {
				task = &Task{
					TaskId:      i,
					TaskType:    REDUCE,
					ReduceFiles: c.getIntermediateFiles(i),
				}
				c.tasks[task.TaskType+strconv.Itoa(task.TaskId)] = task
				c.reduceTask <- task
			}
		}
	} else if args.FinishTaskType == REDUCE {
		c.nReduce--
		if c.nReduce == 0 {
			c.reduceDone = true
		}
	}
	return nil
}

// getIntermediateFiles 获取带处理的Reduce文件
func (c *Coordinator) getIntermediateFiles(reduceId int) []string {
	files, err := filepath.Glob(fmt.Sprintf("mr-*-%d", reduceId))
	if err != nil {
		log.Fatal("file glob failed")
	}
	return files
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mapDone && c.reduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTask:    make(chan *Task, len(files)),
		reduceTask: make(chan *Task, nReduce),
		nReduce:    nReduce,
		mMap:       len(files),
		mapDone:    false,
		reduceDone: false,
		tasks:      make(map[string]*Task),
	}

	// Your code here.

	// 初始化Map任务
	for i, filename := range files {
		task := &Task{
			TaskId:   i,
			FileName: filename,
			State:    Idle,
			TaskType: MAP,
		}
		c.tasks[task.TaskType+strconv.Itoa(task.TaskId)] = task
		c.mapTask <- task
	}

	c.server()

	// 启动一个goroutine检查任务状态，处理超时任务
	go c.checkTimeOut()

	return &c
}
