package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	READY = iota + 1
	PROCESS
	FINISH
	WAIT
)
const (
	MAP = iota + 1
	REDUCE
	COMPLETE
	NONE
)

type Task struct {
	id        int
	fileNames []string
	start     time.Time
	status    int
	taskType  int
}

type Coordinator struct {
	// Your definitions here.
	tasks   []Task
	nReduce int
	nMap    int
	done    bool
}

var mutex sync.Mutex

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	mutex.Lock()
	if c.done {
		reply.TaskType = COMPLETE
	} else {
		reply.TaskType = NONE

		for i := range c.tasks {
			task := &c.tasks[i]
			cur := time.Now()
			if task.status == PROCESS && cur.Sub(task.start).Seconds() >= 10 {
				// fmt.Println("process task timeout")
				task.status = READY
			}
			if task.status == READY {
				task.start = cur
				task.status = PROCESS
				reply.TaskId = task.id
				reply.TaskType = task.taskType
				reply.FileNames = task.fileNames
				reply.NReduce = c.nReduce
				break
			}
		}
	}
	mutex.Unlock()
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	mutex.Lock()
	if !c.done {
		for i := range c.tasks {
			task := &c.tasks[i]
			if task.taskType == args.TaskType && task.id == args.TaskId && task.status == PROCESS {
				task.status = FINISH
				// fmt.Printf("task-%d-%d finish\n", task.taskType, task.id)
			}
		}
	}
	mutex.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	mutex.Lock()

	// map task
	mapl := len(c.tasks) - c.nReduce
	for i := 0; i < mapl; i++ {
		task := c.tasks[i]
		if task.status != FINISH {
			ret = false
			break
		}
	}
	if ret {
		for i := mapl; i < len(c.tasks); i++ {
			task := &c.tasks[i]
			if task.status == WAIT {
				task.status = READY
			}
			if task.status != FINISH {
				ret = false
			}
		}
	}

	// if ret {
	// 	fmt.Println("task complete, exit")
	// }
	c.done = ret

	mutex.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.tasks = make([]Task, c.nMap+nReduce)
	for i, fileName := range files {
		task := Task{}
		task.id = i
		task.fileNames = []string{fileName}
		task.taskType = MAP
		task.status = READY
		c.tasks[i] = task
	}
	c.nReduce = nReduce
	for i := 0; i < nReduce; i++ {
		task := Task{}
		task.id = i
		task.fileNames = []string{}
		task.taskType = REDUCE
		task.status = WAIT
		for j := 0; j < c.nMap; j++ {
			task.fileNames = append(task.fileNames, fmt.Sprintf("tmp/mr-%d-%d", j, i))
		}
		c.tasks[i+c.nMap] = task
	}
	// create tmp dir
	os.Mkdir("tmp", 0755)

	c.server()
	return &c
}
