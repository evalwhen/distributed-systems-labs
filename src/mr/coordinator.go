package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var (
	ErrInvalidTaskID  = errors.New("invalid task id")
	ErrAllTaskDone    = errors.New("all task done")
	ErrWaitReduceDone = errors.New("wait reduce done")
	ErrWaitMapDone    = errors.New("wait map done")
)

type Coordinator struct {
	// Your definitions here.
	sync.RWMutex

	files       []string
	nReduce     int
	tasks       map[string]*Task
	mapTasks    map[int]*Task
	reduceTasks map[int]*Task
}

type Task struct {
	Id         int
	Type       TaskType
	File       string // maybe, abstract out it
	InterFiles []string
	Status     TaskStatus
	Nreduce    int
}

type TaskStatus int

const (
	TaskStatusDone TaskStatus = iota + 1
	TaskStatusInit
	TaskStatusFail
	TaskStatusRunning
)

func (ts *TaskStatus) String() string {
	switch *ts {
	case TaskStatusDone:
		return "done"
	case TaskStatusInit:
		return "init"
	case TaskStatusFail:
		return "fail"
	case TaskStatusRunning:
		return "running"
	default:
		return "unknown"
	}
}

func (t *Task) Ready() bool {
	return t.Status == TaskStatusInit || t.Status == TaskStatusFail
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	task, err := c.getTask()
	if err != nil {
		return err
	}

	// log.Printf("get task: %+v", *task)
	*reply = TaskReply{
		Task: task,
	}

	go func() {
		time.Sleep(10 * time.Second)

		c.Lock()
		if task.Status != TaskStatusDone {
			task.Status = TaskStatusFail
		}
		c.Unlock()
	}()

	return nil
}

func (c *Coordinator) Notify(args *NotifyArgs, reply *NotifyReply) error {
	err := c.taskDone(args.Task)
	if err != nil {
		// ignore err and send a new task if we have task
		return err
	}

	return nil
}

func (c *Coordinator) taskDone(task *Task) error {
	c.Lock()
	defer c.Unlock()

	// log.Printf("task %+v done", *task)

	var tas *Task
	if task.Type == TaskTypeMap {
		tas = c.mapTasks[task.Id]
	} else {
		tas = c.reduceTasks[task.Id]

	}
	if tas != nil {
		if tas.Status != TaskStatusFail {
			tas.Status = TaskStatusDone
		} else {
			// timeout
		}
	} else {
		return ErrInvalidTaskID
	}

	return nil
}

func (c *Coordinator) getTask() (*Task, error) {
	c.Lock()
	defer c.Unlock()

	var res *Task

	for _, task := range c.mapTasks {
		if task.Ready() {
			res = task
		}
	}

	if res != nil {

		res.Status = TaskStatusRunning

		return res, nil

	} else if c.mapDone() { // no map idle

		for _, task := range c.reduceTasks {
			if task.Ready() {
				res = task
			}
		}

		if res != nil {

			res.Status = TaskStatusRunning

			return res, nil

		} else if c.reduceDone() {

			return nil, ErrWaitReduceDone

		}
	} else {
		return nil, ErrWaitMapDone
	}

	return nil, ErrAllTaskDone
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
	c.RLock()
	defer c.RUnlock()

	return c.mapDone() && c.reduceDone()
}

func (c *Coordinator) mapDone() bool {
	ret := true

	for _, task := range c.mapTasks {
		if task.Status != TaskStatusDone {
			ret = false
			break
		}
	}

	return ret
}

func (c *Coordinator) reduceDone() bool {
	ret := true

	for _, task := range c.reduceTasks {
		if task.Status != TaskStatusDone {
			ret = false
			break
		}
	}
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce, mapTasks: make(map[int]*Task), reduceTasks: make(map[int]*Task)}

	for i, file := range files {
		c.mapTasks[i] = &Task{
			Id:      i,
			File:    file,
			Type:    TaskTypeMap,
			Status:  TaskStatusInit,
			Nreduce: nReduce,
		}
	}

	for j := 0; j < nReduce; j++ {
		task := &Task{
			Id: j,
			// File:    fmt.Sprintf("mr-%d-%d", i, j),
			InterFiles: make([]string, 0),
			Type:       TaskTypeReduce,
			Status:     TaskStatusInit,
			Nreduce:    nReduce,
		}

		c.reduceTasks[j] = task

		for x := 0; x < len(files); x++ {
			task.InterFiles = append(task.InterFiles, fmt.Sprintf("mr-%d-%d", x, j))
		}
	}

	log.Printf("get total %v maptasks and %v reducetask", len(c.mapTasks), len(c.reduceTasks))
	// Your code here.

	c.server()
	return &c
}
