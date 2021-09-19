package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

var (
	ErrNotFound      = errors.New("empty task list")
	ErrInvalidTaskID = errors.New("invalid task id")
)

type Coordinator struct {
	// Your definitions here.
	sync.RWMutex
	files   []string
	nReduce int
	tasks   map[string]*Task
}

type Task struct {
	Id     string
	Type   TaskType
	File   string // maybe, abstract out it
	Status TaskStatus
}

type TaskStatus int

const (
	TaskStatusDone TaskStatus = iota + 1
	TaskStatusInit
	TaskStatusFail
	TaskStatusIdle
)

func (ts *TaskStatus) String() string {
	switch *ts {
	case TaskStatusDone:
		return "done"
	case TaskStatusInit:
		return "init"
	case TaskStatusFail:
		return "fail"
	case TaskStatusIdle:
		return "idle"
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
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	task, err := c.getTask()
	if err != nil {
		return err
	}

	reply = &TaskReply{
		task: task,
	}

	return nil
}

func (c *Coordinator) Notify(args *NotifyArgs, reply *NotifyReply) error {
	err := c.taskDone(args.taskId, args.interFileName)
	if err != nil {
		// ignore err and send a new task if we have task
		log.Println(err)
	}

	task, err := c.getTask()
	if err != nil {
		return err
	}

	reply = &NotifyReply{
		task: task,
	}

	return nil
}

func (c *Coordinator) taskDone(id, interFileName string) error {
	c.Lock()
	defer c.Unlock()

	if task, ok := c.tasks[id]; ok {
		task.Status = TaskStatusDone
		// assume interFileName is not empty
		if task.Type == TaskTypeMap {
			c.tasks[interFileName] = &Task{
				Id:     interFileName,
				File:   interFileName,
				Type:   TaskTypeReduce,
				Status: TaskStatusInit,
			}
		}
	} else {
		return ErrInvalidTaskID
	}

	return nil
}

func (c *Coordinator) getTask() (*Task, error) {
	c.RLock()
	defer c.RUnlock()

	for _, task := range c.tasks {
		if task.Ready() {
			return task, nil
		}
	}
	return nil, ErrNotFound
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

	if len(c.tasks) == 2*len(c.files) {
		for _, task := range c.tasks {
			if task.Status != TaskStatusDone {
				ret = false
				break
			}
		}
	} else {
		ret = false
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
	c := Coordinator{files: files, nReduce: nReduce, tasks: make(map[string]*Task)}

	for _, file := range files {
		c.tasks[file] = &Task{
			Id:     file,
			File:   file,
			Type:   TaskTypeMap,
			Status: TaskStatusInit,
		}
	}
	// Your code here.

	c.server()
	return &c
}
