package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task, err := FetchTask()
		if err != nil {
			if err.Error() == ErrAllTaskDone.Error() {
				break
			} else if err.Error() == ErrWaitMapDone.Error() || err.Error() == ErrWaitReduceDone.Error() {
				continue
			} else {
				// panic(err) // master quit
				log.Printf("master quit: %v", err.Error())
				break
			}
		}
		if task.Type == TaskTypeMap {
			err := executeMap(mapf, task)
			if err != nil {
				panic(err)
			}

		} else if task.Type == TaskTypeReduce {
			err := executeReduce(reducef, task)
			if err != nil {
				panic(err)
			}
		} else {
			// log.Printf("task %+v", *task)
			log.Println("unkown task type")
		}
	}
}

type IntermediateFile struct {
	tmpFile     *os.File
	realName    string
	jsonEncoder *json.Encoder
}

func executeMap(mapf func(string, string) []KeyValue, task *Task) error {
	file, err := os.Open(task.File)
	if err != nil {
		log.Fatalf("cannot open %v", task.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.File)
	}
	file.Close()
	// os.Rename(oldpath string, newpath string)
	tmpFiles := make(map[int]*IntermediateFile)
	for i := 0; i < task.Nreduce; i++ {
		f, err := ioutil.TempFile("./", fmt.Sprintf("mr-%d-%d-*", task.Id, i))
		if err != nil {
			return err
		}
		tmpFiles[i] = &IntermediateFile{
			tmpFile:     f,
			realName:    fmt.Sprintf("mr-%d-%d", task.Id, i),
			jsonEncoder: json.NewEncoder(f),
		}
	}

	kva := mapf(task.File, string(content))
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % task.Nreduce
		t := tmpFiles[reduceId]
		err := t.jsonEncoder.Encode(&kv)
		if err != nil {
			return err
		}
	}

	for _, t := range tmpFiles {
		err := os.Rename(t.tmpFile.Name(), "./"+t.realName)
		if err != nil {
			return err
		}
		err = t.tmpFile.Close()
		if err != nil {
			return err
		}
	}

	err = TaskDone(task)
	if err != nil {
		panic(err)
	}

	return nil
}

func executeReduce(reducef func(string, []string) string, task *Task) error {

	kva := make([]KeyValue, 0)
	for _, interFile := range task.InterFiles {
		file, err := os.Open(interFile)
		if err != nil {
			log.Fatalf("cannot open %v", interFile)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		defer file.Close()
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d", task.Id)
	ofile, _ := ioutil.TempFile("./", fmt.Sprintf("mr-out-%d-*", task.Id))

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

	os.Rename(ofile.Name(), "./"+oname)

	ofile.Close()

	err := TaskDone(task)
	if err != nil {
		panic(err)
	}

	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func FetchTask() (*Task, error) {

	// declare an argument structure.
	args := TaskArgs{}

	// declare a reply structure.

	var reply TaskReply
	// send the RPC request, wait for the reply.
	err := call("Coordinator.GetTask", &args, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Task, nil
}

func TaskDone(task *Task) error {

	// declare an argument structure.
	args := NotifyArgs{}
	args.Task = task

	// declare a reply structure.

	var reply NotifyReply
	// send the RPC request, wait for the reply.
	err := call("Coordinator.Notify", &args, &reply)
	if err != nil {
		return err
	}
	return nil
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return err
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}
