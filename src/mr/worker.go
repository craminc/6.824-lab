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
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

type TmpFile struct {
	tmpPath string
	enc     *json.Encoder
}

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := doGetTask()
		switch reply.TaskType {
		case MAP:
			doMap(reply, mapf)
			completeMap(reply)
		case REDUCE:
			doReduce(reply, reducef)
			completeMap(reply)
		case NONE:
			// fmt.Println("wait for task")
			time.Sleep(time.Second)
		case COMPLETE:
			// fmt.Println("work complete, exit")
			return
		}
	}

}

func completeMap(taskReply GetTaskReply) {
	args := CompleteTaskArgs{}
	args.TaskId = taskReply.TaskId
	args.TaskType = taskReply.TaskType
	reply := CompleteTaskReply{}
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if !ok {
		fmt.Printf("call [CompleteTask] failed!\n")
	}
}

//
// get a task from coordinator
//
func doGetTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Printf("call [GetTask] failed!\n")
	}
	return reply
}

//
// do map func
//
func doMap(reply GetTaskReply, mapf func(string, string) []KeyValue) {
	// fmt.Printf("map task %d start\n", reply.TaskId)
	for _, filename := range reply.FileNames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		kva := mapf(filename, string(content))

		tmpFiles := make([]TmpFile, reply.NReduce)
		for i := 0; i < reply.NReduce; i++ {
			tfile, _ := ioutil.TempFile("/tmp/tmpFile", "tmp-")
			enc := json.NewEncoder(tfile)
			tmpFile := TmpFile{}
			tmpFile.tmpPath = tfile.Name()
			tmpFile.enc = enc

			tmpFiles[i] = tmpFile
		}

		for _, kv := range kva {
			hash := ihash(kv.Key) % reply.NReduce
			err := tmpFiles[hash].enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write %v", kv)
			}
		}

		for i, tf := range tmpFiles {
			mname := fmt.Sprintf("tmp/mr-%d-%d", reply.TaskId, i)
			os.Rename(tf.tmpPath, mname)
			// rm tmp file
			defer os.Remove(tf.tmpPath)
		}
	}
}

//
// do reduce func
//
func doReduce(reply GetTaskReply, reducef func(string, []string) string) bool {
	intermediate := []KeyValue{}
	// fmt.Printf("reduce task %d start\n", reply.TaskId)
	for _, filename := range reply.FileNames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	oname := "mr-out-" + strconv.Itoa(reply.TaskId)
	ofile, _ := os.Create(oname)

	sort.Sort(ByKey(intermediate))

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	return true
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
