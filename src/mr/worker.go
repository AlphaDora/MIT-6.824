package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "strconv"
import "path/filepath"
import "encoding/json"
import "sort"
import "time"


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
	
	// Your worker implementation here.
	for {
		reply := TaskReply{}
		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
		ok := call("Coordinator.Task", &TaskArgs{}, &reply)
    	if !ok || !reply.Assigned { 
			time.Sleep(10 * time.Millisecond)
			continue 
		}

		if reply.TaskId == -1 {
			break
		}

		if reply.IsMap {
			//log.Printf("taskMap %v: enter, filename: %s", reply.TaskId, reply.Filename)
			taskMap(mapf, reply)
		} else {
			//log.Printf("taskReduce %v: enter, filename: %s", reply.TaskId, reply.Filename)
			taskReduce(reducef, reply)
		}
		call("Coordinator.TaskDone", &TaskArgs{TaskId: reply.TaskId, IsMap: reply.IsMap}, &TaskReply{})
		// go func(reply *TaskReply) {
		// 	if reply.IsMap {
		// 		log.Printf("taskMap %v: enter, filename: %s", reply.TaskId, reply.Filename)
		// 		taskMap(mapf, *reply)
		// 	} else {
		// 		log.Printf("taskReduce %v: enter, filename: %s", reply.TaskId, reply.Filename)
		// 		taskReduce(reducef, *reply)
		// 	}
		// 	call("Coordinator.TaskDone", &TaskArgs{TaskId: reply.TaskId, IsMap: reply.IsMap}, &TaskReply{})
		// }(&reply)
	}
}


func taskMap(mapf func(string, string) []KeyValue, reply TaskReply) {
	// map task
	// log.Printf("taskMap %v: begin", reply.TaskId)
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()
	kva := mapf(reply.Filename, string(content))
	// log.Printf("taskMap %v: complete, create intermediate files", reply.TaskId)
	// create nReduce intermediate files
	for i := 0; i < reply.NReduce; i++ {
		oname := "mr-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(i)
		if _, err := os.Stat(oname); os.IsNotExist(err) {
			ofile, _ := os.Create(oname)
			ofile.Close()
		}
	}
	sort.Sort(ByKey(kva))
	// log.Printf("taskMap %v: sort intermediate files", reply.TaskId)
	for _, kv := range kva {
		// i is the reduce task number
		i := ihash(kv.Key) % reply.NReduce
		oname := "mr-" + strconv.Itoa(reply.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_WRONLY, 0600)
		enc := json.NewEncoder(ofile)
		enc.Encode(kv)
		ofile.Close()
	}
}

func taskReduce(reducef func(string, []string) string, reply TaskReply) {
	for {
		call("Coordinator.CanReduceStart", &TaskArgs{TaskId: reply.TaskId}, &reply)
		if reply.CanReduceStart {
			break
		}
		time.Sleep(1 * time.Second)
	}
	// log.Printf("taskReduce %v: start", reply.TaskId)
	// reduce task
	rid := reply.TaskId
	pattern := fmt.Sprintf("mr-*-%d", rid)
	paths, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("cannot find %v", pattern)
	}
	intermediate := []KeyValue{}
	for _, path := range paths {
		ofile, _ := os.Open(path)
		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		ofile.Close()
		// delete the intermediate file
		// if err := os.Remove(path); err != nil {
		// 	log.Printf("failed to remove %s: %v", path, err)
		// }
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(rid)
	ofile, _ := os.Create(oname)
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
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

	// fmt.Println(err)
	return false
}
