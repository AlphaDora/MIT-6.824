package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "errors"
import "time"


type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
	inputFiles []string
	nReduce int
	MapTasksList []MapTaskStatus
	ReduceTasksList []ReduceTaskStatus
	reduceTasksDone int
	mapTasksDone int
}

type TaskState int
const (
	Pending TaskState = iota
	InProgress
	Done
)

type MapTaskStatus struct {
	TaskId int
	Filename string
	State TaskState
	AssignedTimestamp time.Time
}

type ReduceTaskStatus struct {
	TaskId int
	State TaskState
	AssignedTimestamp time.Time
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

func (c *Coordinator) Task(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// map task alive checking
	for id, task := range c.MapTasksList {
		if task.State == InProgress && time.Since(task.AssignedTimestamp) > 10 * time.Second {
			c.MapTasksList[id].State = Pending
		}
	}

	// assign map task
	for id, task := range c.MapTasksList {
		if task.State == Pending {
			c.MapTasksList[id] = MapTaskStatus{
				TaskId: id,
				Filename: c.inputFiles[id],
				State: InProgress,
				AssignedTimestamp: time.Now(),
			}
			reply.Assigned = true
			reply.IsMap = true
			reply.TaskId = id
			reply.Filename = c.inputFiles[id]
			reply.NReduce = c.nReduce
			return nil
		}
	}
	
	if c.mapTasksDone < len(c.inputFiles) {
		reply.Assigned = false
		return nil
	}

	for id, task := range c.ReduceTasksList {
		if task.State == InProgress && time.Since(task.AssignedTimestamp) > 10 * time.Second {
			c.ReduceTasksList[id].State = Pending
		}
	}
	for id, task := range c.ReduceTasksList {
		if task.State == Pending {
			c.ReduceTasksList[id] = ReduceTaskStatus{
				TaskId: id,
				State: InProgress,
				AssignedTimestamp: time.Now(),
			}
			reply.Assigned = true
			reply.IsMap = false
			reply.TaskId = id
			reply.Filename = ""
			reply.NReduce = c.nReduce
			return nil
		}
	}
	
	if c.reduceTasksDone == c.nReduce {
		reply.TaskId = -1
		return errors.New("All tasks completed")
	}
	
	//log.Printf("No tasks available")
	return errors.New("No tasks available")
}

func (c *Coordinator) TaskDone(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.IsMap {
		c.mapTasksDone++
		c.MapTasksList[args.TaskId].State = Done
	} else {
		c.reduceTasksDone++
		c.ReduceTasksList[args.TaskId].State = Done
	}
	return nil
}

func (c *Coordinator) CanReduceStart(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapTasksDone == len(c.inputFiles) {
		reply.CanReduceStart = true
	} else {
		reply.CanReduceStart = false
	}
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
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceTasksDone == c.nReduce {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.inputFiles = files
	c.nReduce = nReduce
	// Your code here.
	c.MapTasksList = make([]MapTaskStatus, len(files))
	c.ReduceTasksList = make([]ReduceTaskStatus, nReduce)
	c.server()
	return &c
}
