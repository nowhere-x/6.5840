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

type Coordinator struct {
	// Your definitions here.
	Mutex      sync.Mutex
	InputFiles []string

	MapTasks    []TaskResponse
	ReduceTasks []TaskResponse

	Workers map[string]int

	MapRemaining    int
	MapRunning      int
	ReduceRemaining int
	ReduceRunning   int

	NReduce int
	TimeOut time.Duration
}

// Your code here -- RPC handlers for the worker to call.

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

func (c *Coordinator) CompleteTask(request *TaskRequest, response *TaskResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// check if it is a valid task completed
	if request.TaskType == MAP && c.MapTasks[request.TaskID].Status != MAP_IN_PROGRESS {
		return nil
	} else if request.TaskType == REDUCE && c.ReduceTasks[request.TaskID].Status != REDUCE_IN_PROGRESS {
		return nil
	}

	c.Workers[request.WorkerID] = FINISHED
	switch request.TaskType {
	case MAP:
		{
			c.MapTasks[request.TaskID].Status = FINISHED
			c.MapRemaining--
			c.MapRunning--
		}
	case REDUCE:
		{
			c.ReduceTasks[request.TaskID].Status = FINISHED
			c.ReduceRemaining--
			c.ReduceRunning--
		}
	}

	return nil
}

func (c *Coordinator) RequestTask(request *TaskRequest, response *TaskResponse) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	_, ok := c.Workers[request.WorkerID]
	if !ok {
		c.Workers[request.WorkerID] = WAIT
	}

	// assign a new map task
	if (c.MapRemaining - c.MapRunning) > 0 {
		for i := 0; i < len(c.MapTasks); i++ {
			if c.MapTasks[i].Status == UNASSIGNED {
				c.MapTasks[i].Status = MAP_IN_PROGRESS
				c.MapTasks[i].TimeStamp = time.Now()
				*response = c.MapTasks[i]

				c.MapRunning++
				break
			}
		}
	} else if c.MapRemaining > 0 { // no map task to assign, still running
		response.Status = WAIT
	} else if (c.ReduceRemaining - c.ReduceRunning) > 0 {
		// log.Print(c.ReduceRemaining, c.ReduceRunning)

		for i := 0; i < len(c.ReduceTasks); i++ { // assign a new reduce task
			if c.ReduceTasks[i].Status == UNASSIGNED {
				c.ReduceTasks[i].Status = REDUCE_IN_PROGRESS
				c.ReduceTasks[i].TimeStamp = time.Now()
				*response = c.ReduceTasks[i]

				c.ReduceRunning++
				break
			}
		}
	} else if c.ReduceRemaining > 0 { // no reduce task to assign, still running
		response.Status = WAIT
	} else {
		response.Status = EXIT
	}
	return nil
}

func (c *Coordinator) ResetDeadTasks() {
	var task *[]TaskResponse
	var status int
	var running *int
	if c.MapRemaining > 0 {
		task = &c.MapTasks
		status = MAP_IN_PROGRESS
		running = &c.MapRunning

	} else {
		task = &c.ReduceTasks
		status = REDUCE_IN_PROGRESS
		running = &c.ReduceRunning
	}

	endtime := time.Now()
	for i := 0; i < len(*task); i++ {
		if status == (*task)[i].Status && endtime.Sub((*task)[i].TimeStamp) > c.TimeOut {
			(*task)[i].Status = UNASSIGNED
			(*running)--
		}
	}
}

func (c *Coordinator) TimeoutCheck() {
	for {
		// check every 1 seconds
		time.Sleep(1 * time.Second)

		// since Reset need to access shared variables, mutex needed
		c.Mutex.Lock()
		c.ResetDeadTasks()
		c.Mutex.Unlock()
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// remove ResetDeadTasks() here to a new coroutine
	return c.MapRemaining == 0 && c.ReduceRemaining == 0
}

func IntermediateFiles(bucket int, nReduce int) []string {
	var files []string
	for i := 0; i < nReduce; i++ {
		file_name := fmt.Sprintf("./mr-%v-%v.json", bucket, i)
		files = append(files, file_name)
	}
	return files
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.

	timeout, _ := time.ParseDuration("10s")
	coordinator := Coordinator{
		Mutex:           sync.Mutex{},
		InputFiles:      files,
		MapTasks:        make([]TaskResponse, len(files)),
		ReduceTasks:     make([]TaskResponse, nReduce),
		Workers:         map[string]int{},
		MapRemaining:    len(files),
		MapRunning:      0,
		ReduceRemaining: nReduce,
		ReduceRunning:   0,
		NReduce:         nReduce,
		TimeOut:         timeout,
	}

	// init MapTasks
	for i := range coordinator.MapTasks {
		coordinator.MapTasks[i] = TaskResponse{
			TaskID:      i,
			Status:      UNASSIGNED,
			TargetFiles: []string{files[i]},
			NReduce:     coordinator.NReduce,
			// WorkerID and Timestamp will be generated when task is assigned
		}
	}

	// init ReduceTasks
	for i := range coordinator.ReduceTasks {
		coordinator.ReduceTasks[i] = TaskResponse{
			TaskID:      i,
			Status:      UNASSIGNED,
			TargetFiles: IntermediateFiles(i, len(files)),
			NReduce:     coordinator.NReduce,
			// WorkerID, Timestamp and TargetFiles will be generated when task is assigned
		}
	}

	coordinator.server()

	// set a new coroutine to periodically check timeout
	go coordinator.TimeoutCheck()

	return &coordinator
}
