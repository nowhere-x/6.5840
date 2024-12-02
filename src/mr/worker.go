package mr

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	INFO = iota
	WARNING
	FATAL
)

func log_message(msg string, level int) {
	switch level {
	case INFO:
		log.Println("INFO", msg)
	case WARNING:
		log.Println("WARNING: ", msg)
	case FATAL:
		log.Println("FATAL: ", msg)
	}
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

func map_task(response *TaskResponse, mapf func(string, string) []KeyValue) {
	// read target file(input)
	file_name := response.TargetFiles[0]
	file, err := os.Open(file_name)
	if err != nil {
		log_message("Missing File "+file_name, FATAL)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log_message(file_name+" read failed", WARNING)
	}
	file.Close()

	word_list := mapf(file_name, string(content))
	intermediate := make([][]KeyValue, response.NReduce)

	// hash words into NReduce buckets
	for _, kv := range word_list {
		bucket := ihash(kv.Key) % response.NReduce
		intermediate[bucket] = append(intermediate[bucket], kv)
	}

	// encode bucket to intermediate JSON files
	for bucket, bucket_list := range intermediate {
		oname := fmt.Sprintf("./mr-%d-%d.json", bucket, response.TaskID)
		ofile, err := os.Create(oname)
		if err != nil {
			log_message(oname+" create failed", WARNING)
			continue
		}

		encoder := json.NewEncoder(ofile)
		encoder.Encode(bucket_list)
		ofile.Close()
	}

	// Update status
	notify_request := TaskRequest{
		WorkerID: response.WorkerID,
		TaskID:   response.TaskID,
		Status:   FINISHED,
		TaskType: MAP,
	}
	notify_response := TaskResponse{}
	call("Coordinator.CompleteTask", &notify_request, &notify_response)
}

func reduce_task(response *TaskResponse, reducef func(string, []string) string) {
	// Load intermediate files
	intermediate := []KeyValue{}
	for i := 0; i < len(response.TargetFiles); i++ {
		file, err := os.Open(response.TargetFiles[i])
		if err != nil {
			log.Printf("cannot open %v", response.TargetFiles[i])
			continue
		}
		decoder := json.NewDecoder(file)

		var bucket_list []KeyValue
		decoder.Decode(&bucket_list)
		intermediate = append(intermediate, bucket_list...)
		file.Close()
	}

	// Sort intermediate key-value pairs by key
	sort.Sort(ByKey(intermediate))

	// Create output file
	oname := fmt.Sprintf("mr-out-%d", response.TaskID)
	ofile, _ := os.Create(oname)

	// Apply reduce function
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		word_count_list := []string{}
		for k := i; k < j; k++ {
			word_count_list = append(word_count_list, intermediate[k].Value)
		}
		word_count := reducef(intermediate[i].Key, word_count_list)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, word_count)
		i = j
	}

	// Close output file
	ofile.Close()

	// Update status
	notify_request := TaskRequest{
		WorkerID: response.WorkerID,
		TaskID:   response.TaskID,
		Status:   FINISHED,
		TaskType: REDUCE,
	}
	notify_response := TaskResponse{}
	call("Coordinator.CompleteTask", &notify_request, &notify_response)
}

func generateID() (string, error) {
	// Allocate a byte slice for 4 bytes (32 bits)
	bytes := make([]byte, 4)
	// Fill the slice with random bytes
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}
	// Convert the bytes to a hexadecimal string
	return fmt.Sprintf("%08x", bytes), nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	worker_id, _ := generateID()
	// Your worker implementation here.
	for {
		request := TaskRequest{
			WorkerID: worker_id,
			TaskID:   -1,
			Status:   UNASSIGNED,
			TaskType: -1,
		}
		response := TaskResponse{}

		res := call("Coordinator.RequestTask", &request, &response)
		if !res {
			break
		}

		//log.Print(response)
		switch response.Status {
		case EXIT:
			os.Exit(0)
		case MAP_IN_PROGRESS:
			map_task(&response, mapf)
		case REDUCE_IN_PROGRESS:
			reduce_task(&response, reducef)
		case WORKER_ID_EXIST:
			continue
		default:
			time.Sleep(1 * time.Second)
		}
		//time.Sleep(3 * time.Second)
	}
}
