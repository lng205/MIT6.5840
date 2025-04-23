package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	id, nMap, nReduce := registerWorker()
	args := TaskArgs{id}

	for {
		reply := TaskReply{}
		if ok := call("Coordinator.RequestTask", &args, &reply); !ok {
			log.Fatalf("Worker %d failed to request task", id)
		}

		// log.Printf("Worker %d received task %d of type %d", id, reply.TaskID, reply.TaskType)

		switch reply.TaskType {
		case MapTask:
			doMap(reply.FileName, reply.TaskID, nReduce, mapf)
			call("Coordinator.CompleteTask", &args, &struct{}{})

		case ReduceTask:
			doReduce(reply.TaskID, id, nMap, reducef)
			call("Coordinator.CompleteTask", &args, &struct{}{})

		case NoTask:
			time.Sleep(time.Second)

		case End:
			return
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		os.Exit(1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func registerWorker() (int, int, int) {
	args := struct{}{}
	reply := RegisterReply{}
	if ok := call("Coordinator.Register", &args, &reply); !ok {
		log.Fatalf("Worker failed to register")
	}
	return reply.WorkerID, reply.NMap, reply.NReduce
}

func doMap(filename string, taskID, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Worker %d cannot open %v: %v", taskID, filename, err)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Worker %d cannot read %v: %v", taskID, filename, err)
	}
	file.Close()

	kva := mapf(filename, string(content))
	encodeIntermediate(kva, nReduce, taskID)
}

func encodeIntermediate(kva []KeyValue, nReduce int, mapID int) {
	// Create intermediate files for each reduce task
	files := make([]*os.File, nReduce)
	for i := range nReduce {
		filename := fmt.Sprintf("mr-%d-%d", mapID, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Worker %d cannot create %v: %v", mapID, filename, err)
		}
		files[i] = file
	}

	// Write each key-value pair to the appropriate reduce task's file
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % nReduce
		fmt.Fprintf(files[reduceID], "%s %s\n", kv.Key, kv.Value)
	}

	for _, file := range files {
		file.Close()
	}
}

func doReduce(taskID, workerID, nMap int, reducef func(string, []string) string) {
	fname := fmt.Sprintf("mr-out-%d", taskID)
	file, err := os.Create(fname)
	if err != nil {
		log.Fatalf("Worker %d cannot create %v: %v", workerID, fname, err)
	}
	defer file.Close()

	kva := readIntermediate(nMap, taskID)
	m := make(map[string][]string)
	for _, kv := range kva {
		m[kv.Key] = append(m[kv.Key], kv.Value)
	}

	for k, v := range m {
		result := reducef(k, v)
		fmt.Fprintf(file, "%s %s\n", k, result)
	}
}

func readIntermediate(nMap, reduceID int) []KeyValue {
	kva := []KeyValue{}
	// Read intermediate files from all map tasks for this reduce task
	for mapID := range nMap {
		filename := fmt.Sprintf("mr-%d-%d", mapID, reduceID)
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("Worker cannot open %v: %v", filename, err)
			continue
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, " ", 2)
			if len(parts) != 2 {
				log.Printf("Worker %d invalid line format in %v: %v", reduceID, filename, line)
				continue
			}
			kva = append(kva, KeyValue{Key: parts[0], Value: parts[1]})
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Worker %d error scanning %v: %v", reduceID, filename, err)
		}
		file.Close()
	}
	return kva
}
