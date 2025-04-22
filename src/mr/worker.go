package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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

	id := registerWorker()
	log.Printf("Worker %d registered", id)
	args := TaskArgs{id}

	for {
		reply := TaskReply{}
		if ok := call("Coordinator.RequestTask", &args, &reply); !ok {
			log.Fatalf("Worker %d failed to request task", id)
		}

		switch reply.TaskType {
		case MapTask:
			content := readContent(reply.FileName)
			kva := mapf(reply.FileName, content)
			encodeIntermediate(kva, reply.NReduce, reply.TaskID)
			call("Coordinator.CompleteTask", &args, &struct{}{})

		case ReduceTask:
			kva := readIntermediate(reply.NMap, reply.TaskID)
			m := groupByKey(kva)

			fname := fmt.Sprintf("mr-out-%d", reply.TaskID)
			file, err := os.Create(fname)
			if err != nil {
				log.Fatalf("Worker %d cannot create %v: %v", id, fname, err)
			}

			for k, v := range m {
				result := reducef(k, v)
				fmt.Fprintf(file, "%v %v\n", k, result)
			}
			file.Close()
			log.Printf("Worker %d completed reduce task %d, wrote to %s", id, reply.TaskID, fname)
			call("Coordinator.CompleteTask", &args, &struct{}{})

		case Done:
			time.Sleep(time.Second)

		default:
			log.Fatalf("Worker %d received unknown task type: %v", id, reply.TaskType)
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
	counts := make([]int, nReduce)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % nReduce
		enc := json.NewEncoder(files[reduceID])
		if err := enc.Encode(&kv); err != nil {
			log.Fatalf("Worker %d cannot encode %v: %v", mapID, kv, err)
		}
		counts[reduceID]++
	}

	// Close all files
	for i, file := range files {
		file.Close()
		log.Printf("Worker %d wrote %d key-value pairs to mr-%d-%d", mapID, counts[i], mapID, i)
	}
}

func registerWorker() int {
	args := struct{}{}
	reply := RegisterReply{}
	if ok := call("Coordinator.Register", &args, &reply); !ok {
		log.Fatalf("Worker failed to register")
	}
	return reply.WorkerID
}

func readContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	return string(content)
}

func readIntermediate(nMap int, reduceID int) []KeyValue {
	kva := []KeyValue{}
	// Read intermediate files from all map tasks for this reduce task
	for mapID := range nMap {
		filename := fmt.Sprintf("mr-%d-%d", mapID, reduceID)
		file, err := os.Open(filename)
		if err != nil {
			if !os.IsNotExist(err) {
				log.Printf("Worker %d cannot open %v: %v", reduceID, filename, err)
			}
			continue
		}

		dec := json.NewDecoder(file)
		count := 0
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err != io.EOF {
					log.Printf("Worker %d cannot decode %v: %v", reduceID, filename, err)
				}
				break
			}
			kva = append(kva, kv)
			count++
		}
		file.Close()
		log.Printf("Worker %d read %d key-value pairs from %s", reduceID, count, filename)
	}
	return kva
}

func groupByKey(kva []KeyValue) map[string][]string {
	m := make(map[string][]string)
	for _, kv := range kva {
		m[kv.Key] = append(m[kv.Key], kv.Value)
	}
	return m
}
