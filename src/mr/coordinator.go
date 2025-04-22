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

type Coordinator struct {
	mu sync.Mutex

	mapTasks   map[int]*task
	mapAllDone bool

	reduceTasks map[int]*task

	workers map[int]*worker
}

type task struct {
	fileName string
	status   taskStatus
}

type taskStatus int

const (
	idle taskStatus = iota
	running
	completed
)

type worker struct {
	taskType TaskType
	taskID   int
	started  time.Time
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	mp := make(map[int]*task)
	for i, file := range files {
		mp[i] = &task{file, idle}
	}

	rd := make(map[int]*task)
	for i := range nReduce {
		rd[i] = &task{"", idle}
	}

	c := Coordinator{
		mapTasks:    mp,
		reduceTasks: rd,
		workers:     make(map[int]*worker),
	}

	go checkWorkerTimeout(&c)
	c.server()
	log.Println("Coordinator started")
	return &c
}

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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, task := range c.reduceTasks {
		if task.status != completed {
			return false
		}
	}

	c.cleanUpIntermediateFiles()
	log.Println("Coordinator done")
	return true
}

func (c *Coordinator) Register(args *TaskArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	workerID := len(c.workers)
	c.workers[workerID] = &worker{Done, -1, time.Now()}
	reply.WorkerID = workerID
	return nil
}

func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	worker, ok := c.workers[args.WorkerID]
	if !ok {
		return errors.New("worker not registered")
	}

	if worker.taskType != Done {
		return errors.New("worker is busy")
	}

	if !c.mapAllDone {
		// map tasks
		taskID := getTask(c.mapTasks)

		reply.TaskType = MapTask
		reply.TaskID = taskID
		reply.FileName = c.mapTasks[taskID].fileName
		reply.NReduce = len(c.reduceTasks)

		worker.taskType = MapTask
		worker.taskID = taskID
		worker.started = time.Now()

	} else {
		// reduce tasks
		taskID := getTask(c.reduceTasks)
		if taskID == -1 {
			return errors.New("no reduce tasks available")
		}

		reply.TaskType = ReduceTask
		reply.TaskID = taskID
		reply.NMap = len(c.mapTasks)

		worker.taskType = ReduceTask
		worker.taskID = taskID
		worker.started = time.Now()
	}

	return nil
}

func getTask(tasks map[int]*task) int {
	for i, task := range tasks {
		if task.status == idle {
			return i
		}
	}
	return -1
}

func (c *Coordinator) CompleteTask(args *TaskArgs, reply *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	worker := c.workers[args.WorkerID]

	switch worker.taskType {
	case MapTask:
		log.Printf("Worker %d completed map task %d (file: %s)", args.WorkerID, worker.taskID, c.mapTasks[worker.taskID].fileName)
		c.mapTasks[worker.taskID].status = completed
		c.checkMapAllDone()
	case ReduceTask:
		log.Printf("Worker %d completed reduce task %d", args.WorkerID, worker.taskID)
		c.reduceTasks[worker.taskID].status = completed
	case Done:
		return errors.New("worker already completed task")
	default:
		return errors.New("unknown task type")
	}

	worker.taskType = Done
	return nil
}

func (c *Coordinator) checkMapAllDone() {
	for _, task := range c.mapTasks {
		if task.status != completed {
			return
		}
	}
	c.mapAllDone = true
}

func checkWorkerTimeout(c *Coordinator) {
	for {
		c.mu.Lock()
		for _, worker := range c.workers {
			if time.Since(worker.started) > 10*time.Second {
				log.Println("Worker timed out:", worker.taskID)
				worker.taskType = Done
			}
		}
		c.mu.Unlock()
		time.Sleep(5 * time.Second)
	}
}

func (c *Coordinator) cleanUpIntermediateFiles() {
	for i := range len(c.mapTasks) {
		for j := range len(c.reduceTasks) {
			fname := fmt.Sprintf("mr-%d-%d", i, j)
			if err := os.Remove(fname); err != nil && !os.IsNotExist(err) {
				log.Printf("Error removing intermediate file %s: %v", fname, err)
			}
		}
	}
}
