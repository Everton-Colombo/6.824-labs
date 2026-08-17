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

type MasterTaskStatus int

const (
	NotStarted MasterTaskStatus = iota
	InProgress
	Completed
)

type MapTask struct {
	inputFile string
	status    MasterTaskStatus // 0: not started, 1: in progress, 2: completed
}

type ReduceTask struct {
	bucket int
	status MasterTaskStatus // 0: not started, 1: in progress, 2: completed
}

type Master struct {
	mu          sync.Mutex
	mapTasks    []MapTask
	reduceTasks []ReduceTask

	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	anyMapInProgress := false
	for i := range m.mapTasks {
		if m.mapTasks[i].status == NotStarted {
			m.mapTasks[i].status = InProgress
			reply.TaskType = MapTaskType
			reply.FileName = m.mapTasks[i].inputFile
			reply.NReduce = m.nReduce
			reply.TaskNum = i

			time.AfterFunc(10*time.Second, func() { m.CheckTaskCompleted(MapTaskType, i) })

			fmt.Printf("[master] Answering to GetTask with TaskType = MAP, TaskNum = %v, FileName = %v, NReduce = %v\n",
				reply.TaskNum, reply.FileName, reply.NReduce)

			return nil
		} else if m.mapTasks[i].status == InProgress {
			anyMapInProgress = true
		}
	}

	if anyMapInProgress {
		reply.TaskType = WaitTaskType
		fmt.Printf("[master] Answering to GetTask with TaskType = WAIT\n")
		return nil
	}

	allReducesCompleted := true
	for i := range m.reduceTasks {
		if m.reduceTasks[i].status == NotStarted {
			m.reduceTasks[i].status = InProgress
			reply.TaskType = ReduceTaskType
			reply.NReduce = m.nReduce
			reply.TaskNum = i

			time.AfterFunc(10*time.Second, func() { m.CheckTaskCompleted(ReduceTaskType, i) })

			fmt.Printf("[master] Answering to GetTask with TaskType = REDUCE, TaskNum = %v, NReduce = %v\n", reply.TaskNum, reply.NReduce)
			return nil
		} else if m.reduceTasks[i].status == InProgress {
			allReducesCompleted = false
		}
	}

	if allReducesCompleted {
		reply.TaskType = ExitTaskType
		fmt.Printf("[master] Answering to GetTask with TaskType = EXIT\n")
	} else {
		reply.TaskType = WaitTaskType
		fmt.Printf("[master] Answering to GetTask with TaskType = WAIT\n")
	}
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Printf("[master] Received ReportTask: TaskType = %v, TaskNum = %v, Status = %v\n", args.TaskType, args.TaskNum, args.Status)

	if args.TaskType == MapTaskType {
		if args.Status == Success { // success
			m.mapTasks[args.TaskNum].status = Completed
		} else { // failure
			m.mapTasks[args.TaskNum].status = NotStarted
		}
	} else if args.TaskType == ReduceTaskType {
		if args.Status == Success { // success
			m.reduceTasks[args.TaskNum].status = Completed
		} else { // failure
			m.reduceTasks[args.TaskNum].status = NotStarted
		}
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	for _, val := range m.mapTasks {
		if val.status != Completed {
			fmt.Println("[master] Not all map tasks completed yet. Not exiting...")
			return false
		}
	}

	for _, val := range m.reduceTasks {
		if val.status != Completed {
			return false
		}
	}

	fmt.Println("[master] All tasks completed. Exiting...")

	return true
}

func (m *Master) CheckTaskCompleted(taskType TaskType, taskNum int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Print("[master] CheckTaskCompleted() called")

	if taskType == MapTaskType {
		if m.mapTasks[taskNum].status == InProgress {
			fmt.Printf("[master] Map task %v timed out. Marking it as NotStarted.\n", taskNum)
			m.mapTasks[taskNum].status = NotStarted
		}
	} else if taskType == ReduceTaskType {
		if m.reduceTasks[taskNum].status == InProgress {
			fmt.Printf("[master] Reduce task %v timed out. Marking it as NotStarted.\n", taskNum)
			m.reduceTasks[taskNum].status = NotStarted
		}
	}
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Populate mapTasks
	for _, file := range files {
		m.mapTasks = append(m.mapTasks, MapTask{inputFile: file, status: NotStarted})
	}

	// Populate reduceTasks
	for i := 0; i < nReduce; i++ {
		m.reduceTasks = append(m.reduceTasks, ReduceTask{bucket: i, status: NotStarted})
	}

	m.nReduce = nReduce

	m.server()
	return &m
}
