package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/fs"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key:
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeToIntermediateFile(keyVals []KeyValue, taskNum int, nReduce int) {
	// Write the key-value pairs to intermediate files for the corresponding reduce task.
	// Appends to the file if it already exists, otherwise creates a new file.
	// Returns: the name of the intermediate file.

	// sort keyvals by bucket
	sort.Slice(keyVals, func(i, j int) bool {
		return ihash(keyVals[i].Key)%nReduce < ihash(keyVals[j].Key)%nReduce
	})

	// delete all existing intermediate files for this map task
	for i := 0; i < nReduce; i++ {
		intermediateFilename := fmt.Sprintf("mr-%d-%d", taskNum, i)
		if _, err := os.Stat(intermediateFilename); err == nil {
			err := os.Remove(intermediateFilename)
			if err != nil {
				log.Fatalf("cannot remove existing intermediate file %v: %v", intermediateFilename, err)
			}
		}
	}

	for i := 0; i < len(keyVals); {
		j := i + 1
		for j < len(keyVals) && ihash(keyVals[j].Key)%nReduce == ihash(keyVals[i].Key)%nReduce {
			j++
		}

		tempFilename := fmt.Sprintf("mr-%d-%d.tmp", taskNum, ihash(keyVals[i].Key)%nReduce)
		intermediateFilename := fmt.Sprintf("mr-%d-%d", taskNum, ihash(keyVals[i].Key)%nReduce)
		file, err := os.CreateTemp("", tempFilename)
		if err != nil {
			log.Fatalf("cannot open temp intermediate file %v", tempFilename)
		}

		encoder := json.NewEncoder(file)
		for _, kv := range keyVals[i:j] {
			err := encoder.Encode(kv)
			if err != nil {
				log.Fatalf("cannot encode key-value pair %v", kv)
			}
		}
		tmpFilepath := file.Name()
		file.Close()

		// Rename the temporary file to the final name:
		err = os.Rename(tmpFilepath, intermediateFilename)
		if err != nil {
			log.Fatalf("cannot rename temp intermediate file %v to %v", tempFilename, intermediateFilename)
		}

		i = j
	}
}

func doMapTask(mapf func(string, string) []KeyValue, inputFileName string, taskNum int, nReduce int) {
	bytes, err := os.ReadFile(inputFileName)
	if err != nil {
		log.Fatalf("cannot read %v", inputFileName)
	}
	keyVals := mapf(inputFileName, string(bytes))
	writeToIntermediateFile(keyVals, taskNum, nReduce)
	CallReportTask(MapTaskType, taskNum, Success)
}

func findFilesByRegex(root string, pattern *regexp.Regexp) ([]string, error) {
	var matches []string

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && pattern.MatchString(d.Name()) {
			matches = append(matches, path)
		}
		return nil
	})

	return matches, err
}

func doReduceTask(reducef func(string, []string) string, bucket int) {
	intermediates := []KeyValue{}

	// Read all files mr-*-bucket and decode the key-value pairs.
	pattern := regexp.MustCompile(fmt.Sprintf(`^mr-\d+-%d$`, bucket))
	files, err := findFilesByRegex(".", pattern)
	if err != nil {
		log.Fatalf("cannot find intermediate files for bucket %d: %v", bucket, err)
	}
	// fmt.Printf("[worker] doReduceTask(): found files for bucket %d: %v\n", bucket, files)

	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open intermediate file %v", filename)
		}

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediates = append(intermediates, kv)
		}
		file.Close()
	}

	// fmt.Println("[worker] doReduceTask(): finished reading intermediate files for bucket ", bucket,
	// 	"total key-value pairs: ", len(intermediates))

	sort.Sort(ByKey(intermediates))
	outFile, _ := os.CreateTemp("", fmt.Sprintf("mr-tmp-out-%d", bucket))

	for i := 0; i < len(intermediates); {
		j := i + 1
		for j < len(intermediates) && intermediates[j].Key == intermediates[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediates[k].Value)
		}
		output := reducef(intermediates[i].Key, values)

		fmt.Fprintf(outFile, "%v %v\n", intermediates[i].Key, output)

		i = j
	}
	tmpFilepath := outFile.Name()
	outFile.Close()
	os.Rename(tmpFilepath, fmt.Sprintf("mr-out-%d", bucket))
	CallReportTask(ReduceTaskType, bucket, Success)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for true {
		taskReply := CallGetTask()

		switch taskReply.TaskType {
		case MapTaskType:
			fmt.Printf("[worker (%v)] Received MAP task: TaskNum = %v, FileName = %v, NReduce = %v\n",
				os.Getpid(), taskReply.TaskNum, taskReply.FileName, taskReply.NReduce)
			doMapTask(mapf, taskReply.FileName, taskReply.TaskNum, taskReply.NReduce)
		case ReduceTaskType:
			fmt.Printf("[worker (%v)] Received REDUCE task: TaskNum = %v, NReduce = %v\n", os.Getpid(), taskReply.TaskNum, taskReply.NReduce)
			doReduceTask(reducef, taskReply.TaskNum)
		case WaitTaskType:
			fmt.Printf("[worker (%v)] Received WAIT task. Sleeping for 1 second...\n", os.Getpid())
			time.Sleep(1 * time.Second)
		case ExitTaskType:
			fmt.Printf("[worker (%v)] Received EXIT task. Exiting...\n", os.Getpid())
			return
		}
	}

}

func CallGetTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	call("Master.GetTask", &args, &reply)

	return reply
}

func CallReportTask(taskType TaskType, taskNum int, status TaskReportStatus) {
	args := ReportTaskArgs{taskType, taskNum, status}
	reply := ReportTaskReply{}
	call("Master.ReportTask", &args, &reply)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
