package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

	// Your worker implementation here.

	workerID := -1

	for true {
		// 请求任务
		// Declare a request structure
		args := TaskReq{}
		// Declare a response structure
		reply := TaskResp{}

		args.WorkerID = workerID
		args.HaveFinishedTask = true

		// Send the RPC request, wait for the response
		ok := call("Coordinator.TaskRequest", &args, &reply)
		if !ok {
			log.Fatal("call failed")
		}

		if workerID == -1 {
			workerID = reply.WorkerID
		}
		if reply.AllTasksDone {
			log.Printf("Worker[%d]: All tasks done", workerID)
			return
		}

		if reply.HaveNewTask {
			log.Printf("Worker[%d]: Have new task", workerID)
			if reply.NewTask.TaskType == Map {
				log.Printf("Worker[%d]: Map task", workerID)
				filename := reply.NewTask.Filename
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()

				intermediate := []KeyValue{}
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
				// log.Printf("Worker[%d]: intermediate -> %v    NReduce -> %v", intermediate, reply.NewTask.NReduce)
				tmp_files, err := writeIntermediateToTmpFiles(intermediate, reply.NewTask.NReduce, workerID)
				if err != nil {
					log.Println("doMap.writeIntermediateToTmpFiles :", err)
				}
				mapTaskDone(tmp_files, workerID, reply.NewTask.TaskID)
			} else if reply.NewTask.TaskType == Reduce {
				log.Printf("Worker[%d]: Reduce task", workerID)
				intermediate, err := collectIntermediateFiles(reply.NewTask.NReduce)
				if err != nil {
					log.Println("doReduce.collectIntermediate err = ", err)
				}
				sort.Sort(ByKey(intermediate))

				res := make([]KeyValue, 0)
				i := 0
				for i < len(intermediate) {
					//  the key in intermediate [i...j]  is the same since intermediate is already sorted
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}

					// sum the val number of intermediate [i...j]
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					kv := KeyValue{Key: intermediate[i].Key, Value: output}
					res = append(res, kv)

					i = j
				}

				tmp_file, err := writeReduceResToTmpFile(res, workerID)
				if err != nil {
					log.Println("doReduce.writeReduceResToTmpFile err = ", err)
				}
				reduceTaskDone([]string{tmp_file}, workerID, reply.NewTask.TaskID)
			}

		}
		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func mapTaskDone(tmp_files []string, WorkerID int, TaskId int) error {
	args := TaskReq{}
	// Declare a response structure
	reply := TaskResp{}

	args.WorkerID = WorkerID
	args.HaveFinishedTask = true
	args.FinishedTask = Task{
		TaskID:   TaskId,
		TaskType: Map,
	}

	args.TemporaryFiles = tmp_files

	// Send the RPC request, wait for the response
	ok := call("Coordinator.TaskRequest", &args, &reply)
	if !ok {
		log.Fatal("call failed")
	}

	log.Printf("Worker[%d]: Map task done", WorkerID)

	return nil
}

func reduceTaskDone(tmp_files []string, WorkerID int, TaskId int) error {
	args := TaskReq{}
	// Declare a response structure
	reply := TaskResp{}

	args.WorkerID = WorkerID
	args.HaveFinishedTask = true
	args.FinishedTask = Task{
		TaskID:   TaskId,
		TaskType: Reduce,
	}

	args.TemporaryFiles = tmp_files

	// Send the RPC request, wait for the response
	ok := call("Coordinator.TaskRequest", &args, &reply)
	if !ok {
		log.Fatal("call failed")
	}

	log.Printf("Worker[%d]: Reduce task done", WorkerID)
	return nil
}

func writeIntermediateToTmpFiles(intermediate []KeyValue, NReduce int, WorkerID int) ([]string, error) {

	tmp_files := []string{}
	hashed_intermediate := make([][]KeyValue, NReduce)

	for _, kv := range intermediate {
		hash_val := ihash(kv.Key) % NReduce
		hashed_intermediate[hash_val] = append(hashed_intermediate[hash_val], kv)
	}

	for i := 0; i < NReduce; i++ {
		tmp_file, err := os.CreateTemp(TmpMapFilePath, "mr-*.txt")
		if err != nil {
			log.Println("writeIntermediateToTmpFiles.os.CreateTemp err = ", err)
			return nil, err
		}
		defer os.Remove(tmp_file.Name())
		defer tmp_file.Close()

		enc := json.NewEncoder(tmp_file)
		for _, kv := range hashed_intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Println("writeIntermediateToTmpFiles.enc.Encode", err)
				return nil, err
			}
		}

		file_path := fmt.Sprintf("mr-%v-%v", WorkerID, i)
		err = os.Rename(tmp_file.Name(), TmpMapFilePath+file_path)
		if err != nil {
			log.Println("writeIntermediateToTmpFiles os.Rename: ", err)
			return nil, err
		}
		tmp_files = append(tmp_files, TmpMapFilePath+file_path)
	}

	return tmp_files, nil
}

func collectIntermediateFiles(NReduce int) ([]KeyValue, error) {
	intermediate := make([]KeyValue, 0)
	file_pattern := fmt.Sprintf("%s/mr-*-%v.txt", TmpMapFilePath, NReduce)
	files, err := filepath.Glob(file_pattern)
	if err != nil {
		log.Println("collectIntermediate.filepath.Glob err = ", err)
		return nil, err
	}

	for _, file_path := range files {
		file, err := os.Open(file_path)
		if err != nil {
			log.Println("collectIntermediateos.Open err = ", err)
			return nil, err
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

	return intermediate, nil
}

func writeReduceResToTmpFile(res []KeyValue, WorkerId int) (string, error) {
	tempFile, err := os.CreateTemp(TmpReduceFilePath, "mr-")
	if err != nil {
		log.Println("writeReduceResToTmpFile.os.CreateTemp err = ", err)
		return "", err
	}

	// write key-val pair into tmp file
	for _, kv := range res {
		fmt.Fprintf(tempFile, "%s %s\n", kv.Key, kv.Value)
	}

	temp_name := TmpReduceFilePath + "mr-out-" + strconv.Itoa(WorkerId) + ".txt"
	err = os.Rename(tempFile.Name(), temp_name)
	if err != nil {
		log.Println("writeReduceResToTmpFile.os.Rename err = ", err)
		return "", err
	}

	return temp_name, nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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
