package mr

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu            sync.Mutex
	phase         AssignPhase // Map phase or Reduce phase
	nMap          int
	nReduce       int
	workerCounter int
	waiting       []Task
	running       []Task
	finished      []Task
	workerState   map[int]bool // Worker is crashed or not
	allTasksDone  bool         // Exit flag

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskRequest(args *TaskReq, reply *TaskResp) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == MapPhase && len(c.running) == 0 && len(c.waiting) == 0 && len(c.finished) == c.nMap {
		log.Println("Map phase done")
		c.phase = ReducePhase
		//clear finished tasks
		c.finished = c.finished[:0]

		for i := 1; i <= c.nReduce; i++ {
			task := Task{
				TaskID:   i,
				TaskType: Reduce,
				WorkerID: -1,
				NReduce:  i,
			}
			c.waiting = append(c.waiting, task)
		}
	} else if c.phase == ReducePhase && len(c.running) == 0 && len(c.waiting) == 0 && len(c.finished) == c.nReduce {
		c.allTasksDone = true
		reply.AllTasksDone = true
		return nil
	} else if c.allTasksDone {
		reply.AllTasksDone = true
		return nil
	}

	reply.AllTasksDone = false
	log.Printf("waiting = %d, running = %d, finished = %d\n", len(c.waiting), len(c.running), len(c.finished))
	if args.WorkerID == -1 {
		// new worker

		reply.WorkerID = c.workerCounter
		c.workerCounter++

		if len(c.waiting) > 0 {
			task := c.waiting[0]
			task.WorkerID = reply.WorkerID
			task.StartTime = time.Now()
			c.running = append(c.running, task)
			c.waiting = c.waiting[1:]

			reply.HaveNewTask = true
			reply.NewTask = task
		} else {
			reply.HaveNewTask = false
		}
	} else if args.HaveFinishedTask {
		// worker has finished task
		if args.FinishedTask.TaskType == Map {
			err := c.genMapFinalFiles(args.TemporaryFiles)
			if err != nil {
				log.Println("TaskRequest.genMapFinalFiles err = ", err)
				return err
			}
		} else if args.FinishedTask.TaskType == Reduce {
			err := c.genReduceFinalFile(args.TemporaryFiles[0])
			if err != nil {
				log.Println("TaskRequest.genReduceFinalFile err = ", err)
				return err
			}
		}

		for i, task := range c.running {
			if task.WorkerID == args.WorkerID && task.TaskID == args.FinishedTask.TaskID {
				c.running = append(c.running[:i], c.running[i+1:]...)
				c.finished = append(c.finished, task)
				break
			}
		}

		if len(c.waiting) > 0 {
			task := c.waiting[0]
			task.WorkerID = args.WorkerID
			task.StartTime = time.Now()
			c.running = append(c.running, task)
			c.waiting = c.waiting[1:]

			reply.HaveNewTask = true
			reply.NewTask = task
			reply.WorkerID = args.WorkerID
		} else {
			reply.HaveNewTask = false
		}

	} else {
		// worker is crashed
		c.workerState[args.WorkerID] = true
		for i, task := range c.running {
			if task.WorkerID == args.WorkerID {
				c.waiting = append(c.waiting, task)
				c.running = append(c.running[:i], c.running[i+1:]...)
			}
		}
		reply.HaveNewTask = false
	}

	return nil
}

func (c *Coordinator) genMapFinalFiles(files []string) error {
	for _, file := range files {
		tmp_file, err := os.Open(file)
		if err != nil {
			log.Println("genMapFinalFiles err = ", err)
			return err
		}
		defer tmp_file.Close()
		tmp_file_name := filepath.Base(file)
		final_file_path := FinalMapFilePath + tmp_file_name
		final_file, err := os.Create(final_file_path)
		if err != nil {
			log.Println("genMapFinalFiles.os.Create err = ", err)
			return err
		}
		defer final_file.Close()
		_, err = io.Copy(final_file, tmp_file)
		if err != nil {
			log.Println("genMapFinalFiles.io.Copy err = ", err)
			return err
		}
	}
	return nil
}

func (c *Coordinator) genReduceFinalFile(file string) error {
	tmp_file, err := os.Open(file)
	if err != nil {
		log.Println("genReduceFinalFile.os.Open err = ", err)
		return err
	}
	defer tmp_file.Close()
	tmp_file_name := filepath.Base(file)
	final_file_path := FinalReduceFilePath + tmp_file_name
	final_file, err := os.Create(final_file_path)
	if err != nil {
		log.Println("genReduceFinalFile.os.Create err = ", err)
		return err
	}
	defer final_file.Close()
	_, err = io.Copy(final_file, tmp_file)
	if err != nil {
		log.Println("genReduceFinalFile.os.Copy err = ", err)
		return err
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	ret := false

	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.allTasksDone {
		ret = true
	}

	return ret
}
func (c *Coordinator) periodicallyRmExpTasks() {
	for !c.Done() {
		time.Sleep(time.Second)
		c.mu.Lock()
		// TODO
		c.mu.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:         MapPhase,
		nMap:          int(len(files)),
		nReduce:       int(nReduce),
		workerCounter: 0,
		waiting:       make([]Task, 0),
		running:       make([]Task, 0),
		finished:      make([]Task, 0),
		workerState:   make(map[int]bool),
		allTasksDone:  false,
	}

	// Your code here.
	// alloc map tasks
	c.mu.Lock()

	for file := range files {
		task := Task{
			TaskID:   file,
			TaskType: Map,
			Filename: files[file],
			WorkerID: -1,
			NReduce:  file%nReduce + 1,
		}
		c.waiting = append(c.waiting, task)
	}
	c.mu.Unlock()

	// TODO: 超时
	go c.periodicallyRmExpTasks()

	c.server()
	return &c
}
