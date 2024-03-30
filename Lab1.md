# Lab 1: MapReduce

## Requirements
实现 `mr/coordinator.go`, `mr/worker.go` 和 `mr/rpc.go`，满足
   1. 一个 coordinator process 以及若干个 worker process 运行在一台机器上；
   2. 每个 worker 通过 RPC 向 coordinator 请求 task；
   3. worker 从若干个 input files 中读取输入，执行 task，将输出写入若干个 output files；
   4. coordinator 记录 worker 的执行时间，超时则分配 task 给另一个 worker。

## Analysis
### mrsequential.go
1. 通过 `loadPlugin` 函数从 `mrapps/wc.go` 中加载 Map 和 Reduce 函数，这一步在 `mrworker.go` 中实现，通过 `Worker()` 函数创建若干个 worker；
2. 读取多个输入文件，执行 Map 操作，这一步应由若干个 worker 实现；
3. 中间结果存储在一个变量中，这一步应改为：若干个 worker 将中间结果写入文件 `mr-X-Y` 中，`X` 代表 Map task 的编号，`Y` 代表 Reduce task 的编号；
4. 对中间结果进行排序，这一步应由 worker 实现；
5. 对中间结果执行 Reduce 操作，这一步应该为：`nReduce` 个 worker 读取中间结果文件，然后执行 Reduce 操作，将结果写入文件 `mr-out-Y` 中，`Y` 代表 Reduce task 的编号。

### details
#### 1. coordinator 相关
##### task 分配策略
coordiantor 在收到 worker 的请求后，需要确定分配 Map task 还是 Reduce task，原则是在所有 Map task 结束后，才可以开始 Reduce task。

因此，coordinator 需要记录当前执行阶段，初始时为 Map 阶段。可以通过维护 3 个 task list，waiting、running 和 finished。当且仅当 waiting、running 为空、finished 中全部是 Map task 时，才可以切换执行阶段为 Reduce 阶段。另外，如果收到 worker 请求时，waiting 已经为空，即没有需要分配的 task，需要在 RPC 响应中提示 worker 等待。

##### 结束
结束时应由 `Done()` 函数返回 true。判断依据是：当且仅当 在 Reduce 阶段下、waiting、running 为空、 finished 中 task 数量等于 `nReduce` 时，coordinator 应退出。

另外，coordinator 应该通过 RPC 响应统治 worker 结束。

#### 2. worker 相关
##### task 请求
worker 通过 RPC 向 coordinator 请求 task，执行完成后通过 RPC 告知 coordinator 完成。

可以通过在 RPC args 中添加一个标识变量来区分 Map task or Reduce task。

##### crash
根据要求，如果 worker 响应时间超过 10s，coordinator 将其视为 crashed。分配给该 worker 的 task 应该重新进入 waiting list，等待分配。另外，由于 worker 写入的是中间文件，因此不用考虑文件冲突问题。

##### 结束
当收到的 RPC 响应中标识了当前 task 已全部完成，可以退出。

#### 3. 其他
##### 中间结果
worker 会将中间结果写入文件 `mr-X-Y` 中。需要考虑的问题是，如果 worker crashed，需要确保这些中间结果会被丢弃。提示中的做法是先写入临时文件，确认完成后将其改名。

因此，可以由 worker 将中间结果写入临时文件，再由 coordinator 进行改名操作，因为 coordinator 可以确定一个 task 是否完成。

##### clock
 worker 的计时应该在分配 task 时开始。可以做的优化是对 running list 中的所有 task 统一计时，有超出 10s 的就进入 waiting list 等待重新分配，而不用为每个 task 单独做判断。

## Implements
在此仅展示一些 struct 的设计。

### coordinator
```go
type Coordinator struct {
	mu             sync.Mutex
	phase          bool           // Map phase or Reduce phase
   nMap           int32
	nReduce        int32
	workerCounter  int32
	waiting        []Task
	running        []Task
	finished       []Task
	workerState    map[int32]bool // Worker is crashed or not
	allTasksDone   bool           // Exit flag
}
```

### Task
```go
type Task struct {
	TaskID      int32
	TaskType    bool        // Map task or Reduce task
	Filenames   []string
	StartTime   time.Time
	WorkerID    int32
	NReduce     int32       // Reduce number
}
```

### RPC
```go
type TaskReq struct {
	WorkerID          int32
	HaveFinishedTask  bool
	FinishedTask      Task
	TemporaryFiles    []string
}

type TaskResp struct {
	WorkerID       int32
	AllTasksDone   bool
	HaveNewTask    bool
	NewTask        Task
}
```
