package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"
import "strconv"

const (
	TaskStatusReady      = 0
	TaskStatusQueue      = 1
	TaskStatusRunning    = 2
	TaskStatusCompleted  = 3
	TaskStatusErr		 = 4
)

const (
	MaxTaskRunTime   = time.Second * 10
	ScheduleInterval = time.Millisecond * 100
)

type Task struct {
	Id		   string
	Status     int
	WorkerID   int
	StartTime  time.Time
}

type WorkerTask struct {
	FileName	string
	ReduceId	string
	NReduce		int
	NMap		int
	Seq			int
	Type		string
}

type Master struct {
	// Your definitions here.44
	files			   []string
	nReduce			   int
	workerNum		   int
	tasks			   []Task
	mapTaskPhase       bool
	reduceTaskPhase    bool
	mutex			   sync.Mutex
	taskCh			   chan WorkerTask
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) Register(args *RegisterArgs, reply *RegisterReply) error {
	reply.WorkerID = m.workerNum
	m.workerNum++
	return nil
}

func (m *Master) ReqTask(args *ReqTaskArgs, reply *ReqTaskReply) error {
	task := <- m.taskCh
	m.RegTask(args, reply, task)
	return nil
}

func (m *Master) RegTask(args *ReqTaskArgs, reply *ReqTaskReply, task WorkerTask) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	reply.Task = task

	if !m.mapTaskPhase {
		// 分配map任务
		reply.Task.Type = "Map"
		reply.Task.FileName = m.tasks[task.Seq].Id + ".txt"
		m.tasks[task.Seq].Status = TaskStatusRunning
	} else if !m.reduceTaskPhase {
		// 分配reduce任务
		fmt.Println("你进来没？？")
		reply.Task.Type = "Reduce"
		reply.Task.ReduceId = m.tasks[task.Seq].Id
		m.tasks[task.Seq].Status = TaskStatusRunning
	} else {
		 // 通知关闭该worker
		reply.Task = WorkerTask{
			Type: "Over",
		}
	}
	m.tasks[task.Seq].WorkerID   = args.WorkerID
	m.tasks[task.Seq].StartTime = time.Now()
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var phase string
	if !m.mapTaskPhase {
		phase = "map"
	} else {
		phase = "reduce"
	}

	fmt.Printf("get report task: %+v, taskPhase: %+v\n", args, phase)
	task := &m.tasks[args.Seq]
	if args.Success && task.Status == TaskStatusRunning {
		task.Status = TaskStatusCompleted
	} else {
		task.Status = TaskStatusErr
	}
	go m.schedule()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mutex.Lock()
	defer m.mutex.Unlock()
	ret = m.reduceTaskPhase

	return ret
}

func (m *Master) schedule() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.reduceTaskPhase {
		return
	}
	allFinish := true

	for i, task := range m.tasks {
		switch task.Status {
		case TaskStatusReady:
			allFinish = false
			m.tasks[i].Status = TaskStatusQueue
			m.taskCh <- m.getTask(i)
		case TaskStatusQueue:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			if time.Now().Sub(task.StartTime) > MaxTaskRunTime {
				m.tasks[i].Status = TaskStatusErr
			}
		case TaskStatusCompleted:
		case TaskStatusErr:
			allFinish = false
			m.tasks[i].Status = TaskStatusQueue
			m.taskCh <- m.getTask(i)
		default:
			panic("Task Err!!")
		}
	}

	if allFinish {
		if !m.mapTaskPhase {
			m.mapTaskPhase = true
			fmt.Println("进入reduce阶段")
			//清空map任务
			m.tasks = []Task{}
			m.initReduceTasks()
		} else if !m.reduceTaskPhase {
			m.reduceTaskPhase = true
		} 
	}
}

func (m *Master) tickSchedule() {
	// 每隔0.1秒去调度任务
	for !m.reduceTaskPhase {
		m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

func (m *Master) initMapTasks() {
	for _, file := range m.files {
		task := Task{Id: file[:len(file)-4], Status: TaskStatusReady}
		m.tasks = append(m.tasks, task)
	}
}

func (m *Master) initReduceTasks() {
	for i := 0; i < m.nReduce; i++ {
		task := Task{Id: strconv.Itoa(i), Status: TaskStatusReady}
		m.tasks = append(m.tasks, task)
	}
}

func (m *Master) getTask(taskSeq int) WorkerTask {
	task := WorkerTask {
		FileName:   "",
		ReduceId:	"",
		NReduce:    m.nReduce,
		NMap:		len(m.files),
		Seq: 		taskSeq,
	}
	return task
}
//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	// 首先初始化两个任务数组和master状态
	m.files = files
	m.nReduce = nReduce
	m.mapTaskPhase = false
	m.reduceTaskPhase = false

	fmt.Println(len(files))
	if nReduce > len(files) {
		m.taskCh = make(chan WorkerTask, nReduce)
	} else {
		m.taskCh = make(chan WorkerTask, len(files))
	}

	m.initMapTasks()

	go m.tickSchedule()
	m.server()
	fmt.Println("master init")
	return &m
}
