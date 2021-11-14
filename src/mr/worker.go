package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "strings"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	id		  int
	mapf      func(string, string) []KeyValue
	reducef   func(string, []string) string
	isAlive   bool
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func ReduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := worker{mapf: mapf, reducef: reducef, isAlive: true}
	worker.CallRegister()
	worker.run()

	// uncomment to send the Example RPC to the master.
	//CallExample()
}

func (worker *worker) run() {
	for {
		fmt.Println("开始请求")
		worker.CallReqTask()
		if(!worker.isAlive) {
			fmt.Printf("Worker %d has dead, exit...\n", worker.id)
			break
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func (worker *worker) CallRegister() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	if ok := call("Master.Register", &args, &reply); !ok {
		log.Fatal("register fail")
	}
	worker.id = reply.WorkerID
}

func (worker *worker) CallReqTask() {
	args := ReqTaskArgs{}
	reply := ReqTaskReply{}
	call("Master.ReqTask", &args, &reply)
	if reply.Task.Type == "Map" {
		worker.doMapTask(reply.Task)
	} else if reply.Task.Type == "Reduce" {
		worker.doReduceTask(reply.Task)
	} else if reply.Task.Type == "Over" {
		worker.isAlive = false
	}
}

func (worker *worker) CallReportTask(task WorkerTask, success bool, err error) {
	args := ReportTaskArgs{}
	args.WorkerID = worker.id
	args.Seq = task.Seq
	args.Success = success
	reply := ReportTaskReply{}

	if ok := call("Master.ReportTask", &args, &reply); !ok {
		fmt.Printf("report task fail:%+v\n", args)
	}
}

func (worker *worker) doMapTask(task WorkerTask) {
	contents, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		worker.CallReportTask(task, false, err)
		return
	}
	kvs := worker.mapf(task.FileName, string(contents))

	reduces := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % task.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {
		fileName := ReduceName(task.Seq, idx)
		f, _ := os.Create(fileName)

		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				worker.CallReportTask(task, false, err)
			}
		}
		f.Close()
	}
	worker.CallReportTask(task, true, nil)
}

func (worker *worker) doReduceTask(task WorkerTask) {
	maps := make(map[string][]string)
	fmt.Println("开始reduce处理")
	for idx := 0; idx < task.NMap; idx++ {
		fileName := ReduceName(idx, task.Seq)
		fmt.Println("读取文件：", fileName)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
			worker.CallReportTask(task, false, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
		file.Close()
	}
	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, worker.reducef(k, v)))
	}

	fmt.Println(mergeName(task.Seq))
	if err := ioutil.WriteFile(mergeName(task.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		worker.CallReportTask(task, false, err)
	}

	worker.CallReportTask(task, true, nil)
	
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
