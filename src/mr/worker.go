package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

const TIMEWAIT = 100

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// continually request for task
	for {
		args := GetArgs{}
		reply := GetReply{}
		ok := call("Master.GetTask", &args, &reply)
		if !ok {
			log.Println("RPC call GetTask error")
			continue
		}

		t := reply.T
		switch t.Type {
		case MAP:
			doMap(mapf, &t)
			break
		case REDUCE:
			{
				doReduce(reducef, &t)

				args := ReportArgs{}
				reply := ReportReply{}
				ok := call("mr.PutTask", &args, &reply)
				if !ok {
					log.Fatalf("RPC call PutTask error")
					// TODO:
					continue
				}
				break
			}
		case WAIT:
			time.Sleep(TIMEWAIT * time.Millisecond)
			break
		case DONE:
			return
		default:
			fmt.Printf("Invalid task type %v", t.Type)
			return
		}

	}

}

// 1. apply map function
// 2. periodically write to disk
// 3. inform master where the completed intermediate value
func doMap(mapf func(string, string) []KeyValue, t *Task) {

	// read ininput files
	intermediate := make([][]KeyValue, t.nReduce)
	file, err := os.Open(t.File)
	if err != nil {
		log.Fatalf("cannot open %v", t.File)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("read inputfile %v failed", t.File)
	}
	file.Close()

	// apply map function
	kva := mapf(t.File, string(content))

	for _, kv := range kva {
		Rindex := ihash(kv.Key) % (*t).nReduce
		intermediate[Rindex] = append(intermediate[Rindex], kv)
	}

	// flush to local disk
	for Rindex := 0; Rindex < (*t).nReduce; Rindex++ {
		ofile, _ := ioutil.TempFile("../mr/mr-tmp", "mr-tmp-*")
		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate[Rindex] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("flush error")
			}
		}
		ofile.Close()
		// atomic write
		oldpath := fmt.Sprintf("../mr/mr-tmp/%s", ofile.Name())
		fmt.Printf("temp file oldpath %v\n", oldpath)
		newpath := fmt.Sprintf("../mr/mr-%d/mr-%d-%d", Rindex, t.TaskId, Rindex)
		os.Rename(oldpath, newpath)

		(*t).Status = COMPLETED
		(*t).File = newpath
		fmt.Printf("Task no %d is done", (*t).TaskId)
		args := ReportArgs{*t}
		reply := ReportReply{}
		ok := call("Master.ReportTask", args, reply)
		if !ok {
			log.Fatalf("RPC call Report error")
			// TODO:
			return
		}
	}
}

func doReduce(reducef func(string, []string) string, t *Task) {

	path := fmt.Sprintf("../mr/mr-%d", (*t).TaskId)
	intermediate := read(path)

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", (*t).TaskId)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		// group similar values
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

}

func read(path string) []KeyValue {

	kva := []KeyValue{}
	files, err := os.ReadDir(path)
	if err != nil {
		log.Fatalf("Read directory error %v", err)
	}

	for _, file := range files {
		f, err := os.Open(fmt.Sprintf("%s/%s", path, file.Name()))
		if err != nil {
			log.Fatalf("intermeidate file open error %v", err)
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	return kva
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock() // fake master socket
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
