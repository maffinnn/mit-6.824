package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const TASKTIMEOUT = 10000

type Master struct {
	// Your definitions here.
	nReduce     int
	nMap        int
	reduceTasks []Task
	mapTasks    []Task
	mu          sync.Mutex
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

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.nMap == 0 && m.nReduce == 0
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nMap = len(files)
	m.nReduce = nReduce
	m.mapTasks = make([]Task, 0, m.nMap)
	m.reduceTasks = make([]Task, 0, m.nReduce)

	for i := 0; i < m.nMap; i++ {
		m.mapTasks[i] = Task{MAP, IDLE, files[i], i, nReduce}
	}

	for i := 0; i < m.nReduce; i++ {
		m.reduceTasks[i] = Task{REDUCE, IDLE, "", -1, nReduce}
	}

	m.server()
	return &m
}

func (m *Master) schedule(taskList []Task) *Task {
	var task *Task
	m.mu.Lock()
	n := len(taskList)
	for i := 0; i < n; i++ {
		if taskList[i].Status == IDLE {
			m.mapTasks[i].Status = IN_PROGRESS
			task = &m.mapTasks[i]
			break
		}
	}

	m.mu.Unlock()
	return task
}

func (m *Master) checkTimeOut(task *Task) {
	if (*task).Type != MAP && (*task).Type != REDUCE {
		return
	}

	<-time.After(TASKTIMEOUT * time.Millisecond)

	m.mu.Lock()
	defer m.mu.Unlock()

	if (*task).Status == IN_PROGRESS {
		(*task).Status = IDLE
	}

}
