package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type TaskType int
type TaskStatus int
type Task struct {
	Type    TaskType
	Status  TaskStatus // state of the task {IDLE, IN_PROGRESS, COMPLETE}
	File    string
	TaskId  int
	nReduce int
}

// tasktype enum
const (
	MAP TaskType = iota
	REDUCE
	WAIT
	DONE
)

// task state enum
const (
	IDLE TaskStatus = iota
	IN_PROGRESS
	COMPLETED
)

// (worker) request the input files
type GetArgs struct {
	// id int // id of the worker machine
}

type GetReply struct {
	T Task
}

// to write to the intermediate []file
type ReportArgs struct {
	T Task
}

type ReportReply struct {
	Err error
}

// Add your RPC definitions here.
func (m *Master) GetTask(args *GetArgs, reply *GetReply) error {
	var task *Task
	if m.nMap > 0 {
		task = m.schedule(m.mapTasks)
	} else if m.nReduce > 0 {
		task = m.schedule(m.reduceTasks)
	} else if m.nMap == 0 && m.nReduce == 0 {
		task = &Task{DONE, COMPLETED, "", -1, -1}
	}

	if task == nil {
		task = &Task{WAIT, IN_PROGRESS, "", -1, -1}
	}

	(*reply).T = *task
	go m.checkTimeOut(task)

	return nil
}

func (m *Master) ReportTask(args *ReportArgs, reply *ReportReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := args.T
	if task.Type == MAP && task.Status == COMPLETED {

		mt := &m.mapTasks[task.TaskId]
		(*mt).Status = COMPLETED
		filename := strings.Split((*mt).File, "-")
		Rindex, _ := strconv.Atoi(filename[len(filename)-1])
		rt := &m.reduceTasks[Rindex]
		(*rt).File = task.File
		(*rt).TaskId = Rindex
		m.nMap--

	} else if task.Type == REDUCE && task.Status == COMPLETED {

		t := &m.reduceTasks[task.TaskId]
		(*t).Status = COMPLETED
		m.reduceTasks[(*t).TaskId], m.reduceTasks[m.nReduce-1] = m.reduceTasks[m.nReduce-1], m.reduceTasks[(*t).TaskId]
		m.nReduce--

	} else {
		msg := fmt.Sprintf("Invalid Task to Report %d", task.Type)
		return errors.New(msg)
	}

	return nil
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
