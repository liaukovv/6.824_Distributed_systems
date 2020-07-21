package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// TaskStatus indicates status of the task waiting in queue for a worker
type taskStatus int

const (
	ready taskStatus = iota
	inProgress
	finished
)

type mapTask struct {
	fileName       string
	status         taskStatus
	expirationTime time.Time
}

type reduceTask struct {
	key            int
	status         taskStatus
	expirationTime time.Time
}

var masterMutex sync.Mutex

type Master struct {
	// Your definitions here.
	mapTasks         []mapTask
	reduceTasks      []reduceTask
	mappingComplete  bool
	reducingComplete bool
	nReduce          int
	keys             map[int]bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RequestTask RPC handler
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	masterMutex.Lock()
	defer masterMutex.Unlock()
	if !m.mappingComplete {
		for i, mt := range m.mapTasks {
			if mt.status == ready {
				reply.Id = i
				reply.IsMap = true
				reply.Key = mt.fileName
				reply.Nreduce = m.nReduce
				m.mapTasks[i].status = inProgress
				m.mapTasks[i].expirationTime = time.Now().Add(time.Duration(10) * time.Second)
				return nil
			}
		}
	} else {
		for i, rt := range m.reduceTasks {
			//fmt.Println("Checking task ", rt.key, rt.status)
			if rt.status == ready {
				reply.Id = rt.key
				reply.IsMap = false
				reply.Nreduce = m.nReduce
				reply.Key = "reduce"
				m.reduceTasks[i].status = inProgress
				m.reduceTasks[i].expirationTime = time.Now().Add(time.Duration(10) * time.Second)
				return nil
			}
		}
	}

	return nil
}

// NotifyComplete RPC handler
func (m *Master) NotifyComplete(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	masterMutex.Lock()
	defer masterMutex.Unlock()
	if args.IsMap {
		m.mapTasks[args.Id].status = finished
		for _, k := range args.Keys {
			m.keys[k] = true
		}
	} else {
		for i := range m.reduceTasks {
			if m.reduceTasks[i].key == args.Id {
				m.reduceTasks[i].status = finished
			}
		}
	}
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
	masterMutex.Lock()
	defer masterMutex.Unlock()
	ret := false
	mapping := false
	reducing := false
	// Your code here.
	if !m.mappingComplete {
		for i, mt := range m.mapTasks {
			if mt.status == ready {
				mapping = true
			} else if mt.status == inProgress {
				if time.Now().After(mt.expirationTime) {
					m.mapTasks[i].status = ready
				}
				mapping = true
			}
		}
		if mapping {
			return ret
		}
		for k := range m.keys {
			t := reduceTask{key: k, status: ready}
			m.reduceTasks = append(m.reduceTasks, t)

		}
		m.mappingComplete = true
	} else if !m.reducingComplete {
		for i, rt := range m.reduceTasks {
			if rt.status == ready {
				reducing = true
			} else if rt.status == inProgress {
				if time.Now().After(rt.expirationTime) {
					m.reduceTasks[i].status = ready
				}
				reducing = true
			}
		}
		if reducing {
			return ret
		}
		m.reducingComplete = true
	}

	if m.reducingComplete && m.mappingComplete {
		out, err := os.OpenFile("mr-out-0", os.O_CREATE|os.O_WRONLY, 0755)
		if err != nil {
			log.Fatalln("failed to open output file:", err)
		}
		defer out.Close()
		for k := range m.keys {
			file := fmt.Sprintf("temp-r-%d", k)
			fmt.Println(file)
			intermediateFile, err := os.Open(file)
			if err != nil {
				log.Fatalf("cannot open %v", file)
			}
			_, err = io.Copy(out, intermediateFile)
			if err != nil {
				log.Fatalln("failed to append file to output:", err)
			}
			intermediateFile.Close()
		}

		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{keys: make(map[int]bool)}
	for _, file := range files {
		//fmt.Println(file)
		t := mapTask{fileName: file, status: ready}
		m.mapTasks = append(m.mapTasks, t)
	}
	m.nReduce = nReduce

	// Your code here.

	m.server()
	return &m
}
