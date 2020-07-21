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
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	tempDir := "temp-mr"
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for true {
		time.Sleep(1 * time.Second)
		reply := CallRequestTask()
		if len(reply.Key) > 0 {
			if reply.IsMap {
				keys := make(map[int]bool)
				file, err := os.Open(reply.Key)
				if err != nil {
					log.Fatalf("cannot open %v", reply.Key)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.Key)
				}
				file.Close()
				kva := mapf(reply.Key, string(content))

				_ = os.Mkdir(tempDir, 0777)

				fileGlob := fmt.Sprintf("mr-%d-*", reply.Id)
				files, err := filepath.Glob(filepath.Join(tempDir, fileGlob))
				if err != nil {
					log.Fatalf("cannot glob files")
				}
				for _, f := range files {
					if err := os.Remove(f); err != nil {
						log.Fatalf("cannot remove file %s", f)
					}
				}
				for _, kv := range kva {
					numkey := ihash(kv.Key) % reply.Nreduce
					intermediateFilename := fmt.Sprintf("mr-%d-%d", reply.Id, numkey)
					//fmt.Printf("%s : %s : %s\n", kv.Key, kv.Value, intermediateFilename)
					path := filepath.Join(tempDir, intermediateFilename)
					intermediateFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755)
					if err != nil {
						log.Fatalf("cannot open %v", path)
					}
					enc := json.NewEncoder(intermediateFile)
					err = enc.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encode %v", reply.Key)
					}
					intermediateFile.Close()
					keys[numkey] = true
				}
				keysList := make([]int, 0, len(keys))
				for k := range keys {
					keysList = append(keysList, k)
				}
				_ = CallTaskFinished(reply.Id, keysList, true)
			} else {
				//fmt.Printf("Reduce task : %d \n", reply.Id)
				var intermediateReduce []KeyValue
				fileGlob := fmt.Sprintf("mr-*-%d", reply.Id)
				files, err := filepath.Glob(filepath.Join(tempDir, fileGlob))
				if err != nil {
					log.Fatalf("cannot glob files")
				}
				for _, f := range files {
					intermediateFile, err := os.Open(f)
					if err != nil {
						log.Fatalf("cannot open %v", f)
					}
					dec := json.NewDecoder(intermediateFile)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediateReduce = append(intermediateReduce, kv)
					}
				}
				sort.Sort(ByKey(intermediateReduce))

				oname := fmt.Sprintf("temp-r-%d", reply.Id)
				ofile, _ := ioutil.TempFile(tempDir, oname)
				tempName := ofile.Name()
				defer os.Remove(tempName)
				i := 0
				for i < len(intermediateReduce) {
					j := i + 1
					for j < len(intermediateReduce) && intermediateReduce[j].Key == intermediateReduce[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediateReduce[k].Value)
					}
					output := reducef(intermediateReduce[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediateReduce[i].Key, output)

					i = j
				}

				ofile.Close()
				os.Rename(tempName, oname)
				var keysList []int
				_ = CallTaskFinished(reply.Id, keysList, false)
			}
		}
	}
}

func CallRequestTask() *RequestTaskReply {

	// declare an argument structure.
	args := RequestTaskArgs{}

	// declare a reply structure.
	reply := RequestTaskReply{}

	// send the RPC request, wait for the reply.
	call("Master.RequestTask", &args, &reply)

	//fmt.Printf("reply.id %v\n", reply.Id)

	return &reply
}

func CallTaskFinished(id int, keys []int, isMap bool) *TaskFinishedReply {
	args := TaskFinishedArgs{}
	reply := TaskFinishedReply{}

	args.Id = id
	args.Keys = keys
	args.IsMap = isMap
	call("Master.NotifyComplete", &args, &reply)

	return &reply
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
