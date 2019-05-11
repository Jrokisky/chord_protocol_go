package main

import (
	cn "chord/chordNode"

	"fmt"
    "encoding/json"
    "net/http"

	"github.com/Jeffail/gabs"
    zmq "github.com/pebbe/zmq4"
    "github.com/gorilla/mux"
)

// Map of Node ids to addresses
var nodeDirectory map[uint32]string

// Stores all nodes in the system.
var nodes []cn.ChordNode


/*
Send a message over 0MQ
*/
func SendMessage(address string, port int, data string) string {
	context, _ := zmq.NewContext()
	socket, _ := context.NewSocket(zmq.REQ)
	defer socket.Close()
	socket.Bind(fmt.Sprintf("tcp://%s:%d", address, port))
	socket.Send(data, 0)

	// Wait for reply:
	reply, _ := socket.Recv(0)
	fmt.Printf("Received '%s'\n", string(reply))
	return string(reply)
}

/* Generate the JSON command to instruct the node that receives it to join the ring
at the 'address' argument. */
// func commandSendJoinRing(address string) string {
// 	command := make(map[string]interface{})
// 	command["do"] = "join-ring"
// 	jsonCommand, err := json.Marshal(command)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return string(jsonCommand)
// }

// func CommandPutItem(data string) {

// }

func JoinRingCommand(address string) *gabs.Container {
	jsonObj := gabs.New()
	jsonObj.Set("join-ring", "do")
	jsonObj.Set(address, "sponsoring-node")
	return jsonObj

}

func main() {
    nodeDirectory = make(map[uint32]string, 1000)
    router := mux.NewRouter()
	router.HandleFunc("/nodes", NodeHandler).Methods("GET", "POST")
	router.HandleFunc("/nodeDirectory", NodeDirectoryHandler).Methods("GET")
	http.ListenAndServe(":8080", router)
}

func NodeHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method == "GET" {
       json.NewEncoder(w).Encode(nodes)
    } else if r.Method == "POST" {
        node := cn.GenerateRandomNode()
        // Add node contact information to directory.
        nodeDirectory[node.ID] = fmt.Sprintf("tcp://%s:%d", node.Address, node.Port)
        // Add node to global list of nodes.
        nodes = append(nodes, node)
        w.WriteHeader(200)
        json.NewEncoder(w).Encode("success")
    }
}


func NodeDirectoryHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method == "GET" {
        json.NewEncoder(w).Encode(nodeDirectory)
    }
}
