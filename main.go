package main

import (
	cn "chord/chordNode"
	"chord/utils"

	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	zmq "github.com/pebbe/zmq4"
)

// Map of Node ids to addresses
var NodeDirectory map[uint32]string

// Stores all nodes in the system.
var nodes map[uint32]cn.ChordNode
var nodeIds []uint32

func getSponsoringNodeAddress() (string, error) {
	for _, id := range nodeIds {
		if nodes[id].InRing {
			return NodeDirectory[nodes[id].ID], nil
		}
	}
	return "", errors.New("No nodes in Ring")
}

func main() {
	NodeDirectory = map[uint32]string{}
	nodes = map[uint32]cn.ChordNode{}
	router := mux.NewRouter()
	router.HandleFunc("/visualize", VizHandler).Methods("GET")
	router.HandleFunc("/visualize/scripts.js", VizJSHandler).Methods("GET")
	router.HandleFunc("/nodes", NodeHandler).Methods("GET", "POST")
	router.HandleFunc("/nodes/{id}join", NodeJoinHandler).Methods("POST")
	router.HandleFunc("/nodeDirectory", NodeDirectoryHandler).Methods("GET")
	http.ListenAndServe(":8080", router)
}

// API ENDPOINTS
func NodeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		json.NewEncoder(w).Encode(nodes)
	} else if r.Method == "POST" {
		node := cn.GenerateRandomNode(&NodeDirectory)
		// Add node contact information to directory.
		NodeDirectory[node.ID] = fmt.Sprintf("tcp://%s:%d", node.Address, node.Port)
		// Add node to global map of nodes.
		nodes[node.ID] = node
		nodeIds = append(nodeIds, node.ID)
		go node.Run()
		w.WriteHeader(200)
		json.NewEncoder(w).Encode("success")
	}
}

// WIP
func VizHandler(w http.ResponseWriter, r *http.Request) {
	f, err := ioutil.ReadFile("chord/static/page.html")
	if err != nil {
		json.NewEncoder(w).Encode(err)
	} else {
		w.Header().Set("Content-type", "text/html")
		fmt.Fprintf(w, string(f))
	}
}

// WIP
func VizJSHandler(w http.ResponseWriter, r *http.Request) {
	f, err := ioutil.ReadFile("chord/static/scripts.js")
	if err != nil {
		json.NewEncoder(w).Encode(err)
	} else {
		w.Header().Set("Content-type", "text/javascript")
		fmt.Fprintf(w, string(f))
	}
}

func NodeJoinHandler(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id, err := strconv.ParseUint(params["id"], 10, 32)
	if err != nil {
		// todo error handling
	}
	address := NodeDirectory[uint32(id)]
	var cmd string
	sponsorNodeAddr, err := getSponsoringNodeAddress()

	// First Node.
	if err != nil {
		cmd = utils.CreateRingCommand()
	} else {
		cmd = utils.JoinRingCommand(sponsorNodeAddr)
	}
	response := SendCommand(address, cmd)

	w.WriteHeader(200)
	json.NewEncoder(w).Encode(response)

}

func NodeDirectoryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		json.NewEncoder(w).Encode(NodeDirectory)
	}
}

func SendCommand(target string, data string) string {
	context, _ := zmq.NewContext()
	defer context.Term()

	socket, _ := context.NewSocket(zmq.REQ)
	defer socket.Close()

	socket.Connect(target)
	socket.Send(data, 0)
	response, _ := socket.Recv(0)
	return response
}
