package main

import (
	cn "chord/chordNode"
	"chord/utils"

	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/gorilla/mux"
)

const DEBUG = true
const STABILIZE_TIME = 2000
const CHK_PREDECESSOR_TIME = 2500

// Map of Node ids to addresses
var NodeDirectory map[uint32]string

// Stores all nodes in the system.
var nodes map[uint32]*cn.ChordNode
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
	nodes = map[uint32]*cn.ChordNode{}
	router := mux.NewRouter()
	go Stabilizer()
	go CheckPredecessorLoop()
	router.HandleFunc("/visualize", VizHandler).Methods("GET")
	fs := http.FileServer(http.Dir("./chord/static"))
	router.PathPrefix("/js/").Handler(fs)
	router.PathPrefix("/css/").Handler(fs)
	router.HandleFunc("/nodes", NodeHandler).Methods("GET", "POST")
	router.HandleFunc("/nodes/{count}", MultiNodeHandler).Methods("POST")
	router.HandleFunc("/nodes/{id}/join", NodeJoinHandler).Methods("POST")
	router.HandleFunc("/nodes/{id}/ping", NodePingHandler).Methods("POST")
	router.HandleFunc("/nodes/{id}/leave/{mode}", NodeLeaveHandler).Methods("POST")
	router.HandleFunc("/nodeDirectory", NodeDirectoryHandler).Methods("GET")
	http.ListenAndServe(":8080", router)
}

func Stabilizer() {
	for {

		for i := 0; i < len(nodeIds); i++ {
			address := NodeDirectory[nodeIds[i]]
			cmd := utils.StabilizeRingCommand()
			response, err := utils.SendMessage(cmd, address)
			if err != nil {
				utils.Debug("[Stabilizer] unable to stabilize %s\n", fmt.Sprint(nodeIds[i]))
			} else {
				utils.Debug("[Stabilizer] response: %s\n", response)
			}
			time.Sleep(STABILIZE_TIME * time.Millisecond)
		}
	}
}

func CheckPredecessorLoop() {
	for {

		for i := 0; i < len(nodeIds); i++ {
			address := NodeDirectory[nodeIds[i]]
			cmd := utils.CheckPredecessorCommand()
			_, _ = utils.SendMessage(cmd, address)
			time.Sleep(CHK_PREDECESSOR_TIME * time.Millisecond)
		}
	}
}
// API ENDPOINTS
func NodeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		json.NewEncoder(w).Encode(nodes)
	} else if r.Method == "POST" {
		node := cn.GenerateRandomNode(&NodeDirectory)
		// Add node contact information to directory.
		NodeDirectory[node.ID] = node.GetOwnAddress()
		// Add node to global map of nodes.
		nodes[node.ID] = node
		nodeIds = append(nodeIds, node.ID)
		go node.Run()
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(node.ID)
	}
}

func MultiNodeHandler(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	count, _ := strconv.ParseUint(params["count"], 10, 32)
	for j := 0; j < int(count); j++ {
		node := cn.GenerateRandomNode(&NodeDirectory)
		// Add node contact information to directory.
		NodeDirectory[node.ID] = node.GetOwnAddress()
		// Add node to global map of nodes.
		nodes[node.ID] = node
		nodeIds = append(nodeIds, node.ID)
		go node.Run()
	}
	w.WriteHeader(200)
	json.NewEncoder(w).Encode("Nodes added")
}

func VizHandler(w http.ResponseWriter, r *http.Request) {
	f, err := ioutil.ReadFile("chord/static/page.html")
	if err != nil {
	json.NewEncoder(w).Encode(err)
	} else {
		w.Header().Set("Content-type", "text/html")
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
	response, _ := utils.SendMessage(cmd, address)

	w.WriteHeader(200)
	json.NewEncoder(w).Encode(response)

}

func NodePingHandler(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id, err := strconv.ParseUint(params["id"], 10, 32)
	if err != nil {
		// todo error handling
	}
	address := NodeDirectory[uint32(id)]
	var cmd string
	cmd = utils.PingCommand()
	response, _ := utils.SendMessage(cmd, address)

	w.WriteHeader(200)
	json.NewEncoder(w).Encode(response)

}
func NodeLeaveHandler(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	id, err := strconv.ParseUint(params["id"], 10, 32)
	if err != nil {
		// todo error handling
	}
	mode := params["mode"]
	address := NodeDirectory[uint32(id)]
	cmd := utils.LeaveRingCommand(mode)
	response, _ := utils.SendMessage(cmd, address)

	w.WriteHeader(200)
	json.NewEncoder(w).Encode(response)
}

func NodeDirectoryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		json.NewEncoder(w).Encode(NodeDirectory)
	}
}

