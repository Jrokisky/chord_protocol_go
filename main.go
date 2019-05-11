package main

import (
	cn "chord/chordNode"

	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

// Map of Node ids to addresses
var nodeDirectory map[uint32]string

// Stores all nodes in the system.
var nodes []cn.ChordNode

func main() {
	nodeDirectory = map[uint32]string{}
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
