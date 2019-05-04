package main

import (
	"chord/chordNode"
	"fmt"
	"math/rand"
	"time"

	zmq "github.com/alecthomas/gozmq"
)

type nodeAddress struct {
	NodeID    uint32
	IpAddress string
	Port      string
}

type nodeDirectory struct {
	nodes []nodeAddress
}

func main() {
	// ringSize := 32 // 2^32

	// User input loop, initially no nodes
	nodes := make(map[uint32]nodeAddress)
	// User adds node
	newNode := chordNode.New(rand.Uint32())
	newNodeAddress := nodeAddress{NodeID: newNode.ID, IpAddress: "localhost", Port: "5555"}
	// Add node to directory
	nodes[newNode.ID] = newNodeAddress
	go newNode.Run()

	port := "5555"

	context, _ := zmq.NewContext()
	defer context.Close()

	server, _ := context.NewSocket(zmq.REP)
	defer server.Close()

	server.Bind("tcp://*:" + port)
	fmt.Printf("Server bound to port %s\n", port)

	// Server run loop
	/*
			message = socket.recv()
		    print "Received request: ", message
		    time.sleep(1)
			socket.send("World from port %s" % port)
	*/
	for true {
		msg, _ := server.Recv(0)
		fmt.Printf("Received %s\n", msg)
		time.Sleep(time.Second)
		reply := fmt.Sprintf(" world")
		server.Send([]byte(reply), 0)
	}

	time.Sleep(10000 * time.Millisecond)
}
