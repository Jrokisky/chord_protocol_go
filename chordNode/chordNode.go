package chordNode

import (
	"fmt"

	zmq "github.com/alecthomas/gozmq"
)

type fingerTableEntry struct {
	Key       uint32
	Successor uint32
}
type fingerTable struct {
	Entries []fingerTableEntry
	Size    int
}
type chordNode struct {
	ID        uint32
	Successor uint32
	Table     fingerTable
}

func New(id uint32) chordNode {
	n := chordNode{ID: id}
	n.Table = fingerTable{Size: 32}
	return n
}

func (n chordNode) Run() {

	port := "5555"

	context, _ := zmq.NewContext()
	defer context.Close()

	client, _ := context.NewSocket(zmq.REQ)
	defer client.Close()

	// TODO: Why is this localhost here and * on the server?
	client.Connect("tcp://localhost:" + port)
	// TODO: Why socket.connect vs socket.bind?
	fmt.Printf("Client bound to port %s\n", port)

	/*
			for request in range(1, 10):
		    print "Sending request ", request, "..."
		    socket.send("Hello")
		    message = socket.recv()
		    print "Received reply", request, "[", message, "]"
	*/

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("Hello %d", i)
		client.Send([]byte(msg), 0)
		println("Sending", msg)

		reply, _ := client.Recv(0)
		println("Received", string(reply))
	}
}
