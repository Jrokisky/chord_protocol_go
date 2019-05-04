package chordNode

import (
	"fmt"
	"time"

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

/*
A node capable of joining and operating a Chord ring
*/
type ChordNode struct {
	ID        uint32
	Successor uint32
	Table     fingerTable
	Address   string
	Port      int
}

/*
Returns a new ChordNode
*/
func New(id uint32, address string, port int) ChordNode {
	n := ChordNode{
		ID:      id,
		Address: address,
		Port:    port}
	n.Table = fingerTable{Size: 32}
	return n
}

func (n ChordNode) Run() {

	context, _ := zmq.NewContext()
	defer context.Close()

	socket, _ := context.NewSocket(zmq.REP)
	defer socket.Close()

	// TODO: Why is this localhost here and * on the server?
	socket.Connect(fmt.Sprintf("tcp://%s:%d", n.Address, n.Port))
	// TODO: Why socket.connect vs socket.bind?
	fmt.Printf("Client bound to port %d\n", n.Port)

	for true {
		msg, _ := socket.Recv(0)
		fmt.Printf("Node %d received '%s'\n", n.ID, msg)
		time.Sleep(time.Second)
		reply := fmt.Sprintf("Message received.")
		socket.Send([]byte(reply), 0)
	}

	/*
			for request in range(1, 10):
		    print "Sending request ", request, "..."
		    socket.send("Hello")
		    message = socket.recv()
		    print "Received reply", request, "[", message, "]"
	*/

	// for i := 0; i < 10; i++ {
	// 	msg := fmt.Sprintf("Hello %d", i)
	// 	client.Send([]byte(msg), 0)
	// 	println("Sending", msg)

	// 	reply, _ := client.Recv(0)
	// 	println("Received", string(reply))
	// }

}
