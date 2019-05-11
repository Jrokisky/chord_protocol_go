package chordNode

import (
    "chord/utils"
	"fmt"

	"github.com/Jeffail/gabs"

	zmq "github.com/pebbe/zmq4"
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
func New(address string, port int) ChordNode {
    id := utils.ComputeId(fmt.Sprintf("tcp://%s:%d", address, port))
	n := ChordNode{
		ID:      id,
		Address: address,
		Port:    port}
	n.Table = fingerTable{Size: 32}
	return n
}

// Respond to an instruction to join a chord ring
func (n ChordNode) JoinRing(msg *gabs.Container) {
	fmt.Printf("Command received to join ring %s")
}

func (n ChordNode) LeaveRing(msg *gabs.Container) {

}

func (n ChordNode) InitRingFingers(msg *gabs.Container) {

}

func (n ChordNode) FixRingFingers(msg *gabs.Container) {

}

func (n ChordNode) StabilizeRing(msg *gabs.Container) {

}

func (n ChordNode) RingNotify(msg *gabs.Container) {

}

func (n ChordNode) GetRingFingers(msg *gabs.Container) {

}

func (n ChordNode) FindRingSuccessor(msg *gabs.Container) {

}

func (n ChordNode) FindRingPredecessor(msg *gabs.Container) {

}

func (n ChordNode) Put(msg *gabs.Container) {

}

func (n ChordNode) Get(msg *gabs.Container) {

}

func (n ChordNode) Remove(msg *gabs.Container) {

}

func (n ChordNode) ListItems(msg *gabs.Container) {

}

func (n ChordNode) ProcessIncomingCommand(command string, msg *gabs.Container) string {
	switch command {
	case "join-ring":
		n.JoinRing(msg)
	case "init-ring-fingers":
		n.InitRingFingers(msg)
	case "fix-ring-fingers":
		n.FixRingFingers(msg)
	case "stabilize-ring":
		n.StabilizeRing(msg)
	case "leave-ring":
		n.LeaveRing(msg)
	case "ring-notify":
		n.RingNotify(msg)
	case "get-ring-fingers":
		n.GetRingFingers(msg)
	case "find-ring-successor":
		n.FindRingSuccessor(msg)
	case "find-ring-predecessor":
		n.FindRingPredecessor(msg)
	case "put":
		n.Put(msg)
	case "get":
		n.Get(msg)
	case "remove":
		n.Remove(msg)
	case "list-items":
		n.ListItems(msg)
	}
	return "foo"
}

func (n ChordNode) Run() {

	context, _ := zmq.NewContext()
	defer context.Term()

	socket, _ := context.NewSocket(zmq.REP)
	defer socket.Close()

	socket.Connect(fmt.Sprintf("tcp://%s:%d", n.Address, n.Port))
	fmt.Printf("Client bound to port %d\n", n.Port)

	// Main loop, listening for commands
	for true {
		msg, _ := socket.Recv(0)
		jsonParsed, _ := gabs.ParseJSON([]byte(msg))
		fmt.Println(jsonParsed.StringIndent("", "  "))
		command := jsonParsed.Path("do").String()
		reply := n.ProcessIncomingCommand(command, jsonParsed)
		socket.Send(reply, 0)
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
