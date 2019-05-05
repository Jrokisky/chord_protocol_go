package chordNode

import (
	"encoding/json"
	"fmt"
	"time"

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
func New(id uint32, address string, port int) ChordNode {
	n := ChordNode{
		ID:      id,
		Address: address,
		Port:    port}
	n.Table = fingerTable{Size: 32}
	return n
}

// Respond to an instruction to join a chord ring
func (n ChordNode) JoinRing(msg map[string]interface{}) {

}

func (n ChordNode) LeaveRing(msg map[string]interface{}) {

}

func (n ChordNode) InitRingFingers(msg map[string]interface{}) {

}

func (n ChordNode) FixRingFingers(msg map[string]interface{}) {

}

func (n ChordNode) StabilizeRing(msg map[string]interface{}) {

}

func (n ChordNode) RingNotify(msg map[string]interface{}) {

}

func (n ChordNode) GetRingFingers(msg map[string]interface{}) {

}

func (n ChordNode) FindRingSuccessor(msg map[string]interface{}) {

}

func (n ChordNode) FindRingPredecessor(msg map[string]interface{}) {

}

func (n ChordNode) Put(msg map[string]interface{}) {

}

func (n ChordNode) Get(msg map[string]interface{}) {

}

func (n ChordNode) Remove(msg map[string]interface{}) {

}

func (n ChordNode) ListItems(msg map[string]interface{}) {

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

		// Set up dict for data to be imported into
		var msgMap map[string]interface{}
		err := json.Unmarshal([]byte(msg), &msgMap)
		if err != nil {
			panic(err)
		}

		command := msgMap["do"].(string) // type assertion
		println(command)

		// var commandMap map[string]interface{}
		// err := json.Unmarshal([]byte(command), &commandMap)

		switch command {
		case "join-ring":
			n.JoinRing(msgMap)
		case "init-ring-fingers":
			n.InitRingFingers(msgMap)
		case "fix-ring-fingers":
			n.FixRingFingers(msgMap)
		case "stabilize-ring":
			n.StabilizeRing(msgMap)
		case "leave-ring":
			n.LeaveRing(msgMap)
		case "ring-notify":
			n.RingNotify(msgMap)
		case "get-ring-fingers":
			n.GetRingFingers(msgMap)
		case "find-ring-successor":
			n.FindRingSuccessor(msgMap)
		case "find-ring-predecessor":
			n.FindRingPredecessor(msgMap)
		case "put":
			n.Put(msgMap)
		case "get":
			n.Get(msgMap)
		case "remove":
			n.Remove(msgMap)
		case "list-items":
			n.ListItems(msgMap)
		default:
			socket.Send("Invalid command received.", 0)
		}

		// fmt.Printf("Node %d received '%s'\n", n.ID, msg)
		// fmt.Println(reflect.TypeOf(msg))
		// msgString := string(msg)
		// b, err := json.MarshalIndent(&msgString, "", "\t")
		// if err != nil {
		// 	fmt.Println("error:", err)
		// }
		// os.Stdout.Write(b)
		// n.Log(l, string(messageFormatted))
		// println(string(msg))
		time.Sleep(time.Second)
		reply := "" //fmt.Sprintf("Message received.")
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
