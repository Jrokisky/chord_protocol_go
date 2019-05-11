package chordnode

import (
	"chord/utils"
	"strconv"

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
	ID          uint32
	Predecessor *uint32
	Successor   *uint32
	Table       fingerTable
	Address     string
	Port        int
	InRing      bool
	Data        map[string]string
	Directory   *map[uint32]string
}

/*
Returns a new ChordNode
*/
func New(address string, port int, directory *map[uint32]string) ChordNode {
	id := utils.ComputeId(fmt.Sprintf("tcp://%s:%d", address, port))
	n := ChordNode{
		ID:      id,
		Address: address,
		Port:    port}
	n.Table = fingerTable{Size: 32}
	n.Data = make(map[string]string)
	n.Directory = directory
	return n
}

/**
 * Try to find an open port.
 */
func GenerateRandomNode(directory *map[uint32]string) ChordNode {
	context, _ := zmq.NewContext()
	defer context.Term()

	socket, _ := context.NewSocket(zmq.REP)
	defer socket.Close()

	randPort := utils.GetRandomPort()
	err := socket.Connect(fmt.Sprintf("tcp://%s:%d", utils.Localhost, randPort))

	// Error while connecting. Get new port
	for err != nil {
		randPort = utils.GetRandomPort()
		err = socket.Connect(fmt.Sprintf("tcp://%s:%d", utils.Localhost, randPort))
	}
	return New(utils.Localhost, randPort, directory)
}

func (n ChordNode) Print() {
	fmt.Printf("%+v\n", n)
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

// {"do": "find-ring-successor", "id": id, "reply-to": address}
func (n ChordNode) FindRingSuccessor(msg *gabs.Container) *gabs.Container {
	var id uint32
	id64, err := strconv.ParseUint(msg.Path("id").String(), 10, 32)
	id = uint32(id64) // TODO: This is stupid
	replyTo := msg.Path("reply-to").String()
	var result uint32
	// Check if this key should belong to this node
	if n.Predecessor != nil && id > *n.Predecessor && id < n.ID {
		result = n.ID // Return this node's ID, since it's the successor of the given ID
	} else if id > n.ID && id < *n.Successor {
		result = *n.Successor
	} else {
		// TODO is there a case where successor is self?
		// Make zmq call to successor
		request := utils.FindRingPredecessorCommand(id, n.Address).String()
		directory := *n.Directory
		successor := *n.Successor
		reply := utils.SendMessage(request, directory[successor]) // TODO This is BS why can't I nest them
		result = jsonParsed.Path("id").String()
	}
	jsonParsed, _ := gabs.ParseJSON([]byte(reply))
	jsonObj := gabs.New()
	jsonObj.Set(id, "id")
	return jsonobj
}

func (n ChordNode) FindRingPredecessor(msg *gabs.Container) {

}

func (n ChordNode) Put(msg *gabs.Container) {
	key := msg.Path("data").Path("key").String()
	value := msg.Path("data").Path("value").String()
	replyTo := msg.Path("reply-to").String()
	id := utils.ComputeId(key)
	fmt.Printf("Storing key '%s' with value '%s' at hash %d", key, value, id)
	return key
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
