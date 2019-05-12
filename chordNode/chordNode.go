package chordnode

import (
	"chord/utils"
	"strconv"
	"fmt"
	"strings"
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

func (n ChordNode) CreateRing(msg *gabs.Container) string {
	n.InRing = true
	jsonObj := gabs.New()
	jsonObj.Set("ok", "status")
	msg.Merge(jsonObj)

	return msg.String()
}

// Respond to an instruction to join a chord ring
func (n ChordNode) JoinRing(msg *gabs.Container) string {
	n.InRing = true
	jsonObj := gabs.New()
	jsonObj.Set("ok", "status")
	msg.Merge(jsonObj)

	return msg.String()
}

func (n ChordNode) LeaveRing(msg *gabs.Container) string {
	mode := msg.Path("mode").String()

	// Leave gracefully and inform others
	if (strings.Compare(mode, "orderly") == 0) {
		// notify predecessor and successor
		// transfer keys to its successor
		successorAddress, present := (*n.Directory)[*n.Successor]
		myAddress, _ := n.GetSocketAddress()
		if (!present) {
			// TODO:
			// Look in finger table // find closest alive successor
			fmt.Println("Finger table entry, find closest alive successor")
		} else {
			// This is iterative and painful - can we send it all at once?
			for k, v := range n.Data {
				putCommand := utils.PutCommand(k, v, myAddress)
				// TODO: Verify reply below?
				reply := utils.SendMessage(putCommand.String(), successorAddress)
				fmt.Println("k:%s v:%s reply:%s", k, v, reply)
			}
		}

		// TODO: How do we communicate node updates like this? stabilization handles this?
		// predecessor removes n from successor list
		// add last node in n's successor list to predecessor's list
		// successor will replace predecessor with n's predecessor

	}

	// Case for immediate: Just leave the party
	// remove ourselves from the directory?
	delete(*n.Directory, n.ID)
	n.InRing = false

	jsonObj := gabs.New()
	jsonObj.Set("ok", "status")
	msg.Merge(jsonObj)

	return msg.String()
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
	id64, _ := strconv.ParseUint(msg.Path("id").String(), 10, 32)
	id = uint32(id64) // TODO: This is stupid
	//replyTo := msg.Path("reply-to").String()
	//var result uint32
	// Check if this key should belong to this node
	if n.Predecessor != nil && id > *n.Predecessor && id < n.ID {
		//result = n.ID // Return this node's ID, since it's the successor of the given ID
	} else if id > n.ID && id < *n.Successor {
		//result = *n.Successor
	} else {
		// TODO is there a case where successor is self?
		// Make zmq call to successor
		//request := utils.FindRingPredecessorCommand(id, n.Address).String()
		//directory := *n.Directory
		//successor := *n.Successor
		//reply := utils.SendMessage(request, directory[successor]) // TODO This is BS why can't I nest them
		//result = jsonParsed.Path("id").String()
	}
	//jsonParsed, _ := gabs.ParseJSON([]byte(reply))
	jsonObj := gabs.New()
	jsonObj.Set(id, "id")
	return jsonObj
}

func (n ChordNode) FindRingPredecessor(msg *gabs.Container) {

}

func (n ChordNode) Put(msg *gabs.Container) string {
	key := msg.Path("data").Path("key").String()
	value := msg.Path("data").Path("value").String()
	//replyTo := msg.Path("reply-to").String()
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
	cmd, _ := strconv.Unquote(command)
	switch cmd {
	case "create-ring":
		return n.CreateRing(msg)
	case "join-ring":
		return n.JoinRing(msg)
	case "init-ring-fingers":
		n.InitRingFingers(msg)
	case "fix-ring-fingers":
		n.FixRingFingers(msg)
	case "stabilize-ring":
		n.StabilizeRing(msg)
	case "leave-ring":
		return n.LeaveRing(msg)
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
	default:
		return "default"
	}

	return "fail"
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

}

func (n ChordNode) AddNodeToDirectory() {
	(*n.Directory)[n.ID] = fmt.Sprintf("tcp://%s:%d", n.Address, n.Port)
}

func (n ChordNode) GetSocketAddress() (string, bool) {
	address, present := (*n.Directory)[n.ID]
	return address, present
}