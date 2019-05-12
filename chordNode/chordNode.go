package chordnode

import (
	"chord/utils"
	"fmt"
	"strconv"
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
	if strings.Compare(mode, "orderly") == 0 {
		// notify predecessor and successor
		// transfer keys to its successor
		successorAddress, present := (*n.Directory)[*n.Successor]
		myAddress, _ := n.GetSocketAddress()
		if !present {
			// TODO:
			// Look in finger table // find closest alive successor
			fmt.Println("Finger table entry, find closest alive successor")
		} else {
			// This is iterative and painful - can we send it all at once?
			for k, v := range n.Data {
				putCommand := utils.PutCommand(k, v, myAddress)
				// TODO: Verify reply below?
				reply := utils.SendMessage(putCommand, successorAddress)
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

func (n ChordNode) InitRingFingers() string {
	return ""
}

func (n ChordNode) FixRingFingers() string {
	return ""
}

func (n ChordNode) StabilizeRing() string {
	return ""
}

func (n ChordNode) RingNotify() string {
	return ""
}

func (n ChordNode) GetRingFingers() string {
	return ""
}

// {"do": "find-ring-successor", "id": id, "reply-to": address}
// Will return the id of this node's successor in the ring
func (n ChordNode) FindRingSuccessor(id uint32) uint32 {
	// var id uint32
	// replyTo := msg.Path("reply-to").String()
	var result uint32
	// Check if this key should belong to this node
	if n.Predecessor != nil && id > *n.Predecessor && id < n.ID {
		//result = n.ID // Return this node's ID, since it's the successor of the given ID
	} else if id > n.ID && id < *n.Successor {
		//result = *n.Successor
	} else {
		// TODO is there a case where successor is self?
		// Make zmq call to successor
		request := utils.FindRingPredecessorCommand(id, n.Address) // ask for id with self as reply-to
		reply := utils.SendMessage(request, (*n.Directory)[*n.Successor])
		jsonParsed, _ := gabs.ParseJSON([]byte(reply))
		result = utils.ParseToUInt32(jsonParsed.Path("id").String())
	}
	return result
}

func (n ChordNode) FindRingPredecessor(id uint32) {

}

func (n ChordNode) Put(msg *gabs.Container) string {
	key := msg.Path("data").Path("key").String()
	value := msg.Path("data").Path("value").String()
	//replyTo := msg.Path("reply-to").String()
	id := utils.ComputeId(key)
	targetNode := n.FindRingSuccessor(id)
	// Should we store it on this node?
	if targetNode == n.ID {
		n.Data[key] = value
		fmt.Printf("Storing key '%s' with value '%s' at hash %d on this node, %d", key, value, id, n.ID)
	} else {
		fmt.Printf("Sending key '%s' with value '%s' to be stored on %d at hash %d", key, value, targetNode, id)
		request := utils.PutCommand(key, value, n.Address)
		reply := utils.SendMessage(request, (*n.Directory)[targetNode])
		print(reply)
	}
}

func (n ChordNode) Get(key string) string {
	return ""
}

func (n ChordNode) Remove(key string) string {
	return ""

}

func (n ChordNode) ListItems() string {
	return ""

}

func (n ChordNode) ProcessIncomingCommand(msg string) string {

	jsonParsed, _ := gabs.ParseJSON([]byte(msg))
	fmt.Println(jsonParsed.StringIndent("", "  "))
	command := jsonParsed.Path("do").String()

	if command == "create-ring" {
		n.CreateRing() // TODO: Update args
		return ""
	} else if command == "join-ring" {
		sponsoringNode := jsonParsed.Path("sponsoring-node").String()
		result := n.JoinRing(sponsoringNode)
		return result // TODO what should this be?
	} else if command == "init-ring-fingers" {
		n.InitRingFingers()
		return ""
	} else if command == "fix-ring-fingers" {
		n.FixRingFingers()
		return ""
	} else if command == "stabilize-ring" {
		n.StabilizeRing()
		return ""
	} else if command == "leave-ring" {
		mode := jsonParsed.Path("mode").String()
		n.LeaveRing(mode)
		return ""
	} else if command == "ring-notify" {
		n.RingNotify()
		return ""
	} else if command == "get-ring-fingers" {
		n.GetRingFingers()
		return ""
	} else if command == "find-ring-successor" {
		id := utils.ParseToUInt32(jsonParsed.Path("id").String())
		n.FindRingSuccessor(id)
		return ""
	} else if command == "find-ring-predecessor" {
		id := utils.ParseToUInt32(jsonParsed.Path("id").String())
		n.FindRingPredecessor(id)
		return ""
	} else if command == "put" {
		key := jsonParsed.Path("data").Path("key").String()
		value := jsonParsed.Path("data").Path("value").String()
		var result string
		n.Put(key, value)
		replyTo := jsonParsed.Path("reply-to").String()
		jsonObj := gabs.New()
		jsonObj.Set(result, "result")
		utils.SendMessage(jsonObj.String(), replyTo)
		return result
	} else if command == "get" {
		key := jsonParsed.Path("data").Path("key").String()
		result := n.Get(key)
		replyTo := jsonParsed.Path("reply-to").String()
		jsonObj := gabs.New()
		jsonObj.Set(result, "result")
		utils.SendMessage(jsonObj.String(), replyTo)
		return result
	} else if command == "remove" {
		key := jsonParsed.Path("data").Path("key").String()
		result := n.Remove(key)
		replyTo := jsonParsed.Path("reply-to").String()
		jsonObj := gabs.New()
		jsonObj.Set(result, "result")
		utils.SendMessage(jsonObj.String(), replyTo)
		return result
	} else if command == "list-items" {
		result := n.ListItems()
		replyTo := jsonParsed.Path("reply-to").String()
		jsonObj := gabs.New()
		jsonObj.Set(result, "result")
		utils.SendMessage(jsonObj.String(), replyTo)
		return result
	} else {
		return "Invalid command received"
	}
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

		reply := n.ProcessIncomingCommand(msg)
		socket.Send(reply, 0)
	}

}

func (n ChordNode) AddNodeToDirectory() {
	(*n.Directory)[n.ID] = fmt.Sprintf("tcp://%s:%d", n.Address, n.Port)
}

func GetAddress(directory map[uint32]string, id string) string {
	uid, _ := strconv.ParseUint(id, 10, 32)
	uid32 := uint32(uid)
	return directory[uid32]
}
func (n ChordNode) GetSocketAddress() (string, bool) {
	address, present := (*n.Directory)[n.ID]
	return address, present
}
