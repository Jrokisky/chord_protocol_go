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
func New(address string, port int, directory *map[uint32]string) *ChordNode {
	id := utils.ComputeId(fmt.Sprintf("tcp://%s:%d", address, port))
	n := ChordNode{
		ID:      id,
		Address: address,
		Port:    port}
	n.Predecessor = new(uint32)
	n.Successor = new(uint32)
	n.Table = fingerTable{Size: 32}
	n.Data = make(map[string]string)
	n.Directory = directory
	n.InRing = false
	return &n
}

/**
 * Try to find an open port.
 */
func GenerateRandomNode(directory *map[uint32]string) *ChordNode {
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
func (n *ChordNode) CreateRing(msg *gabs.Container) string {
	n.Predecessor = nil
	n.Successor = new(uint32)
	*(n.Successor) = n.ID
	n.InRing = true
	jsonObj := gabs.New()
	jsonObj.Set("ok", "status")
	msg.Merge(jsonObj)

	return msg.String()
}

// Respond to an instruction to join a chord ring
func (n *ChordNode) JoinRing(msg *gabs.Container) string {
	utils.Debug("[JoinRing] msg: %s\n", msg.String())
	n.Predecessor = nil
	sponsorAddress := msg.Path("sponsoring-node").Data().(string)
	newmsg := utils.FindRingSuccessorCommand(n.ID, n.GetOwnAddress())
	response_from_sponsor := utils.SendMessage(newmsg, sponsorAddress)
	jsonParsed, _ := gabs.ParseJSON([]byte(response_from_sponsor))
	id := utils.ParseToUInt32(jsonParsed.Path("id").String())
	*(n.Successor) = id
	n.InRing = true
	jsonObj := gabs.New()
	jsonObj.Set("ok", "status")
	msg.Merge(jsonObj)

	return msg.String()

}

func (n *ChordNode) GetOwnAddress() string {
	return fmt.Sprintf("tcp://%s:%d", n.Address, n.Port)
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
func (n *ChordNode) FindRingSuccessor(id uint32) uint32 {
	var result uint32
	// Special case when adding second node to ring
	if n.Predecessor == nil && *(n.Successor) == n.ID {
		utils.Debug("\t[FindRingSuccessor] Adding second node to ring\n")
		// First node in the Chord, so now there's only two nodes in chord.
		n.Predecessor = new(uint32)
		*(n.Predecessor) = id
		n.Successor = new(uint32)
		*(n.Successor) = id
		result = n.ID
	} else if(utils.IsBetween(n.ID, *(n.Successor), id)) {
		result = *(n.Successor)
		*(n.Successor) = id
	} else if (n.Predecessor != nil) && (utils.IsBetween(*(n.Predecessor), n.ID, id)) {
		result = n.ID
		if (n.Predecessor == nil) {
			n.Predecessor = new(uint32)
		}
		*(n.Predecessor) = id
	} else {
		// Recursively ask successors.
		request := utils.FindRingSuccessorCommand(id, n.GetOwnAddress())
		directory := *n.Directory
		successor := *(n.Successor)
		response_from_successor := utils.SendMessage(request, directory[successor])
		jsonParsed, _ := gabs.ParseJSON([]byte(response_from_successor))
		id := utils.ParseToUInt32(jsonParsed.Path("id").String())
		result = id
	}
	return result
}

func (n ChordNode) FindRingPredecessor(id uint32) {

}

func (n ChordNode) Put(key string, value string) string {
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
	return "" //TODO: implement
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

func (n *ChordNode) ProcessIncomingCommand(msg string) string {
	jsonParsed, _ := gabs.ParseJSON([]byte(msg))
	command := jsonParsed.Path("do").Data().(string)

	switch strings.TrimSpace(command) {
	case "create-ring":
		return n.CreateRing(jsonParsed)
	case "join-ring":
		result := n.JoinRing(jsonParsed)
		return result
	case "init-ring-fingers":
		n.InitRingFingers()
		return ""
	case "fix-ring-fingers":
		n.FixRingFingers()
		return ""
	case "stabilize-ring":
		n.StabilizeRing()
		return ""
	case "leave-ring":
		// TODO: Should we pass string or json into leavring?
		//mode := jsonParsed.Path("mode").String()
		n.LeaveRing(jsonParsed)
		return ""
	case "ring-notify":
		n.RingNotify()
		return ""
	case "get-ring-fingers":
		n.GetRingFingers()
		return ""
	case "find-ring-successor":
		id := utils.ParseToUInt32(jsonParsed.Path("id").String())
		result := n.FindRingSuccessor(id)
		jsonObj := gabs.New()
		jsonObj.Set(result, "id")
		return jsonObj.String()
	case "find-ring-predecessor":
		id := utils.ParseToUInt32(jsonParsed.Path("id").String())
		n.FindRingPredecessor(id)
		return ""
	case "put":
		key := jsonParsed.Path("data").Path("key").Data().(string)
		value := jsonParsed.Path("data").Path("value").Data().(string)
		var result string
		n.Put(key, value)
		replyTo := jsonParsed.Path("reply-to").Data().(string)
		jsonObj := gabs.New()
		jsonObj.Set(result, "result")
		utils.SendMessage(jsonObj.String(), replyTo)
		return result
	case "get":
		key := jsonParsed.Path("data").Path("key").Data().(string)
		result := n.Get(key)
		replyTo := jsonParsed.Path("reply-to").Data().(string)
		jsonObj := gabs.New()
		jsonObj.Set(result, "result")
		utils.SendMessage(jsonObj.String(), replyTo)
		return result
	case "remove":
		key := jsonParsed.Path("data").Path("key").Data().(string)
		result := n.Remove(key)
		replyTo := jsonParsed.Path("reply-to").Data().(string)
		jsonObj := gabs.New()
		jsonObj.Set(result, "result")
		utils.SendMessage(jsonObj.String(), replyTo)
		return result
	case "list-items":
		result := n.ListItems()
		replyTo := jsonParsed.Path("reply-to").Data().(string)
		jsonObj := gabs.New()
		jsonObj.Set(result, "result")
		utils.SendMessage(jsonObj.String(), replyTo)
		return result
	default:
		return "Invalid command received"
	}
}

func (n *ChordNode) Run() {
	context, _ := zmq.NewContext()
	defer context.Term()

	socket, _ := context.NewSocket(zmq.REP)
	defer socket.Close()

	socket.Connect(fmt.Sprintf("tcp://%s:%d", n.Address, n.Port))
	utils.Debug("[ChordRun] Client bound to port %sn", fmt.Sprint(n.Port))

	// Main loop, listening for commands
	for true {
		msg, err := socket.Recv(0)
		if err != nil {
			utils.Debug(err.Error())
		}

		utils.Debug("\t[ChordRun] msg received: %s\n", (string)(msg))
		reply := n.ProcessIncomingCommand(msg)
		socket.Send(reply, 0)
		utils.Debug("\t[ChordRun] Replied to message with: %s\n", reply)
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
