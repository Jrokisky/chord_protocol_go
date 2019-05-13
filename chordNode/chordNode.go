package chordnode

import (
	"chord/utils"

	"fmt"
	"strconv"
	"strings"
	"sync"
	"errors"

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
	ID		uint32
	Predecessor	*uint32
	Successor	*uint32
	Table		fingerTable
	Address		string
	Port		int
	InRing		bool
	Data		map[string]string
	Directory	*map[uint32]string
	mux		sync.Mutex
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
	n.mux.Lock()
	n.Predecessor = nil
	n.Successor = new(uint32)
	*(n.Successor) = n.ID
	n.InRing = true
	n.mux.Unlock()

	jsonObj := gabs.New()
	jsonObj.Set("ok", "status")
	msg.Merge(jsonObj)

	return msg.String()
}

// Respond to an instruction to join a chord ring
func (n *ChordNode) JoinRing(msg *gabs.Container) string {
	jsonObj := gabs.New()
	sponsorAddress := msg.Path("sponsoring-node").Data().(string)
	newmsg := utils.FindRingSuccessorCommand(n.ID, n.GetOwnAddress())
	response_from_sponsor, err := utils.SendMessage(newmsg, sponsorAddress)
	if err != nil {
		jsonObj.Set("failure", "error")
	} else {
		jsonParsed, _ := gabs.ParseJSON([]byte(response_from_sponsor))
		id, _ := utils.ParseToUInt32(jsonParsed.Path("id").String())

		n.mux.Lock()
		n.Predecessor = nil
		if n.Successor == nil {
			n.Successor = new(uint32)
		}
		*(n.Successor) = id
		n.mux.Unlock()

		n.InRing = true
		jsonObj.Set("ok", "status")
	}

	msg.Merge(jsonObj)

	return msg.String()
}

func (n *ChordNode) GetOwnAddress() string {
	return fmt.Sprintf("tcp://%s:%d", n.Address, n.Port)
}

func (n *ChordNode) LeaveRing(msg *gabs.Container) string {
	mode := msg.Path("mode").Data().(string)
	utils.Debug("[LeaveRing: %s] leaving with mode: %s\n", fmt.Sprint(n.ID), mode)

	// Leave gracefully and inform others
	if strings.Compare(mode, "orderly") == 0 {
		// notify predecessor and successor
		successorAddress, present := (*n.Directory)[*(n.Successor)]
		orderlyLeaveMsg := utils.NotifyOrderlyLeaveCommand(n.ID, n.Predecessor, n.Successor)
		utils.Debug("[LeavRing: %s] Sending leave msg: %s to successor: %s\n", fmt.Sprint(n.ID), fmt.Sprint(*(n.Successor)), orderlyLeaveMsg)
		_, _ = utils.SendMessage(orderlyLeaveMsg, successorAddress)
		if (n.Predecessor != nil) {
			predecessorAddress, _ := (*n.Directory)[*(n.Predecessor)]
			utils.Debug("[LeavRing: %s] Sending leave msg: %s to predecessor: %s\n", fmt.Sprint(n.ID), fmt.Sprint(*(n.Predecessor)), orderlyLeaveMsg)
			_, _ = utils.SendMessage(orderlyLeaveMsg, predecessorAddress)
		}

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
				reply, err := utils.SendMessage(putCommand, successorAddress)
				if (err != nil) {

				} else {
					fmt.Println("k:%s v:%s reply:%s", k, v, reply)
				}
			}
		}


	}

	n.InRing = false
	n.Predecessor = nil
	n.Successor = nil

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

func (n *ChordNode) StabilizeRing() string {
	if (n.Successor != nil) {
		succ_addr := (*n.Directory)[*(n.Successor)]
		cmd := utils.FindRingPredecessorCommand()
		response, err := utils.SendMessage(cmd, succ_addr)
		if err != nil {
			return "Stabilization Failed due to lack of response from Successor"
		} else {
			successor := *(n.Successor)
			if response != "No Predecessor" {
				succ_pred, _ := utils.ParseToUInt32(response)
				// Successor's Predecessor is in between this node and Successor
				if utils.IsBetween(n.ID, *(n.Successor), succ_pred) {
					n.mux.Lock()
					successor = succ_pred
					*(n.Successor) = successor
					n.mux.Unlock()
				}
			}
			// Send notify message to the new successor.
			cmd := utils.RingNotifyCommand(n.ID, n.GetOwnAddress())
			succ_addr := (*n.Directory)[successor]
			_, err := utils.SendMessage(cmd, succ_addr)
			if err != nil {
				utils.Debug("[Stabilize %s] Error from successor %s\n", fmt.Sprint(n.ID), fmt.Sprint(successor))
				return "Error Stabilizing Ring"
			} else {
				utils.Debug("[Stabilize %s] Error from successor %s\n", fmt.Sprint(n.ID), fmt.Sprint(successor))
				return "Stabilization Successful!"
			}
		}
	}
	return "Could not stabilize. No Successor."
}

func (n *ChordNode) RingNotify(id uint32, replyTo string) string {
	if (n.Predecessor == nil) {
		n.Predecessor = new(uint32)
		*(n.Predecessor) = id
		return fmt.Sprintf("Predecessor set to %d\n", id)
	} else if (utils.IsBetween(*(n.Predecessor), n.ID, id)) {
		*(n.Predecessor) = id
		return fmt.Sprintf("Predecessor set to %d\n", id)
	}
	return "No Predecessor set\n"
}

func (n ChordNode) GetRingFingers() string {
	return ""
}

// {"do": "find-ring-successor", "id": id, "reply-to": address}
func (n *ChordNode) FindRingSuccessor(id uint32) (uint32, error) {
	var result uint32
	// Special case when adding second node to ring
	if n.Predecessor == nil && *(n.Successor) == n.ID {
		utils.Debug("\t[FindRingSuccessor] Adding second node to ring\n")
		// First node in the Chord, so now there's only two nodes in chord.
		n.mux.Lock()
		n.Predecessor = new(uint32)
		*(n.Predecessor) = id
		n.Successor = new(uint32)
		*(n.Successor) = id
		n.mux.Unlock()
		result = n.ID
	} else if(utils.IsBetween(n.ID, *(n.Successor), id)) {
		utils.Debug("\t[FindRingSuccessor: %s] id: %s is between %s and its successor: %s\n", fmt.Sprint(n.ID), fmt.Sprint(id), fmt.Sprint(n.ID), fmt.Sprint(*(n.Successor)))
		n.mux.Lock()
		result = *(n.Successor)
		*(n.Successor) = id
		n.mux.Unlock()
	} else if (n.Predecessor != nil) && (utils.IsBetween(*(n.Predecessor), n.ID, id)) {
		utils.Debug("\t[FindRingSuccessor: %s] id: %s is between %s's predecessorr: %s and itself\n", fmt.Sprint(n.ID), fmt.Sprint(id), fmt.Sprint(n.ID), fmt.Sprint(*(n.Predecessor)))
		n.mux.Lock()
		result = n.ID
		*(n.Predecessor) = id
		n.mux.Unlock()
	} else {
		// Recursively ask successors.
		utils.Debug("\t[FindRingSuccessor: %s] Passing message to successor: %s\n", fmt.Sprint(n.ID), fmt.Sprint(*(n.Successor)))
		request := utils.FindRingSuccessorCommand(id, n.GetOwnAddress())
		directory := *n.Directory
		successor := *(n.Successor)
		response_from_successor, err := utils.SendMessage(request, directory[successor])
		if err != nil {
			return 0, errors.New("Error finding Ring Successor")
		} else {
			jsonParsed, _ := gabs.ParseJSON([]byte(response_from_successor))
			id, _ := utils.ParseToUInt32(jsonParsed.Path("id").String())
			result = id
		}
	}
	return result, nil
}

func (n *ChordNode) ProcessOrderlyLeave(jsonParsed *gabs.Container) string {
	leaver, _ := utils.ParseToUInt32(jsonParsed.Path("leaver").String())
	succ, succ_err := utils.ParseToUInt32(jsonParsed.Path("successor").String())
	pred, pred_err := utils.ParseToUInt32(jsonParsed.Path("predecessor").String())

	if (*(n.Successor) == leaver) && (succ_err == nil) {
		// Replace n's successor (since it's leaving) with the leaving node's successor.
		n.mux.Lock()
		*(n.Successor) = succ
		n.mux.Unlock()
		return "Successor updated with Leaver's successor"
	} else if (n.Predecessor != nil && *(n.Predecessor) == leaver) && (pred_err == nil){
		// Replace n's predecessor (since it's leaving) with the leaving node's predecessor.
		n.mux.Lock()
		*(n.Predecessor) = pred
		n.mux.Unlock()
		return "Precessor updated with Leaver's successor"
	}
	return "No changes made"
}

func (n *ChordNode) FindRingPredecessor() string {
	if (n.Predecessor != nil) {
		return fmt.Sprint(*(n.Predecessor))
	} else {
		return "No Predecessor"
	}
}

func (n ChordNode) Put(key string, value string) (string, error) {
	//replyTo := msg.Path("reply-to").String()
	id := utils.ComputeId(key)
	targetNode, _ := n.FindRingSuccessor(id)
	// Should we store it on this node?
	if targetNode == n.ID {
		n.Data[key] = value
		fmt.Printf("Storing key '%s' with value '%s' at hash %d on this node, %d", key, value, id, n.ID)
	} else {
		fmt.Printf("Sending key '%s' with value '%s' to be stored on %d at hash %d", key, value, targetNode, id)
		request := utils.PutCommand(key, value, n.Address)
		reply, err := utils.SendMessage(request, (*n.Directory)[targetNode])
		return reply, err
	}
	return "", errors.New("error") //TODO: implement
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

func (n *ChordNode) ProcessIncomingCommand(msg string) (string, error) {
	jsonParsed, _ := gabs.ParseJSON([]byte(msg))
	command := jsonParsed.Path("do").Data().(string)

	// If a node is not in the ring, simulate a dropped message.
	switch strings.TrimSpace(command) {
	case "init-ring-fingers", "stabilize-ring", "fix-ring-fingers", "leave-ring", "get-ring-fingers", "find-ring-successor", "find-ring-predecessor", "put", "get", "remove":
		if !n.InRing {
			utils.Debug("[NOT_IN_RING] command: %s | %s is not in the ring.\n", command, fmt.Sprint(n.ID))
			return "", errors.New("Not in Ring")
		}
	}

	switch strings.TrimSpace(command) {
	case "create-ring":
		return n.CreateRing(jsonParsed), nil
	case "join-ring":
		result := n.JoinRing(jsonParsed)
		return result, nil
	case "init-ring-fingers":
		n.InitRingFingers()
		return "", nil
	case "fix-ring-fingers":
		n.FixRingFingers()
		return "", nil
	case "stabilize-ring":
		n.StabilizeRing()
		return "Ring Stabilized", nil
	case "leave-ring":
		n.LeaveRing(jsonParsed)
		return "", nil
	case "notify-orderly-leave":
		result := n.ProcessOrderlyLeave(jsonParsed)
		return result, nil
	case "ring-notify":
		id, _ := utils.ParseToUInt32(jsonParsed.Path("id").String())
		// TODO: i don't think reply to is needed here
		replyTo := jsonParsed.Path("reply-to").Data().(string)
		result := n.RingNotify(id, replyTo)
		return result, nil
	case "get-ring-fingers":
		n.GetRingFingers()
		return "", nil
	case "find-ring-successor":
		id, _ := utils.ParseToUInt32(jsonParsed.Path("id").String())
		result, err := n.FindRingSuccessor(id)
		if err != nil {
			return "", err
		} else {
			jsonObj := gabs.New()
			jsonObj.Set(result, "id")
			return jsonObj.String(), nil
		}
	case "find-ring-predecessor":
		// TODO: I don't think this needs any arguments.
		// AFAICT, a node will send this message to its successor to get the successor's
		// predecessor.
		result := n.FindRingPredecessor()
		return result, nil
	case "put":
		key := jsonParsed.Path("data").Path("key").Data().(string)
		value := jsonParsed.Path("data").Path("value").Data().(string)
		var result string
		n.Put(key, value)
		replyTo := jsonParsed.Path("reply-to").Data().(string)
		jsonObj := gabs.New()
		jsonObj.Set(result, "result")
		result, err := utils.SendMessage(jsonObj.String(), replyTo)
		return result, err
	case "get":
		key := jsonParsed.Path("data").Path("key").Data().(string)
		result := n.Get(key)
		replyTo := jsonParsed.Path("reply-to").Data().(string)
		jsonObj := gabs.New()
		jsonObj.Set(result, "result")
		result, err := utils.SendMessage(jsonObj.String(), replyTo)
		return result, err
	case "remove":
		key := jsonParsed.Path("data").Path("key").Data().(string)
		result := n.Remove(key)
		replyTo := jsonParsed.Path("reply-to").Data().(string)
		jsonObj := gabs.New()
		jsonObj.Set(result, "result")
		result, err := utils.SendMessage(jsonObj.String(), replyTo)
		return result, err
	case "list-items":
		result := n.ListItems()
		replyTo := jsonParsed.Path("reply-to").Data().(string)
		jsonObj := gabs.New()
		jsonObj.Set(result, "result")
		result, err := utils.SendMessage(jsonObj.String(), replyTo)
		return result, err
	default:
		return "Invalid command received", errors.New("invalid command")
	}
}

func (n *ChordNode) Run() {
	context, _ := zmq.NewContext()
	defer context.Term()

	socket, _ := context.NewSocket(zmq.REP)
	defer socket.Close()
	socket.Connect(fmt.Sprintf("tcp://%s:%d", n.Address, n.Port))

	utils.Debug("[ChordRun: %s] Client bound to port %s\n", fmt.Sprint(n.ID), fmt.Sprint(n.Port))

	for {
		msg, err := socket.Recv(0)
		if err != nil {
			utils.Debug(err.Error())
		}

		utils.Debug("[chordRun: %s] msg received: %s\n", fmt.Sprint(n.ID), (string)(msg))
		reply, err := n.ProcessIncomingCommand(msg)
		if err != nil {
			utils.Debug("[ChordRun: %s] Sending Error msg: %s\n", fmt.Sprint(n.ID), err.Error())
			socket.Send(utils.ERROR_MSG, 0)
		} else {
			utils.Debug("[ChordRun: %s] Sending msg: %s\n", fmt.Sprint(n.ID), reply)
			socket.Send(reply, 0)
			utils.Debug("[ChordRun: %s] message sent\n", fmt.Sprint(n.ID))
		}
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
