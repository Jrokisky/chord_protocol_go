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

/*
A node capable of joining and operating a Chord ring
*/
type ChordNode struct {
	ID		uint32
	Predecessor	*uint32
	Successor	*uint32
	Table		[32](*uint32)
	Address		string
	Port		int
	InRing		bool
	Data		map[string]string
	Directory	*map[uint32]string
	mux		sync.Mutex
	curr_finger	int
	SecondNode	bool // This is a janky way for node that created the ring to take
			     // special action when the second node joins.
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
	for i := 0; i < len(n.Table); i++ {
		n.Table[i] = nil
	}
	n.Data = make(map[string]string)
	n.Directory = directory
	n.InRing = false
	n.curr_finger = 0
	n.SecondNode = true
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
	n.SecondNode = false
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
		n.InRing = true
		// Init Finger table
		n.Table[0] = new(uint32)
		*(n.Table[0]) = id
		n.mux.Unlock()

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
	n.mux.Lock()
	n.InRing = false
	n.Predecessor = nil
	n.Successor = nil
	for k := 0; k < 32; k++ {
		n.Table[k] = nil
	}
	n.mux.Unlock()

	jsonObj := gabs.New()
	jsonObj.Set("ok", "status")
	msg.Merge(jsonObj)

	return msg.String()
}

func (n ChordNode) InitRingFingers() string {
	return ""
}

func (n *ChordNode) FixRingFingers() string {
	n.mux.Lock()
	n.curr_finger = n.curr_finger + 1
	if n.curr_finger > 31 {
		n.curr_finger = 0
	}
	// Ask the closest preceding finger
	finger_id := n.ID + (uint32)(2^(n.curr_finger)) // Rely on integer wraparound
	request := utils.FindRingSuccessorCommand(finger_id, n.GetOwnAddress())
	directory := *n.Directory
	response_from_successor, err := utils.SendMessage(request, directory[*(n.Successor)])
	if err != nil {
		// TODO: we could possibly try again with another node in the finger table.
		n.mux.Unlock()
		return "Failure Fixing Finger"
	} else {
		jsonParsed, _ := gabs.ParseJSON([]byte(response_from_successor))
		id, _ := utils.ParseToUInt32(jsonParsed.Path("id").String())
		if n.Table[n.curr_finger] == nil {
			n.Table[n.curr_finger] = new(uint32)
		}
		*(n.Table[n.curr_finger]) = id
		n.mux.Unlock()
		return fmt.Sprintf("Success Fixing Finger %d with value %d\n", finger_id, id)
	}

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
	} else {
		// Successor failed so update with some value from the finger table
		for k := 0; k < 32; k++ {
			if n.Table[k] != nil {
				*(n.Successor) = *(n.Table[k])
				break;
			}
		}
	}
	return "Could not stabilize. No Successor."
}

func (n *ChordNode) RingNotify(id uint32, replyTo string) string {
	if (n.Predecessor == nil && n.ID != id) {
		n.Predecessor = new(uint32)
		*(n.Predecessor) = id
		return fmt.Sprintf("Predecessor set to %d\n", id)
	} else if n.Predecessor != nil && (utils.IsBetween(*(n.Predecessor), n.ID, id)) {
		*(n.Predecessor) = id
		return fmt.Sprintf("Predecessor set to %d\n", id)
	}
	return "No Predecessor set\n"
}

func (n ChordNode) GetRingFingers() string {
	return ""
}

func (n *ChordNode) ClosestPrecedingNode(id uint32) uint32 {
	for i := 31; i >= 0; i-- {
		if (n.Table[i]) != nil {
			finger_val := *(n.Table[i])
			if utils.IsBetween(n.ID, id, finger_val) {
				return finger_val
			}
		}
	}
	return n.ID
}

// {"do": "find-ring-successor", "id": id, "reply-to": address}
func (n *ChordNode) FindRingSuccessor(id uint32) (uint32, bool, error) {
	var result uint32
	var more bool // Did we reach the end of the chain, or is there more to search?
	// Special case for when the second node joins, so we can break the cycle of the 
	// first node's successor being itself.
	if !n.SecondNode { // Will be set to true for all nodes that didn't create the ring
		n.mux.Lock()
		n.Predecessor = new(uint32)
		*(n.Predecessor) = id
		n.Successor = new(uint32)
		*(n.Successor) = id
		n.Table[0] = new(uint32)
		*(n.Table[0]) = id
		n.SecondNode = true
		n.mux.Unlock()
		result = n.ID
		more = false
	} else if id == n.ID {
		result = *(n.Successor)
		more = false
	} else if utils.IsBetween(n.ID, *(n.Successor), id) {
		utils.Debug("\t[FindRingSuccessor: %s] id: %s is between %s and its successor: %s\n", fmt.Sprint(n.ID), fmt.Sprint(id), fmt.Sprint(n.ID), fmt.Sprint(*(n.Successor)))
		result = *(n.Successor)
		more = false
	} else {
		// Return who to ask next.
		result = n.ClosestPrecedingNode(id)
		utils.Debug("\t[FindRingSuccessor: %s] For %s, please contact my closeset successor: %s\n", fmt.Sprint(n.ID), fmt.Sprint(id), fmt.Sprint(result))
		more = true
	}
	return result, more, nil
}

func (n *ChordNode) ProcessOrderlyLeave(jsonParsed *gabs.Container) string {
	leaver, _ := utils.ParseToUInt32(jsonParsed.Path("leaver").String())
	succ, succ_err := utils.ParseToUInt32(jsonParsed.Path("successor").String())
	pred, pred_err := utils.ParseToUInt32(jsonParsed.Path("predecessor").String())

	if n.Successor != nil && (*(n.Successor) == leaver) && (succ_err == nil) {
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

func (n *ChordNode) CheckPredecessor() {
	if n.Predecessor != nil {
		cmd := utils.PingCommand()
		pred_address := (*n.Directory)[*(n.Predecessor)]
		_, err := utils.SendMessage(cmd, pred_address)
		if err != nil {
			n.Predecessor = nil
		}
	}
}

func (n ChordNode) Put(key string, value string) (string, error) {
	//replyTo := msg.Path("reply-to").String()
	id := utils.ComputeId(key)
	var targetNode uint32
	for {
		var more bool
		targetNode, more, _ = n.FindRingSuccessor(id)
		id = targetNode
		if !more {break;}
	}
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
	case "init-ring-fingers", "check-predecessor", "ring-notify", "notify-orderly-leave", "ping", "stabilize-ring", "fix-ring-fingers", "leave-ring", "get-ring-fingers", "find-ring-successor", "find-ring-predecessor", "put", "get", "remove":
		if !n.InRing {
			utils.Debug("[NOT_IN_RING] command: %s | %s is not in the ring.\n", command, fmt.Sprint(n.ID))
			return "", errors.New("Not in Ring")
		}
	}

	switch strings.TrimSpace(command) {
	case "ping":
		return "Healthy", nil
	case "create-ring":
		return n.CreateRing(jsonParsed), nil
	case "join-ring":
		result := n.JoinRing(jsonParsed)
		return result, nil
	case "init-ring-fingers":
		n.InitRingFingers()
		return "", nil
	case "fix-ring-fingers":
		result := n.FixRingFingers()
		return result, nil
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
	case "check-predecessor":
		n.CheckPredecessor()
		return "", nil
	case "find-ring-successor":
		id, _ := utils.ParseToUInt32(jsonParsed.Path("id").String())
		var err error
		err = nil
		var result uint32
		for {
			var more bool
			result, more, err = n.FindRingSuccessor(id)
			id = result
			if !more { break;}
			fmt.Printf("=========================================================MORE: %v\n\n", more)
		}

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

	socket, _ := context.NewSocket(zmq.ROUTER)
	defer socket.Close()
	socket.Bind(fmt.Sprintf("tcp://%s:%d", n.Address, n.Port))

	dealer, _ := zmq.NewSocket(zmq.DEALER)
	defer dealer.Close()
	dealer.Bind(fmt.Sprintf("inproc://%d", n.ID))

	for i := 0; i < 8; i++ {
		utils.Debug("[ChordRun: %s] worker threads spawned\n", fmt.Sprint(n.ID))
		go n.ChordWorker()
	}
	utils.Debug("[ChordRun: %s] Client bound to port %s\n", fmt.Sprint(n.ID), fmt.Sprint(n.Port))

	err := zmq.Proxy(socket, dealer, nil)
	if err != nil {
		utils.Debug("[ChordRun: %s] Proxy dropped\n", fmt.Sprint(n.ID))
	}
}

func (n *ChordNode) ChordWorker() {
	worker,  _ := zmq.NewSocket(zmq.DEALER)
	defer worker.Close()
	worker.Connect(fmt.Sprintf("inproc://%d", n.ID))

	utils.Debug("[ChordRun: %s] Worker thread loop starting\n", fmt.Sprint(n.ID))
	for {
		msg, err := worker.RecvMessage(0)
		id, content, err := utils.Pop(msg)

		if err != nil {
			utils.Debug("[chordRun: %s] worker errord\n", fmt.Sprint(n.ID))
		} else {
			utils.Debug("[chordRun: %s] worker received: %s\n", fmt.Sprint(n.ID), (string)(content[0]))
			reply, err := n.ProcessIncomingCommand(content[0])
			if err != nil {
				utils.Debug("[ChordRun: %s] Sending Error msg: %s\n", fmt.Sprint(n.ID), err.Error())
				worker.SendMessage(id, utils.ERROR_MSG)
			} else {
				utils.Debug("[ChordRun: %s] Sending msg: %s\n", fmt.Sprint(n.ID), reply)
				worker.SendMessage(id, reply)
				utils.Debug("[ChordRun: %s] message sent\n", fmt.Sprint(n.ID))
			}
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
