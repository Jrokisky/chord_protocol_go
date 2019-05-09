package main

import (
	"chord/chordNode"
	"fmt"
	"time"

	zmq "github.com/pebbe/zmq4"
)

import "github.com/Jeffail/gabs"

type nodeAddress struct {
	NodeID    uint32
	IPAddress string
	Port      string
}

type nodeDirectory struct {
	nodes []nodeAddress
}

// func SpawnChordNode(address string, port int, joinAddress string, joinNode int) {
// 	newNode := chordNode.New(rand.Uint32())
// 	// Check if we are joining an existing ring
// 	if joinAddress == nil && joinPort == nil {

// 	}
// 	 // or starting a new ring
// 	else
// 	{

// 		}

// }

/*
Send a message over 0MQ
*/
func SendMessage(address string, port int, data string) string {
	context, _ := zmq.NewContext()
	socket, _ := context.NewSocket(zmq.REQ)
	defer socket.Close()
	socket.Bind(fmt.Sprintf("tcp://%s:%d", address, port))
	socket.Send(data, 0)

	// Wait for reply:
	reply, _ := socket.Recv(0)
	fmt.Printf("Received '%s'\n", string(reply))
	return string(reply)
}

/* Generate the JSON command to instruct the node that receives it to join the ring
at the 'address' argument. */
// func commandSendJoinRing(address string) string {
// 	command := make(map[string]interface{})
// 	command["do"] = "join-ring"
// 	jsonCommand, err := json.Marshal(command)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return string(jsonCommand)
// }

// func CommandPutItem(data string) {

// }

func JoinRingCommand(address string) *gabs.Container {
	jsonObj := gabs.New()
	jsonObj.Set("join-ring", "do")
	jsonObj.Set(address, "sponsoring-node")
	return jsonObj

}

func main() {
	node1 := chordNode.New(1, "127.0.0.1", 5555)
	go node1.Run()
	for {
		// reader := bufio.NewReader(os.Stdin)
		// fmt.Print("Write here > ")
		// input, _ := reader.ReadString('\n')
		// msg := input
		// fmt.Println(input)
		joinCommand := JoinRingCommand("127.0.0.1:5555")
		// fmt.Println(joinCommand.StringIndent("", "  "))
		SendMessage(node1.Address, node1.Port, joinCommand.String())

		time.Sleep(100 * time.Millisecond)

	}
}

// ringSize := 32 // 2^32

// User input loop, initially no nodes
// nodes := make(map[uint32]nodeAddress)
// User adds node

// newNodeAddress := nodeAddress{NodeID: newNode.ID, IpAddress: "localhost", Port: "5555"}
// // Add node to directory
// nodes[newNode.ID] = newNodeAddress
// go newNode.Run()

// fmt.Printf("Server bound to port %s\n", port)

// }
