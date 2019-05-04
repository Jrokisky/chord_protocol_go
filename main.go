package main

import (
	"bufio"
	"chord/chordNode"
	"fmt"
	"os"
	"time"

	zmq "github.com/alecthomas/gozmq"
)

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
func SendMessage(address string, port int, msg string) {
	context, _ := zmq.NewContext()
	socket, _ := context.NewSocket(zmq.REQ)
	defer socket.Close()
	socket.Bind(fmt.Sprintf("tcp://%s:%d", address, port))
	socket.Send([]byte(msg), 0)

	// Wait for reply:
	reply, _ := socket.Recv(0)
	fmt.Printf("Received '%s'\n", string(reply))
}

func main() {
	node1 := chordNode.New(1, "127.0.0.1", 5555)
	go node1.Run()
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Write here > ")
		input, _ := reader.ReadString('\n')
		msg := input
		fmt.Println(input)
		SendMessage(node1.Address, node1.Port, msg)

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
