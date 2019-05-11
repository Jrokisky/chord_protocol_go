package main

import (
	"chord/chordNode"
	"chord/utils"
	"fmt"
	"time"
)

type nodeAddress struct {
	NodeID    uint32
	IPAddress string
	Port      string
}

type nodeDirectory struct {
	nodes []nodeAddress
}

func main() {
	node1 := chordNode.New("127.0.0.1", 5555)
	go node1.Run()
	for {
		// Instruct our new node to join the ring (and it should create its own ring of size 1)
		joinCommand := utils.JoinRingCommand("127.0.0.1:5555")
		utils.SendMessage(fmt.Sprintf("tcp://%s:%d", node1.Address, node1.Port), joinCommand.String())
		time.Sleep(100 * time.Millisecond)
	}
}
