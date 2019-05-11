package main

import (
	"chord/chordNode"
	"chord/utils"
	"fmt"
	"strconv"
	"testing"
)

func TestCreateRing(t *testing.T) {
	nodeDirectory := map[uint32]string{}
	node1 := chordNode.GenerateRandomNode()
	node2 := chordNode.GenerateRandomNode()
	chordNode.AddNodeToDirectory(nodeDirectory, node1)
	chordNode.AddNodeToDirectory(nodeDirectory, node2)

	go node1.Run()
	go node2.Run()

	// Note: if we're using ID as address, should the JoinRing command still use string?
	joinCommand := utils.JoinRingCommand(strconv.FormatUint(uint64(node1.ID), 10))
	fmt.Println(joinCommand.String())
	qAddress := chordNode.GetAddress(nodeDirectory, strconv.FormatUint(uint64(node2.ID), 10) )
	reply := utils.SendMessage(joinCommand.String(), qAddress)

	fmt.Println(reply)
}