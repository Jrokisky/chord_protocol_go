package main

import (
	chordnode "chord/chordNode"
	"chord/utils"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/Jeffail/gabs"
)

func TestCreateRing(t *testing.T) {
	nodeDirectory := map[uint32]string{}
	node1 := chordnode.GenerateRandomNode(&nodeDirectory)
	node1.AddNodeToDirectory()

	go node1.Run()

	sourceAddress, _ := node1.GetSocketAddress()
	createCommand := utils.CreateRingCommand()
	fmt.Println(createCommand)
	reply := utils.SendMessage(createCommand, sourceAddress)
	jsonParsed, _ := gabs.ParseJSON([]byte(reply))
	status, _ := strconv.Unquote(jsonParsed.Path("status").String())
	fmt.Println("InRing - expected: true actual:", node1.InRing)
	if strings.Compare(status, "ok") != 0 {
		t.Errorf("status = %s", status)
	}
}

func TestJoinRing(t *testing.T) {
	nodeDirectory := map[uint32]string{}
	node1 := chordnode.GenerateRandomNode(&nodeDirectory)
	node2 := chordnode.GenerateRandomNode(&nodeDirectory)
	node1.AddNodeToDirectory()
	node2.AddNodeToDirectory()

	go node1.Run()
	go node2.Run()

	sourceAddress, _ := node1.GetSocketAddress()
	joinCommand := utils.JoinRingCommand(sourceAddress)
	fmt.Println(joinCommand)
	destAddress, _ := node2.GetSocketAddress()
	reply := utils.SendMessage(joinCommand, destAddress)
	jsonParsed, _ := gabs.ParseJSON([]byte(reply))
	status, _ := strconv.Unquote(jsonParsed.Path("status").String())
	fmt.Println("InRing - expected: true actual:", node2.InRing)
	if strings.Compare(status, "ok") != 0 {
		t.Errorf("status = %s", status)
	}
}

func TestLeaveRing(t *testing.T) {
	nodeDirectory := map[uint32]string{}
	node1 := chordnode.GenerateRandomNode(&nodeDirectory)
	node2 := chordnode.GenerateRandomNode(&nodeDirectory)
	node1.AddNodeToDirectory()
	node2.AddNodeToDirectory()

	go node1.Run()
	go node2.Run()

	sourceAddress, _ := node1.GetSocketAddress()
	joinCommand := utils.JoinRingCommand(sourceAddress)
	fmt.Println(joinCommand)
	destAddress, _ := node2.GetSocketAddress()
	utils.SendMessage(joinCommand, destAddress)

	leaveCommandI := utils.LeaveRingCommand("immediately")
	leaveCommandO := utils.LeaveRingCommand("orderly")
	reply := utils.SendMessage(leaveCommandI, destAddress)
	jsonParsed, _ := gabs.ParseJSON([]byte(reply))
	status, _ := strconv.Unquote(jsonParsed.Path("status").String())
	fmt.Println("InRing - expected: false actual:", node2.InRing)
	if strings.Compare(status, "ok") != 0 {
		t.Errorf("status = %s", status)
	}

	reply = utils.SendMessage(leaveCommandO, sourceAddress)
	jsonParsed, _ = gabs.ParseJSON([]byte(reply))
	status, _ = strconv.Unquote(jsonParsed.Path("status").String())
	fmt.Println("InRing - expected: false actual:", node1.InRing)
	if strings.Compare(status, "ok") != 0 {
		t.Errorf("status = %s", status)
	}
}
