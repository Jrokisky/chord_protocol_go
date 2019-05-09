package main

import (
	"chord/chordNode"
	"testing"
)

func TestCreateRing(t *testing.T) {
	node1 := chordNode.New(1, "127.0.0.1", 5555)
	go node1.Run()
	reply := SendMessage("127.0.0.1", 5555, "{\"do\":\"ping\"}")
	if reply != ""

}