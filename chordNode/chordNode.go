package chordNode

import (
	"fmt"
)

type chordNode struct {
	name string
}

func New(name string) chordNode {
	n := chordNode{name}
	return n
}

func (n chordNode) HelloWorld() {

	fmt.Printf("Hello from %s\n", n.name)
}
