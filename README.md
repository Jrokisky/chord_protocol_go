# Project 2
### Due May 15 (11:59pm EST) [250 points. Group.]

Write a GoLang distributed application that implements the CHORD protocol/distributed hash table. Assume a CHORD ring of order 2^N for some large constant N, e.g. N=32.  

Chord nodes are assigned randomly unique 32-bit (unsigned integer) identifiers. Each Chord node maintains a bucket (list of) (key, value) pairs of a hash table that is distributed among the nodes that are members of the Chord ring. There is no limit on a node's bucket size besides the available memory to GoLang processes, while the keys and values are assumed to be strings. For convenience, Chord nodes are to be implemented as goroutines in the GoLang. Chord nodes are to communicate asynchronously with other Chord nodes using JSON messages over zeroMQ sockets. The IP address and port number of a node's socket is its access point (address).  

Your main GoLang routine (aka coordinator) should spawn some Chord nodes, and then, instruct them to join/leave the Chord ring, as well as get/put/remove key-value pairs from/to the distributed hash table. You may issue such instructions at random or read/load them from a file. 
Chord nodes reveive JSON request messages from the coordinator or other Chord nodes and respond to the sender (or reply-to address specified) directly. We assume the time it takes a node to respond to any message is a random variable (with exponential distribution whose mean is a parameter in your program).  

-------------------------------------------------------------------------------
## Instructions
1. Clone/download/unzip the project repository into the `src` folder of your go development folder
2. Run `go build main.go`
3. Run `./main`
3. Try it out at `http://localhost:8080/visualize`

## Visualizer

### Table
* table of Node data ranked by their id
* The columns represent the Successor, Predecessor, and Finger table entries
* The operations are to Join, Orderly Leave, and leave
* You may need to click some of the links more than once

### Buttons
* The textfield next to the "Add Nodes" button can be used to add a set number of nodes. (10 is a good starting number)
* The "Random Join" button will have a node randomly join

### Chart
* Nodes are draw on the cicle based on their id
* When nodes are not in the ring, they are red
* When nodes are in the ring, they are green
* The blue line represents a successor
* The red lines represent finger table entries
* If you look closely at the lines, you'll notice that some of the lines don't touch their target. This is intended. The end where the line does not touch the circle is the end (aka the target).

