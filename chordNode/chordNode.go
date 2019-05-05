package chordNode

import (
	"encoding/json"
	"fmt"
	"time"

	zmq "github.com/alecthomas/gozmq"
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
	ID        uint32
	Successor uint32
	Table     fingerTable
	Address   string
	Port      int
}

/*
Returns a new ChordNode
*/
func New(id uint32, address string, port int) ChordNode {
	n := ChordNode{
		ID:      id,
		Address: address,
		Port:    port}
	n.Table = fingerTable{Size: 32}
	return n
}

// // Borrowed from https://stackoverflow.com/questions/26152993/go-logger-to-print-timestamp
// func (n ChordNode) Log(l *log.Logger, msg string) {
// 	l.SetPrefix("[" + time.Now().Format("2006-01-02 15:04:05") + " #" + string(n.ID) + "] ")
// 	l.Print(msg)
// }

func (n ChordNode) Run() {

	// l := log.New(os.Stdout, "", 0)

	// // n.Log(l, "Log 1")

	// <-time.After(time.Second * 3)

	// // n.Log(l, "Log 2")

	context, _ := zmq.NewContext()
	defer context.Close()

	socket, _ := context.NewSocket(zmq.REP)
	defer socket.Close()

	socket.Connect(fmt.Sprintf("tcp://%s:%d", n.Address, n.Port))
	fmt.Printf("Client bound to port %d\n", n.Port)

	// Main loop, listening for commands
	for true {
		msg, _ := socket.Recv(0)

		// Set up dict for data to be imported into
		var data map[string]interface{}
		err := json.Unmarshal([]byte(msg), &data)
		if err != nil {
			panic(err)
		}

		fmt.Println("Hello ", data["hello"])

		// fmt.Printf("Node %d received '%s'\n", n.ID, msg)
		// fmt.Println(reflect.TypeOf(msg))
		// msgString := string(msg)
		// b, err := json.MarshalIndent(&msgString, "", "\t")
		// if err != nil {
		// 	fmt.Println("error:", err)
		// }
		// os.Stdout.Write(b)
		// n.Log(l, string(messageFormatted))
		// println(string(msg))
		time.Sleep(time.Second)
		reply := "" //fmt.Sprintf("Message received.")
		socket.Send([]byte(reply), 0)
	}

	/*
			for request in range(1, 10):
		    print "Sending request ", request, "..."
		    socket.send("Hello")
		    message = socket.recv()
		    print "Received reply", request, "[", message, "]"
	*/

	// for i := 0; i < 10; i++ {
	// 	msg := fmt.Sprintf("Hello %d", i)
	// 	client.Send([]byte(msg), 0)
	// 	println("Sending", msg)

	// 	reply, _ := client.Recv(0)
	// 	println("Received", string(reply))
	// }

}
