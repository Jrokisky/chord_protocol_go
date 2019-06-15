# Chord Protocol Implementation + Web Visualizer in Go using ZeroMQ

## Background

### Authors
* [Duncan Beard] (https://github.com/DuncanBeard)
* [Justin Rokisky] (https://github.com/Jrokisky)
* [Frank Serna] (https://github.com/rancid2040)

### Disclaimer
This was a semester project completed on a deadline, so there are likely bugs.

-------------------------------------------------------------------------------

## Instructions
1. Clone/download/unzip the project repository into the `src` folder of your go development folder
2. Run `go build main.go`
3. Run `./main`
4. Try it out at `http://localhost:8080/visualize`

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

