package utils

import (
	"crypto/sha1"
	"encoding/binary"
	"math/big"
	"math/rand"
	"strconv"

	// TODO: remove - debugging
	"fmt"

	zmq "github.com/pebbe/zmq4"
)

// Inclusive
const MinPort = 1025
const MaxPort = 47808
const Localhost = "127.0.0.1"

func ComputeId(input string) uint32 {
	// Hash input
	hash := sha1.New()
	hash.Write([]byte(input))
	hashed_in := hash.Sum(nil)

	// Conver input to int
	hash_big := new(big.Int)
	hash_big.SetBytes(hashed_in)

	// Mod chord size
	size_big := new(big.Int)
	size_big.Exp(big.NewInt(2), big.NewInt(32), nil)
	result_big := new(big.Int)
	result_big = hash_big.Mod(hash_big, size_big)

	// Convert to uint32
	return binary.BigEndian.Uint32(result_big.Bytes())
}

func SendMessage(msg string, address string) string {
	context, _ := zmq.NewContext()
	defer context.Term()

	socket, _ := context.NewSocket(zmq.REQ)
	defer socket.Close()

	// TODO: remove - debugging
	fmt.Println("address: ", address)
	socket.Bind(address)
	socket.Send(msg, 0)

	reply, _ := socket.Recv(0)
	return string(reply)
}

func GetRandomPort() int {
	return rand.Intn(MaxPort-MinPort) + MinPort
}

func ParseToUInt32(input string) uint32 {
	result64, _ := strconv.ParseUint(input, 10, 32) // TODO add error checking
	result := uint32(result64)
	return result
}
